package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

import ProceduresReflector.DefinedProcedure

private[calldb] sealed abstract class DbArgMode(final val mode: String) {
  def unapply(s: String): Option[DbArgMode] =
    if (mode.equalsIgnoreCase(s)) Some(this) else None
}

private[calldb] object DbArgMode {
  case object In extends DbArgMode("i")
  case object Out extends DbArgMode("o")
  case object InOut extends DbArgMode("b")
  case object Variadic extends DbArgMode("v")
  case object Table extends DbArgMode("t")

  def apply(s: String): DbArgMode = s.toLowerCase() match {
    case In.mode => In
    case Out.mode => Out
    case InOut.mode => InOut
    case Variadic.mode => Variadic
    case Table.mode => Table
  }
}

private[calldb] case class DbProcedureDef(
  name: String,
  argTypes: IndexedSeq[Int],
  allArgTypes: IndexedSeq[Int],
  argModes: IndexedSeq[DbArgMode],
  argNames: IndexedSeq[String],
  retType: Int)

private[calldb] case class DbCompoundType(name: String, typeId: Int, elemOid: Int, arrayOid: Int)

private[calldb] case class DbAttributeDef(name: String, typeId: Int, number: Int)

private[calldb] trait ProcedureCheckSupport {
  // Casting p.proargtypes to int[] directly produces a funky output that crashes pg-async library
  private val proceduresDefsQuery: String =
    """
      |SELECT
      |  p.proname as proname,
      |  p.prorettype::int as prorettype,
      |  string_to_array(p.proargtypes::text, ' ')::int[] as proargtypes,
      |  p.proallargtypes::int[] as proallargtypes,
      |  p.proargmodes::text[] as proargmodes,
      |  p.proargnames::text[] as proargnames
      |FROM pg_catalog.pg_proc p
      |  JOIN pg_catalog.pg_namespace n
      |    ON p.pronamespace = n.oid
      |WHERE n.nspname = 'public' AND p.prolang != 13
    """.stripMargin

  private val proceduresDefsParser = new RowDataParser[DbProcedureDef](row =>
    DbProcedureDef(
      row("proname").asInstanceOf[String],
      Option(row("proargtypes")).getOrElse(IndexedSeq()).asInstanceOf[IndexedSeq[Int]],
      Option(row("proallargtypes")).getOrElse(IndexedSeq()).asInstanceOf[IndexedSeq[Int]],
      Option(row("proargmodes")).getOrElse(IndexedSeq()).asInstanceOf[IndexedSeq[String]].map(s => DbArgMode(s)),
      Option(row("proargnames")).getOrElse(IndexedSeq()).asInstanceOf[IndexedSeq[String]],
      row("prorettype").asInstanceOf[Int])) {
    def expectedColumnsNames: Option[IndexedSeq[String]] =
      Some(IndexedSeq("proname", "proargtypes", "proallargtypes", "proargmodes", "proargnames", "prorettype"))
  }

  private val compoundTypeQuery: String =
    """
      |SELECT t.typname as typname, t.oid::int as typoid, t.typelem::int as typelem, t.typarray::int as typarray
      |FROM pg_type t
      |  JOIN pg_namespace ns
      |    ON t.typnamespace = ns.oid
      |WHERE t.typrelid > 0
      |  AND ns.nspname = 'public'
    """.stripMargin

  private val compoundTypeParser = new RowDataParser[DbCompoundType](row =>
    DbCompoundType(
      row("typname").asInstanceOf[String],
      row("typoid").asInstanceOf[Int],
      row("typelem").asInstanceOf[Int],
      row("typarray").asInstanceOf[Int])) {
    def expectedColumnsNames = Some(IndexedSeq("typname", "typoid", "typelem", "typarray"))
  }

  private def attributeDefsQueryByOid(compoundTypeOid: Int): String =
    s"""
       |SELECT
       |  a.attname as attname,
       |  a.atttypid::int as atttypid,
       |  (a.attnum::int - 1) as attnum
       |FROM pg_class c, pg_attribute a, pg_type t
       |WHERE t.oid = $compoundTypeOid
       |  AND a.attnum > 0
       |  AND a.attrelid = c.oid
       |  AND a.attrelid = t.typrelid
       |ORDER BY a.attnum
     """.stripMargin

  private val attribureDefsParser = new RowDataParser[DbAttributeDef](row =>
    (row("attname"), row("atttypid"), row("attnum")) match {
      case (name: String, typeId: Int, number: Int) =>
        DbAttributeDef(name, typeId, number)
    }
  ) {
    def expectedColumnsNames = Some(IndexedSeq("attname", "atttypid", "attnum"))
  }

  def fetchDbProcedureDefs()(implicit c: Connection): Map[String, DbProcedureDef] = {
    val queryResult = Await.result(c.sendQuery(proceduresDefsQuery), 5.seconds)
    val parsedProcedures = proceduresDefsParser.*.parse(queryResult.rows.get)
    parsedProcedures.groupBy(_.name).mapValues(_.head)
  }

  def fetchUserDefinedCompoundTypes()(implicit c: Connection): Map[Int, DbCompoundType] = {
    val queryResult = Await.result(c.sendQuery(compoundTypeQuery), 5.seconds)
    val parsedTypes = compoundTypeParser.*.parse(queryResult.rows.get)
    parsedTypes.groupBy(_.typeId).mapValues(_.head)
  }

  def fetchCompoundTypeAttributes(compoundTypeOid: Int)(implicit c: Connection): Option[IndexedSeq[DbAttributeDef]] = {
    _fetchCompoundTypeAttributes(compoundTypeOid) match {
      case someAttrs @ Some(_) => someAttrs
      case None => {
        val udtByArrayOid: Map[Int, DbCompoundType] =
          fetchUserDefinedCompoundTypes().values.filter(_.arrayOid > 0).groupBy(_.arrayOid).mapValues(_.head)
        udtByArrayOid.get(compoundTypeOid) flatMap { elemTypeDef =>
          _fetchCompoundTypeAttributes(elemTypeDef.typeId)
        }
      }
    }
  }

  private def _fetchCompoundTypeAttributes(compoundTypeOid: Int)(implicit c: Connection): Option[IndexedSeq[DbAttributeDef]] = {
    val queryResult = Await.result(c.sendQuery(attributeDefsQueryByOid(compoundTypeOid)), 5.seconds)
    val parsedAttributes = attribureDefsParser.*.parse(queryResult.rows.get)
    if (parsedAttributes.nonEmpty) Some(parsedAttributes) else None
  }
}

class ProcedureParamTypeChecker
    (tableName: TableName, procedure: DefinedProcedure[_], dbDef: DbProcedureDef, param: Int)
    (implicit c: Connection)
  extends ProcedureCheckSupport {

  type Result = Option[Seq[String]]

  private def mkError(message: String): String =
    s"${tableName.exactName}: ${procedure.nameAsMember}: param #$param: $message"

  def result(): Result = {
    procedure.paramsDefs(param).typeTraits.asEitherBasicOrCompound match {
      case Left(basicTraits) => checkBasicParamType(basicTraits)
      case Right(compoundTraits) => checkCompoundParamType(compoundTraits)
    }
  }

  private[calldb] def errorCantGetTypeOid(basicTraits: BasicTypeTraits) =
    mkError(s"Can't get type OID for parameter type traits $basicTraits")

  private[calldb] def errorCodeTypeOidDoesNotConform(codeTypeOid: TypeOid, dbOid: Int)(implicit c: Connection) = {
    val codeTypeName = PgType.fetchTypeName(codeTypeOid.exactOid).getOrElse("<cannot be retrieved>")
    val dbTypeName = PgType.fetchTypeName(dbOid).getOrElse("<cannot be retrieved>")
    mkError(s"Code TypeOid $codeTypeOid (name=$codeTypeName) does not conform to DB type OID=$dbOid, name=$dbTypeName")
  }

  protected def checkBasicParamType(basicTraits: BasicTypeTraits): Result = {
    basicTraits.storedInType.getOrFetchOid() match {
      case None =>
        Some(Seq(errorCantGetTypeOid(basicTraits)))
      case Some(codeTypeOid) => {
        if (codeTypeOid.isAssignableFrom(dbDef.argTypes(param))) None else {
          Some(Seq(errorCodeTypeOidDoesNotConform(codeTypeOid, dbDef.argTypes(param))))
        }
      }
    }
  }

  private[calldb] def errorCantGetCompoundTypeAttributes(oid: Int)(implicit c: Connection) = {
    val typeName = PgType.fetchTypeName(oid)
    mkError(s"Can't get DB compound type attributes for code parameter type OID=$oid, " +
            s"name=$typeName (is the type compound? does the type exist in database?)")
  }

  private[calldb] def errorAttributeDefsSizeDoesNotMatch(attrDefsSize: Int, codeTraitsSize: Int) =
    mkError(s"Attribute defs size $attrDefsSize does not match code type traits size $codeTraitsSize")

  private[calldb] def errorCantGetAttributeOid(index: Int) =
    mkError(s"Can't get type OID for attribute #$index")

  private[calldb] def errorCodeAttributeOidDoesNotMatch(index: Int, codeOid: TypeOid, dbOid: Int)(implicit c: Connection) = {
    val codeTypeName = PgType.fetchTypeName(codeOid.exactOid).getOrElse("<cannot be retrieved>")
    val dbTypeName = PgType.fetchTypeName(dbOid).getOrElse("<cannot be retrieved>")
    mkError(s"Code TypeOid $codeOid (name=$codeTypeName) does not match " +
            s"DB type OID=$dbOid, name=$dbTypeName for attribute #$index")
  }

  private[calldb] def checkCompoundParamType(compoundTraits: CompoundTypeTraits): Result = {
    fetchCompoundTypeAttributes(dbDef.argTypes(param)) match {
      case None =>
        Some(Seq(errorCantGetCompoundTypeAttributes(dbDef.argTypes(param))))
      case Some(attributeDefs) => {
        if (attributeDefs.size != compoundTraits.columnsTraits.size) {
          Some(Seq(errorAttributeDefsSizeDoesNotMatch(attributeDefs.size, compoundTraits.columnsTraits.size)))
        } else {
          checkCompoundTypeAttributes(attributeDefs, compoundTraits)
        }
      }
    }
  }

  private[calldb] def checkCompoundTypeAttributes(attributeDefs: IndexedSeq[DbAttributeDef],
                                                  compoundTypeTraits: CompoundTypeTraits): Result = {
    /**
      * Procedure compound parameters are supplied via SQL ROW() operator, which accepts only positional arguments.
      * Thus, code and database attributes must match exactly.
      */
    val accumErrors = new mutable.ArrayBuffer[String]
    for (((attrDef, columnTraits), index) <- attributeDefs.zip(compoundTypeTraits.columnsTraits).zipWithIndex) {
      columnTraits.storedInType.getOrFetchOid() match {
        case None =>
          accumErrors += errorCantGetAttributeOid(index)
        case Some(codeOid) => {
          if (!codeOid.isAssignableFrom(attrDef.typeId)) {
            accumErrors += errorCodeAttributeOidDoesNotMatch(index, codeOid, attrDef.typeId)
          }
        }
      }
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }
}

class ProcedureReturnTypeChecker
  (tableName: TableName, procedure: DefinedProcedure[_], dbDef: DbProcedureDef)
  (implicit c: Connection)
  extends ProcedureCheckSupport {

  type Result = Option[Seq[String]]

  private def mkError(message: String): String =
    s"${tableName.exactName}: ${procedure.nameAsMember}: $message"

  def result(): Result = {
    if (PgType.Record.pgNumericOid.get == dbDef.retType) {
      checkProcedureRecordReturnType()
    } else {
      fetchUserDefinedCompoundTypes().get(dbDef.retType) match {
        case Some(userDefinedType) =>
          checkProcedureUserDefinedCompoundReturnType(userDefinedType)
        case None =>
          checkProcedureScalarReturnType()
      }
    }
  }

  private[calldb] def errorExpectedCompoundReturnType() =
    mkError(s"Expected compound return type to match DB def, got ${procedure.resultType}")

  private[calldb] def errorResultColumnNamesAreNotSpecified() =
    mkError(s"Result column names are not specified, can't check DB result set columns")

  private[calldb] def errorWeirdArgModes(argModes: Traversable[DbArgMode]) =
    mkError(s"Weird output table arg mode value(s): $argModes, expected only ${DbArgMode.Table}")

  private[calldb] def errorColumnTraitsSizeDoesNotMatch(codeTypeTraits: CompoundTypeTraits, columnNames: Seq[String]) =
    mkError(s"Column traits ${codeTypeTraits.columnsTraits} size does not match column names $columnNames size")

  private[calldb] def errorColumnsArePresentOnlyInCode(columns: Traversable[String]) =
    mkError(s"Result columns ${columns.toSet} are present only in code, not in DB")

  private[calldb] def errorColumnsArePresentOnlyInDB(columns: Traversable[String]) =
    mkError(s"Result columns ${columns.toSet} are present only in DB, not in code")

  private[calldb] def errorCantGetOrFetchColimnTypeOid(codeColumnName: String, codeColumnType: PgType) =
    mkError(s"Column $codeColumnName: can't get (or fetch) TypeOid for $codeColumnType")

  private[calldb] def errorCodeColumnTypeOidDoesNotConform(codeColumnName: String, codeTypeOid: TypeOid, dbOid: Int) =
    mkError(s"Column $codeColumnName: code TypeOid $codeTypeOid does not conform to DB OID $dbOid")

  private[calldb] def checkProcedureRecordReturnType(): Result = {

    // Expect a compound type
    if (procedure.resultType.isBasic)
      return Some(Seq(errorExpectedCompoundReturnType()))

    val codeTypeTraits: CompoundTypeTraits = procedure.resultType.asOptCompound.get

    /**
      * Compound procedure return values are parsed with [[ResultSetParser]]s that fetch columns by name.
      * Thus, columns in output result set may be arranged in any order as long as unordered signature is the same.
      * Instead, name conformance matters, we should check whether output columns are named as expected.
      */
    if (procedure.resultColumnsNames.isEmpty) {
      return Some(Seq(errorResultColumnNamesAreNotSpecified()))
    }
    val codeColumnNames = procedure.resultColumnsNames.get
    if (codeTypeTraits.columnsTraits.size != codeColumnNames.size)
      return Some(Seq(errorColumnTraitsSizeDoesNotMatch(codeTypeTraits, codeColumnNames)))

    // First, drop names of call signature args and leave only output table column args
    val dbRecordNames = dbDef.argNames.drop(dbDef.argTypes.size)
    assert(dbRecordNames.size + dbDef.argTypes.size == dbDef.allArgTypes.size)
    assert(dbRecordNames.nonEmpty)
    // Drop signature call type oids and arg modes
    val dbRecordTypeOids = dbDef.allArgTypes.drop(dbDef.argTypes.size)
    val dbRecordArgModes = dbDef.argModes.drop(dbDef.argTypes.size)
    assert(dbRecordNames.size == dbRecordTypeOids.size && dbRecordNames.size == dbRecordArgModes.size)

    // To continue checking does not make sense
    if (dbRecordArgModes.exists(_ != DbArgMode.Table))
      return Some(Seq(errorWeirdArgModes(dbRecordArgModes)))

    val dbOidsByName: Map[String, Int] = dbRecordNames.zip(dbRecordTypeOids).toMap

    val onlyInCode = codeColumnNames.toSet diff dbOidsByName.keySet
    val onlyInDb = dbOidsByName.keySet diff codeColumnNames.toSet

    if (onlyInCode.nonEmpty || onlyInDb.nonEmpty) {
      // TODO: Replace with Seq[Option[String]] and flatten?
      val errors = new mutable.ArrayBuffer[String]
      if (onlyInCode.nonEmpty) {
        errors += errorColumnsArePresentOnlyInCode(onlyInCode)
      }
      if (onlyInDb.nonEmpty) {
        errors += errorColumnsArePresentOnlyInDB(onlyInDb)
      }
      // To continue checking does not make sense
      return Some(errors)
    }

    val accumErrors = new mutable.ArrayBuffer[String]
    for ((codeColumnTypeTraits, codeColumnName) <- codeTypeTraits.columnsTraits.zip(codeColumnNames)) {
      // We have ensured above that columns names match, so this call always succeed
      val dbColumnTypeOid = dbOidsByName(codeColumnName)
      codeColumnTypeTraits.storedInType.getOrFetchOid() match {
        case None =>
          accumErrors += errorCantGetOrFetchColimnTypeOid(codeColumnName, codeColumnTypeTraits.storedInType)
        case Some(codeTypeOid) => {
          if (!codeTypeOid.conforms(dbColumnTypeOid)) {
            accumErrors += errorCodeColumnTypeOidDoesNotConform(codeColumnName, codeTypeOid, dbColumnTypeOid)
          }
        }
      }
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private[calldb] def errorCantFetchCompoundTypeAttributes(dbCompoundType: DbCompoundType) =
    mkError(s"Can't fetch compound type attributes for $dbCompoundType")

  private[calldb] def errorCantGetOrFetchReturnColumnTypeOid(columnName: String, columnType: PgType) =
    mkError(s"Return column $columnName: can't get (or fetch) TypeOid for $columnType")

  private[calldb] def checkProcedureUserDefinedCompoundReturnType(dbCompoundType: DbCompoundType) : Result = {
    if (procedure.resultType.isBasic)
      return Some(Seq(errorExpectedCompoundReturnType()))

    val columnsTraitsSeq = procedure.resultType.asOptCompound.get.columnsTraits

    if (procedure.resultColumnsNames.isEmpty)
      return Some(Seq(errorResultColumnNamesAreNotSpecified()))

    val codeColumnsNames = procedure.resultColumnsNames.get.map(_.toLowerCase)
    val optCompoundTypeAttrs = fetchCompoundTypeAttributes(dbCompoundType.typeId)
    if (optCompoundTypeAttrs.isEmpty)
      return Some(Seq(errorCantFetchCompoundTypeAttributes(dbCompoundType)))

    // TODO: Why an attempt to destructure this in for-comprehension does not compile?
    val compoundTypeAttrs: IndexedSeq[DbAttributeDef] = optCompoundTypeAttrs.get
    val dbAttrByName = (for (attr <- compoundTypeAttrs) yield (attr.name, attr)).toMap

    val namesOnlyInCode = codeColumnsNames.toSet diff dbAttrByName.keySet
    val namesOnlyInDb = dbAttrByName.keySet diff codeColumnsNames.toSet
    if (namesOnlyInCode.nonEmpty || namesOnlyInDb.nonEmpty) {
      // TODO: Use Seq[Option[String]] and flatten?
      val errors = new mutable.ArrayBuffer[String]
      if (namesOnlyInCode.nonEmpty) {
        errors += errorColumnsArePresentOnlyInCode(namesOnlyInCode)
      }
      if (namesOnlyInDb.nonEmpty) {
        errors += errorColumnsArePresentOnlyInDB(namesOnlyInDb)
      }
      // Further checking does not make sense (since it is requeires names match)
      return Some(errors)
    }

    val accumErrors = new mutable.ArrayBuffer[String]
    for ((columnTraits, columnName) <- columnsTraitsSeq.zip(codeColumnsNames)) {
      val attrOid: Int = dbAttrByName(columnName).typeId
      columnTraits.storedInType.getOrFetchOid() match {
        case None =>
          accumErrors += errorCantGetOrFetchReturnColumnTypeOid(columnName, columnTraits.storedInType)
        case Some(codeTypeOid) => {
          if (!codeTypeOid.conforms(attrOid)) {
            accumErrors += errorCodeColumnTypeOidDoesNotConform(columnName, codeTypeOid, attrOid)
          }
        }
      }
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private[calldb] def errorExpectedBasicReturnType(): String =
    mkError(s"Expected basic return type to match DB def, got ${procedure.resultType}")

  private[calldb] def errorCantGetCodeTypeOid(codeRetType: PgType): String =
    mkError(s"Can't get code type OID for $codeRetType")

  private[calldb] def errorCodeBasicReturnTypeDoesNotConform(codeRetType: PgType) =
    mkError(s"Code return type $codeRetType does not conform to DB return type ${dbDef.retType}")

  private[calldb] def checkProcedureScalarReturnType(): Result = {
    if (procedure.resultType.isCompound) {
      return Some(Seq(errorExpectedBasicReturnType()))
    }

    val codeRetType: PgType = procedure.resultType.asOptBasic.get.storedInType
    codeRetType.getOrFetchOid() match {
      case None => {
        return Some(Seq(errorCantGetCodeTypeOid(codeRetType)))
      }
      case Some(codeRetTypeOid) => {
        if (!codeRetTypeOid.isAssignableFrom(dbDef.retType)) {
          return Some(Seq(errorCodeBasicReturnTypeDoesNotConform(codeRetType)))
        }
      }
    }
    None
  }
}

class ProcedureChecker(tableName: TableName, procedure: DefinedProcedure[_], dbDef: DbProcedureDef)(implicit c: Connection)
  extends ProcedureCheckSupport {

  type Result = Option[Seq[String]]

  private def mkError(message: String) =
    s"${tableName.exactName}: ${procedure.nameAsMember}: $message"

  private[calldb] def errorDBArgsCountDoesNotMatchCodeOne(): String =
    mkError(s"DB args count ${dbDef.argTypes.size} does not match code one ${procedure.paramsDefs.size}")

  private[calldb] def errorBasicParamNameMismatch(param: Int, codeName: String, dbName: String): String = {
    mkError(
      s"A code parameter name `$codeName` does not match the database name `$dbName` " +
      s"(a code parameter name gets prefixed by an underscore " +
      s"while calling procedures for basic (scalar/array of scalars) types)")
  }

  private[calldb] def errorCompoundParamNameMismatch(param: Int, codeName: String, dbName: String): String = {
    mkError(
      s"A code parameter name `$codeName` does not match the database name `$dbName` " +
      s"(an exact match is expected for parameters of compound (records/tables and their arrays) types)")
  }

  def result(): Result = {
    val accumErrors = new mutable.ArrayBuffer[String]
    for (errors <- new ProcedureReturnTypeChecker(tableName, procedure, dbDef).result())
      accumErrors ++= errors
    for (errors <- checkProcedureParams(procedure, dbDef))
      accumErrors ++= errors
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private[calldb] def checkProcedureParams(procedure: DefinedProcedure[_], dbDef: DbProcedureDef): Result = {
    // Further checking can't be done (it requires parameters count match)
    if (procedure.paramsDefs.size != dbDef.argTypes.size)
      return Some(Seq(errorDBArgsCountDoesNotMatchCodeOne()))

    val accumErrors = new mutable.ArrayBuffer[String]
    for (((dbTypeOid, paramDef), i) <- dbDef.argTypes.zip(procedure.paramsDefs).zipWithIndex) {
      new ProcedureParamTypeChecker(tableName, procedure, dbDef, i).result() match {
        case Some(errors) =>
          accumErrors ++= errors
        case None =>
          val (codeName, dbName) = (paramDef.name, dbDef.argNames(i))
          if (paramDef.typeTraits.isBasic) {
            if (s"${codeName}_".toLowerCase() != dbName.toLowerCase()) {
              accumErrors += errorBasicParamNameMismatch(i, codeName, dbName)
            }
          } else if (codeName.toLowerCase() != dbName.toLowerCase()) {
            accumErrors += errorCompoundParamNameMismatch(i, codeName, dbName)
          }
      }
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }
}

class TableProceduresChecker(tableName: TableName)(implicit c: Connection) extends ProcedureCheckSupport {
  type Result = Option[Seq[String]]

  private[calldb] def fetchDbProcedureDefs(): Map[String, DbProcedureDef] =
    super.fetchDbProcedureDefs()
  private[calldb] def fetchUserDefinedCompoundTypes(): Map[Int, DbCompoundType] =
    super.fetchUserDefinedCompoundTypes()
  private[calldb] def fetchCompoundTypeAttributes(compoundTypeOid: Int): Option[IndexedSeq[DbAttributeDef]] =
    super.fetchCompoundTypeAttributes(compoundTypeOid)

  def checkProcedures(procedures: Traversable[DefinedProcedure[_]]): Option[Seq[String]] = {
    val databaseProcedures: Map[String, DbProcedureDef] = fetchDbProcedureDefs()
    val resultBuilder = new mutable.ArrayBuffer[DefinedProcedure[_]]
    val allErrors = new mutable.ArrayBuffer[String]

    for (procedure <- procedures) {
      checkProcedure(procedure, databaseProcedures) match {
        case Some(errors) =>
          allErrors ++= errors
        case None =>
          resultBuilder += procedure
      }
    }

    if (allErrors.nonEmpty) Some(allErrors) else None
  }

  def injectProcedures(procedures: mutable.Traversable[DefinedProcedure[_]]): Unit = {
    for (procedure <- procedures) {
      procedure._nameInDatabase = procedure.mkQualifiedName(tableName.withoutPrefix)
    }
  }

  private[calldb] def errorProcedureNameAsMemberMustStartWithP(nameAsMember: String) =
    s"Procedure name as member $nameAsMember must start with `p`"

  private[calldb] def errorProcedureDoesNotHaveItsCounterpart(qualifiedName: String) =
    s"Procedure $qualifiedName does not have its counterpart in database"

  private[calldb] def checkProcedure(procedure: DefinedProcedure[_], databaseProcedures: Map[String, DbProcedureDef]): Result = {
    if (!procedure.nameAsMember.startsWith("p"))
      return Some(Seq(errorProcedureNameAsMemberMustStartWithP(procedure.nameAsMember)))

    val qualifiedName = procedure.mkQualifiedName(tableName.withoutPrefix).toLowerCase()
    databaseProcedures.get(qualifiedName) match {
      case None =>
        Some(Seq(errorProcedureDoesNotHaveItsCounterpart(qualifiedName)))
      case Some(dbDef) =>
        new ProcedureChecker(tableName, procedure, dbDef).result()
    }
  }
}
