package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

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
       |  AND a.atttypid > 0
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

sealed abstract class ProcedureCheckError {
  def description: String
}

object ProcedureCheckError {
  case class NoDatabaseCounterpart(nameForInvocation: String) extends ProcedureCheckError {
    def description: String = s"The procedure invoked as $nameForInvocation does not have its counterpart in database"
  }
  case class NoInvocationName(p: Procedure[_]) extends ProcedureCheckError {
    def description: String = s"A procedure $p does not have a registered name for invocation"
  }
}

sealed abstract class ParamCheckError extends ProcedureCheckError

object ParamCheckError {
  case class ArgCountMismatch(codeSize: Int, dbSize: Int) extends ParamCheckError {
    override def description: String = s"DB args count $dbSize does not match code one $codeSize"
  }

  case class BasicNameMismatch(param: Int, codeName: String, dbName: String) extends ParamCheckError {
    def description: String = s"A code parameter name `$codeName` does not match " +
      s"the database name `$dbName` (a code parameter name gets prefixed by an underscore " +
      s"while calling procedures for basic (scalar/array of scalars) types)"
  }

  case class CompoundNameMismatch(param: Int, codeName: String, dbName: String) extends ParamCheckError {
    def description: String = s"A code parameter name `$codeName` does not match the database name `$dbName` " +
      s"(an exact match is expected for parameters of compound (records/tables and their arrays) types)"
  }

  case class CantGetTypeOid(basicTraits: BasicTypeTraits) extends ParamCheckError {
    def description = s"Can't get type OID for parameter type traits $basicTraits"
  }

  case class BasicOidMismatch(codeOid: TypeOid, codeTypeName: String, dbOid: Int, dbTypeName: String)
    extends ParamCheckError {
    def description = s"Code TypeOid $codeOid (name=$codeTypeName) does not conform to DB type OID=$dbOid, name=$dbTypeName"
  }

  object BasicOidMismatch {
    def apply(codeOid: TypeOid, dbOid: Int)(implicit c: Connection): BasicOidMismatch = {
      val codeTypeName = PgType.fetchTypeName(codeOid.exactOid).getOrElse("<cannot be retrieved>")
      val dbTypeName = PgType.fetchTypeName(dbOid).getOrElse("<cannot be retrieved>")
      BasicOidMismatch(codeOid, codeTypeName, dbOid, dbTypeName)
    }
  }

  case class CantGetTypeAttrs(oid: Int, typeName: String) extends ParamCheckError {
    def description: String = s"Can't get DB compound type attributes for code parameter " +
      s"type OID=$oid, name=$typeName (is the type compound? does the type exist in database?)"
  }

  object CantGetTypeAttrs {
    def apply(oid: Int)(implicit c: Connection): CantGetTypeAttrs =
      CantGetTypeAttrs(oid, PgType.fetchTypeName(oid).getOrElse("<cannot be retrieved>"))
  }

  case class AttrSizeMismatch(attrDefsSize: Int, codeTraitsSize: Int) extends ParamCheckError {
    def description: String = s"Attribute defs size $attrDefsSize does not match code type traits size $codeTraitsSize"
  }

  case class CantGetAttrOid(index: Int) extends ParamCheckError {
    def description: String = s"Can't get type OID for attribute #$index"
  }

  case class AttrOidMismatch(index: Int, codeOid: TypeOid, codeTypeName: String, dbOid: Int, dbTypeName: String)
    extends ParamCheckError {
    def description: String = s"Code TypeOid $codeOid (name=$codeTypeName) " +
      s"does not match DB type OID=$dbOid, name=$dbTypeName for attribute #$index"
  }

  object AttrOidMismatch {
    def apply(index: Int, codeOid: TypeOid, dbOid: Int)(implicit c: Connection): AttrOidMismatch = {
      val codeTypeName = PgType.fetchTypeName(codeOid.exactOid).getOrElse("<cannot be retrieved>")
      val dbTypeName = PgType.fetchTypeName(dbOid).getOrElse("<cannot be retrieved>")
      AttrOidMismatch(index, codeOid, codeTypeName, dbOid, dbTypeName)
    }
  }
}

class ProcedureParamTypeChecker (tableName: TableName, procedure: Procedure[_], dbDef: DbProcedureDef, param: Int)
  (implicit c: Connection)
    extends ProcedureCheckSupport {

  import ParamCheckError._

  type Result = Option[Seq[ParamCheckError]]

  def result(): Result = {
    procedure.paramsDefs(param).typeTraits.asEitherBasicOrCompound match {
      case Left(basicTraits) => checkBasicParamType(basicTraits)
      case Right(compoundTraits) => checkCompoundParamType(compoundTraits)
    }
  }

  protected def checkBasicParamType(basicTraits: BasicTypeTraits): Result = {
    basicTraits.storedInType.getOrFetchOid() match {
      case None => Some(Seq(CantGetTypeOid(basicTraits)))
      case Some(codeTypeOid) if codeTypeOid.isAssignableFrom(dbDef.argTypes(param)) => None
      case Some(codeTypeOid) => Some(Seq(BasicOidMismatch(codeTypeOid, dbDef.argTypes(param))))
    }
  }

  private[calldb] def checkCompoundParamType(compoundTraits: CompoundTypeTraits): Result = {
    val compoundSize = compoundTraits.columnsTraits.size
    fetchCompoundTypeAttributes(dbDef.argTypes(param)) match {
      case None => Some(Seq(CantGetTypeAttrs(dbDef.argTypes(param))))
      case Some(attrDefs) if attrDefs.size == compoundSize => checkCompoundTypeAttributes(attrDefs, compoundTraits)
      case Some(attrDefs) => Some(Seq(AttrSizeMismatch(attrDefs.size, compoundSize)))
    }
  }

  private[calldb] def checkCompoundTypeAttributes(attributeDefs: IndexedSeq[DbAttributeDef],
                                                  compoundTypeTraits: CompoundTypeTraits): Result = {
    /**
      * Procedure compound parameters are supplied via SQL ROW() operator, which accepts only positional arguments.
      * Thus, code and database attributes must match exactly.
      */
    val accumErrors = new mutable.ArrayBuffer[ParamCheckError]
    for (((attrDef, columnTraits), index) <- attributeDefs.zip(compoundTypeTraits.columnsTraits).zipWithIndex) {
      columnTraits.storedInType.getOrFetchOid() match {
        case None => accumErrors += CantGetAttrOid(index)
        case Some(codeOid) if codeOid.isAssignableFrom(attrDef.typeId) => ()
        case Some(codeOid) => accumErrors += AttrOidMismatch(index, codeOid, attrDef.typeId)
      }
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }
}

sealed abstract class ReturnTypeCheckError extends ProcedureCheckError

object ReturnTypeCheckError {
  case class ExpectedBasicType(typeTraits: TypeTraits) extends ReturnTypeCheckError {
    def description: String = s"Expected basic return type to match DB def, got $typeTraits"
  }

  case class ExpectedCompoundType(typeTraits: TypeTraits) extends ReturnTypeCheckError {
    def description: String = s"Expected compound return type to match DB def, got $typeTraits"
  }

  case class FailedToFetchAttrs(dbCompoundType: DbCompoundType) extends ReturnTypeCheckError {
    def description: String = s"Failed to fetch compound type attributes for return type $dbCompoundType"
  }

  case class CantGetCodeTypeOid(codeRetType: PgType) extends ReturnTypeCheckError {
    def description: String = s"Can't get code type OID for $codeRetType"
  }

  case class CantGetCodeColumnTypeOid(name: String, codeRetType: PgType) extends ReturnTypeCheckError {
    def description: String = s"Can't get code type OID for column `$name` of $codeRetType"
  }

  case class ReturnTypeMismatch(codeRetType: PgType, dbType: Int) extends ReturnTypeCheckError {
    def description: String = s"Code return type $codeRetType doesn't conform to DB return type $dbType"
  }

  case class ReturnColumnTypeMismatch(name: String, codeRetType: PgType, dbType: Int) extends ReturnTypeCheckError {
    def description: String = s"Code return type $codeRetType doesn't conform to DB return type $dbType for column `$name`"
  }

  case class MissingResultColumnNames() extends ReturnTypeCheckError {
    def description: String = s"Result column names are not specified, can't check DB result set columns"
  }

  case class NotATableArgMode(argModes: Traversable[DbArgMode]) extends ReturnTypeCheckError {
    def description: String = s"Weird output table arg mode value(s): $argModes, expected only ${DbArgMode.Table}"
  }

  case class TraitsAndNamesSizeMismatch(traits: CompoundTypeTraits, names: Seq[String]) extends ReturnTypeCheckError {
    def description: String = s"Column traits $traits size does not match column names $names size"
  }

  private def columnNamesAsSet(columnNames: Seq[String]): Set[String] = {
    if (columnNames.isEmpty) {
      throw new IllegalArgumentException("Column names must not be empty")
    }
    val set = columnNames.map(_.toLowerCase).toSet
    if (set.size != columnNames.size) {
      throw new IllegalArgumentException(s"There were duplicated columns" +
        s"in $columnNames (considering collation case-insensitive)")
    }
    set
  }

  case class PresentOnlyInCode(columnNames: Set[String]) extends ReturnTypeCheckError {
    def description: String = s"Result columns $columnNames are present only in code, not in DB"
  }

  object PresentOnlyInCode {
    def apply(columnNames: String*): PresentOnlyInCode = PresentOnlyInCode(columnNamesAsSet(columnNames))
  }

  case class PresentOnlyInDB(columnNames: Set[String]) extends ReturnTypeCheckError {
    def description: String = s"Result columns $columnNames are present only in DB, not in code"
  }

  object PresentOnlyInDB {
    def apply(columnNames: String*): PresentOnlyInDB = PresentOnlyInDB(columnNamesAsSet(columnNames))
  }
}

class ProcedureReturnTypeChecker(tableName: TableName, procedure: Procedure[_], dbDef: DbProcedureDef)
  (implicit c: Connection)
    extends ProcedureCheckSupport {

  import ReturnTypeCheckError._

  def result(): Option[Seq[ReturnTypeCheckError]] = {
    if (PgType.Record.pgNumericOid.get == dbDef.retType) {
      checkProcedureRecordReturnType()
    } else {
      fetchUserDefinedCompoundTypes().get(dbDef.retType) match {
        case Some(userDefinedType) => checkProcedureCompoundUdt(userDefinedType)
        case None => checkProcedureScalarReturnType()
      }
    }
  }

  private def testColumnType(codeName: String, codeTraits: BasicTypeTraits, dbOid: Int): Option[ReturnTypeCheckError] = {
    codeTraits.storedInType.getOrFetchOid() match {
      case None => Some(CantGetCodeColumnTypeOid(codeName, codeTraits.storedInType))
      case Some(codeTypeOid) if codeTypeOid.conforms(dbOid) => None
      case Some(_) => Some(ReturnColumnTypeMismatch(codeName, codeTraits.storedInType, dbOid))
    }
  }

  private def mkColumnNamesMismatchErrors(onlyInCode: Set[String], onlyInDb: Set[String]) = {
    assert(onlyInCode.size + onlyInDb.size > 0)
    val seqOnlyInCode = if (onlyInCode.nonEmpty) Seq(PresentOnlyInCode(onlyInCode)) else Seq()
    val seqOnlyInDb = if (onlyInDb.nonEmpty) Seq(PresentOnlyInDB(onlyInDb)) else Seq()
    Some(seqOnlyInCode ++ seqOnlyInDb)
  }

  private[calldb] def checkProcedureRecordReturnType(): Option[Seq[ReturnTypeCheckError]] = {

    // Expect a compound type
    if (procedure.resultType.isBasic)
      return Some(Seq(ExpectedBasicType(procedure.resultType)))

    val codeTypeTraits: CompoundTypeTraits = procedure.resultType.asOptCompound.get

    /**
      * Compound procedure return values are parsed with [[ResultSetParser]]s that fetch columns by name.
      * Thus, columns in output result set may be arranged in any order as long as unordered signature is the same.
      * Instead, name conformance matters, we should check whether output columns are named as expected.
      */
    if (procedure.resultColumnsNames.isEmpty)
      return Some(Seq(MissingResultColumnNames()))

    val codeColumnNames = procedure.resultColumnsNames.get.map(_.toLowerCase)
    if (codeTypeTraits.columnsTraits.size != codeColumnNames.size)
      return Some(Seq(TraitsAndNamesSizeMismatch(codeTypeTraits, codeColumnNames)))

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
      return Some(Seq(NotATableArgMode(dbRecordArgModes)))

    val dbOidsByName: Map[String, Int] = dbRecordNames.zip(dbRecordTypeOids).toMap

    val onlyInCode = codeColumnNames.toSet diff dbOidsByName.keySet
    val onlyInDb = dbOidsByName.keySet diff codeColumnNames.toSet

    if (onlyInCode.nonEmpty || onlyInDb.nonEmpty) {
      return mkColumnNamesMismatchErrors(onlyInCode, onlyInDb)
    }

    val accumErrors: Seq[ReturnTypeCheckError] = {
      for {
        (traits, name) <- codeTypeTraits.columnsTraits.zip(codeColumnNames)
        dbOid = dbOidsByName(name)
        err <- testColumnType(name, traits, dbOid)
      } yield err
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private[calldb] def checkProcedureCompoundUdt(dbCompoundType: DbCompoundType) : Option[Seq[ReturnTypeCheckError]] = {
    if (procedure.resultType.isBasic)
      return Some(Seq(ExpectedBasicType(procedure.resultType)))

    val columnsTraitsSeq = procedure.resultType.asOptCompound.get.columnsTraits

    if (procedure.resultColumnsNames.isEmpty)
      return Some(Seq(MissingResultColumnNames()))

    val codeColumnsNames = procedure.resultColumnsNames.get.map(_.toLowerCase)
    val optCompoundTypeAttrs = fetchCompoundTypeAttributes(dbCompoundType.typeId)
    if (optCompoundTypeAttrs.isEmpty)
      return Some(Seq(FailedToFetchAttrs(dbCompoundType)))

    // TODO: Why an attempt to destructure this in for-comprehension does not compile?
    val compoundTypeAttrs: IndexedSeq[DbAttributeDef] = optCompoundTypeAttrs.get
    val dbAttrByName = (for (attr <- compoundTypeAttrs) yield (attr.name, attr)).toMap

    val namesOnlyInCode = codeColumnsNames.toSet diff dbAttrByName.keySet
    val namesOnlyInDb = dbAttrByName.keySet diff codeColumnsNames.toSet
    if (namesOnlyInCode.nonEmpty || namesOnlyInDb.nonEmpty) {
      return mkColumnNamesMismatchErrors(namesOnlyInCode, namesOnlyInDb)
    }

    val accumErrors: Seq[ReturnTypeCheckError] = {
      for {
        (traits, name) <- columnsTraitsSeq.zip(codeColumnsNames)
        dbOid = dbAttrByName(name).typeId
        err <- testColumnType(name, traits, dbOid)
      } yield err
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private[calldb] def checkProcedureScalarReturnType(): Option[Seq[ReturnTypeCheckError]] = {
    if (procedure.resultType.isCompound) {
      return Some(Seq(ExpectedCompoundType(procedure.resultType)))
    }

    val codeRetType: PgType = procedure.resultType.asOptBasic.get.storedInType
    codeRetType.getOrFetchOid() match {
      case None => Some(Seq(CantGetCodeTypeOid(codeRetType)))
      case Some(codeRetTypeOid) if codeRetTypeOid.isAssignableFrom(dbDef.retType) => None
      case Some(_) => Some(Seq(ReturnTypeMismatch(codeRetType, dbDef.retType)))
    }
  }
}

class ProcedureChecker(tableName: TableName, procedure: Procedure[_], dbDef: DbProcedureDef)(implicit c: Connection)
  extends ProcedureCheckSupport {

  def result(): Option[Seq[ProcedureCheckError]] = {
    val maybeRetTypeErrors = new ProcedureReturnTypeChecker(tableName, procedure, dbDef).result()
    val maybeParamErrors = checkProcedureParams(procedure, dbDef)
    val accumErrors = maybeRetTypeErrors.toSeq.flatten ++ maybeParamErrors.toSeq.flatten
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private[calldb] def checkProcedureParams(p: Procedure[_], dbDef: DbProcedureDef): Option[Seq[ProcedureCheckError]] = {
    // Further checking can't be done (it requires parameters count match)
    if (p.paramsDefs.size != dbDef.argTypes.size)
      return Some(Seq(ParamCheckError.ArgCountMismatch(p.paramsDefs.size, dbDef.argTypes.size)))

    val accumErrors = new mutable.ArrayBuffer[ParamCheckError]
    for (((dbTypeOid, paramDef), i) <- dbDef.argTypes.zip(procedure.paramsDefs).zipWithIndex) {
      new ProcedureParamTypeChecker(tableName, procedure, dbDef, i).result() match {
        case Some(errors) => accumErrors ++= errors
        case None => accumErrors ++= checkParamNames(paramDef, dbDef.argNames(i), i)
      }
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private def checkParamNames(paramDef: TypedCallable.ParamsDef[_], dbName: String, index: Int) = {
    val codeName = paramDef.name
    if (paramDef.typeTraits.isBasic) {
      if (s"${codeName}_".toLowerCase() == dbName.toLowerCase()) None else {
        Some(ParamCheckError.BasicNameMismatch(index, codeName, dbName))
      }
    } else if (codeName.toLowerCase() == dbName.toLowerCase()) None else {
      Some(ParamCheckError.CompoundNameMismatch(index, codeName, dbName))
    }
  }
}

class TableProceduresChecker(tableName: TableName)(implicit c: Connection) extends ProcedureCheckSupport {
  def checkProcedures(procedures: Traversable[Procedure[_]]): Option[Seq[ProcedureCheckError]] = {
    val databaseProcedures: Map[String, DbProcedureDef] = super.fetchDbProcedureDefs()
    val accumErrors: Seq[ProcedureCheckError] = {
      for (p <- procedures.toSeq; errors <- checkProcedure(p, databaseProcedures).toSeq; e <- errors)
        yield e
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private[calldb] def checkProcedure(p: Procedure[_], dbOnes: Map[String, DbProcedureDef]): Option[Seq[ProcedureCheckError]] = {
    ExistingProcedureInvocationFacility.findCallableName(p) match {
      case Some(nameForInvocation) => dbOnes.get(nameForInvocation) match {
        case Some(dbDef) => new ProcedureChecker(tableName, p, dbDef).result()
        case None => Some(Seq(ProcedureCheckError.NoDatabaseCounterpart(nameForInvocation)))
      }
      case None => Some(Seq(ProcedureCheckError.NoInvocationName(p)))
    }
  }
}
