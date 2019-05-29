package com.github.mirabout.calldb

import com.github.mauricio.async.db.{Connection, ResultSet, RowData}

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

private[calldb] case class DbProcedureDef(name: String, argTypes: IndexedSeq[Int], allArgTypes: IndexedSeq[Int],
                                          argModes: IndexedSeq[DbArgMode], argNames: IndexedSeq[String], retType: Int)

private[calldb] case class DbCompoundType(name: String, typeId: Int, elemOid: Int, arrayOid: Int)

private[calldb] case class DbAttrDef(name: String, typeId: Int, number: Int)

private[calldb] trait ProcedureCheckSupport {
  private val Name = "proname"
  private val RetType = "prorettype"
  private val ArgTypes = "proargtypes"
  private val AllArgTypes = "proallargtypes"
  private val ArgModes = "proargmodes"
  private val ArgNames = "proargnames"

  // Casting p.proargtypes to int[] directly produces a funky output that crashes pg-async library
  private val procDefsQuery: String =
    s"""
      |select
      |  p.proname                                         as $Name,
      |  p.prorettype::int                                 as $RetType,
      |  string_to_array(p.proargtypes::text, ' ')::int[]  as $ArgTypes,
      |  p.proallargtypes::int[]                           as $AllArgTypes,
      |  p.proargmodes::text[]                             as $ArgModes,
      |  p.proargnames::text[]                             as $ArgNames
      |from pg_catalog.pg_proc p
      |  join pg_catalog.pg_namespace n
      |    on p.pronamespace = n.oid
      |where n.nspname = 'public' and p.prolang != 13
    """.stripMargin

  private def parseSeq[A](row: RowData, name: String): IndexedSeq[A] =
    Option(row(name)).getOrElse(IndexedSeq()).asInstanceOf[IndexedSeq[A]]

  private def parseDef(row: RowData): DbProcedureDef = {
    val name = row(Name).asInstanceOf[String]
    val argTypes = parseSeq[Int](row, ArgTypes)
    val allArgTypes = parseSeq[Int](row, AllArgTypes)
    val argModes = parseSeq[String](row, ArgModes).map(DbArgMode.apply)
    val argNames = parseSeq[String](row, ArgNames)
    val retType = row(RetType).asInstanceOf[Int]
    DbProcedureDef(name, argTypes, allArgTypes, argModes, argNames, retType)
  }

  private val procDefsParser = new RowDataParser[DbProcedureDef](parseDef) {
    def expectedColumnsNames: Option[IndexedSeq[String]] =
      Some(IndexedSeq(Name, RetType, ArgTypes, AllArgTypes, ArgModes, ArgNames))
  }

  private val TypName = "typname"
  private val TypOid = "typoid"
  private val TypElem = "typelem"
  private val TypArray = "typarray"

  private val typeQuery: String =
    s"""
      |select
      |  t.typname         as $TypName,
      |  t.oid::int        as $TypOid,
      |  t.typelem::int    as $TypElem,
      |  t.typarray::int   as $TypArray
      |from pg_type t
      |  join pg_namespace ns
      |    on t.typnamespace = ns.oid
      |where t.typrelid > 0
      |  and ns.nspname = 'public'
    """.stripMargin

  private def parseType(row: RowData): DbCompoundType = {
    (row(TypName), row(TypOid), row(TypElem), row(TypArray)) match {
      case (name: String, oid: Int, elemOid: Int, arrayOid: Int) =>
        DbCompoundType(name, oid, elemOid, arrayOid)
    }
  }

  private val typeParser = new RowDataParser[DbCompoundType](parseType) {
    def expectedColumnsNames = Some(IndexedSeq(TypName, TypOid, TypElem, TypArray))
  }

  private val AttName = "attname"
  private val AttTypId = "atttypid"
  private val AttNum = "attnum"

  private def attributeDefsQueryByOid(compoundTypeOid: Int): String =
    s"""
       |select
       |  a.attname             as $AttName,
       |  a.atttypid::int       as $AttTypId,
       |  (a.attnum::int - 1)   as $AttNum
       |from pg_class c, pg_attribute a, pg_type t
       |where t.oid = $compoundTypeOid
       |  and a.attnum > 0
       |  and a.attrelid = c.oid
       |  and a.attrelid = t.typrelid
       |  and a.atttypid > 0
       |order by a.attnum
     """.stripMargin

  private def parseAttr(row: RowData) = {
    (row(AttName), row(AttTypId), row(AttNum)) match {
      case (name: String, typeId: Int, number: Int) =>
        DbAttrDef(name, typeId, number)
    }
  }

  private val attrDefsParser = new RowDataParser[DbAttrDef](parseAttr) {
    def expectedColumnsNames = Some(IndexedSeq(AttName, AttTypId, AttNum))
  }

  private def fetchResultSet(query: String)(implicit c: Connection): ResultSet =
    Await.result(c.sendQuery(query), 5.seconds).rows.get

  def fetchDbProcedureDefs()(implicit c: Connection): Map[String, DbProcedureDef] = {
    val parsedProcedures = procDefsParser.seq.parse(fetchResultSet(procDefsQuery))
    parsedProcedures.groupBy(_.name).mapValues(_.head)
  }

  def fetchUserDefinedCompoundTypes()(implicit c: Connection): Map[Int, DbCompoundType] = {
    val parsedTypes = typeParser.seq.parse(fetchResultSet(typeQuery))
    parsedTypes.groupBy(_.typeId).mapValues(_.head)
  }

  def fetchCompoundTypeAttrs(compoundTypeOid: Int)(implicit c: Connection): Option[IndexedSeq[DbAttrDef]] = {
    // The supplied type OID may be an OID of an array.
    // Try getting attributes of the argument OID first.
    // In case of missing attributes consider the supplied OID an array.
    // Fetch an OID of its elements and return attributes of an element OID.
    val maybeScalarAttrs = fetchAttrs(compoundTypeOid)
    if (maybeScalarAttrs.isDefined) maybeScalarAttrs else {
      val arrayTypes = fetchUserDefinedCompoundTypes().values.filter(_.arrayOid > 0)
      val typesMap = arrayTypes.groupBy(_.arrayOid).mapValues(_.head)
      typesMap.get(compoundTypeOid) flatMap { elemTypeDef =>
        fetchAttrs(elemTypeDef.typeId)
      }
    }
  }

  private def fetchAttrs(compoundTypeOid: Int)(implicit c: Connection): Option[IndexedSeq[DbAttrDef]] = {
    val query = attributeDefsQueryByOid(compoundTypeOid)
    val parsedAttributes = attrDefsParser.seq.parse(fetchResultSet(query))
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
    fetchCompoundTypeAttrs(dbDef.argTypes(param)) match {
      case None => Some(Seq(CantGetTypeAttrs(dbDef.argTypes(param))))
      case Some(attrDefs) if attrDefs.size == compoundSize => checkCompoundTypeAttrs(attrDefs, compoundTraits)
      case Some(attrDefs) => Some(Seq(AttrSizeMismatch(attrDefs.size, compoundSize)))
    }
  }

  private[calldb] def checkCompoundTypeAttrs(attrDefs: IndexedSeq[DbAttrDef], traits: CompoundTypeTraits): Result = {
    /**
      * Procedure compound parameters are supplied via SQL ROW() operator, which accepts only positional arguments.
      * Thus, code and database attributes must match exactly.
      */
    val accumErrors = new mutable.ArrayBuffer[ParamCheckError]
    for (((attrDef, columnTraits), index) <- attrDefs.zip(traits.columnsTraits).zipWithIndex) {
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
    val optCompoundTypeAttrs = fetchCompoundTypeAttrs(dbCompoundType.typeId)
    if (optCompoundTypeAttrs.isEmpty)
      return Some(Seq(FailedToFetchAttrs(dbCompoundType)))

    // TODO: Why an attempt to destructure this in for-comprehension does not compile?
    val compoundTypeAttrs: IndexedSeq[DbAttrDef] = optCompoundTypeAttrs.get
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
