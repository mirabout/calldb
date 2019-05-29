package com.github.mirabout.calldb

import com.github.mauricio.async.db.{Connection, ResultSet, RowData}

import java.util.Locale

import scala.concurrent.Await
import scala.concurrent.duration._

private case class DbColumnDef(table: String, name: String, typeOid: Int, isNullable: Boolean, number: Int) {
  override def equals(o: Any) =
    throw new AssertionError("Do not use equals() for DbColumnDef (numeric OIDs may conform even being different)")

  // This stuff is frankly atrocious but suits testing purposes
  private[calldb] def conforms(that: DbColumnDef)(implicit c: Connection): Boolean = {
    val thisSqlName: String = PgType.fetchTypeName(this.typeOid)
      .getOrElse(throw new IllegalStateException(s"Can't fetch type name for numeric OID ${this.typeOid}"))
    val thisPgType: PgType = PgType.typeByName(thisSqlName)
      .getOrElse(throw new IllegalStateException(s"Can't get type by name $thisSqlName"))
    // TypeOid is not a plain numeric OID, it may be used for not just type equality but type conformance comparison
    val thisTypeOid: TypeOid = thisPgType.getOrFetchOid()
      .getOrElse(throw new IllegalStateException(s"Can't get or fetch TypeOid"))
    thisTypeOid.conforms(that.typeOid)
  }
}

private sealed abstract class TableConstraint(val table: String)
// Only primary keys check is performed
private case class PrimaryKey(override val table: String, columns: IndexedSeq[String]) extends TableConstraint(table) {
  override def equals(o: Any): Boolean = {
    o match {
      case that: PrimaryKey => table.equalsIgnoreCase(that.table) && columns.equals(that.columns)
      case _ => false
    }
  }
  override def hashCode: Int = 17 + 31 * table.toLowerCase.hashCode + 31 * 31 * columns.hashCode()
}

object TableColumnsChecker {
  private def mkColumnsQuery(tableName: TableName): String = {
    s"""
       |select lower(table_name), lower(column_name), pg_type.oid::integer, is_nullable='YES', ordinal_position
       |from information_schema.columns
       | join pg_type
       |   on information_schema.columns.udt_name = pg_type.typname
       |where lower(table_name) = lower('${tableName.exactName}') and table_schema = 'public'
       |order by ordinal_position
    """.stripMargin
  }

  private def parseDefs(row: RowData) = {
    (row(0), row(1), row(2), row(3), row(4)) match {
      case (table: String, name: String, oid: Int, isNullable: Boolean, number: Int) =>
        DbColumnDef(table, name, oid, isNullable, number - 1)
    }
  }

  private val columnDefParser = new RowDataParser[DbColumnDef](parseDefs) {
    def expectedColumnsNames: Option[IndexedSeq[String]] = None // Unused
  }

  private def mkPrimaryKeysQuery(tableName: TableName): String = {
    s"""
       |select lower(a.attname)
       |from pg_index i
       |  join pg_attribute a
       |    on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
       |where i.indrelid = lower('${tableName.exactName}')::regclass and i.indisprimary
     """.stripMargin
  }

  private def fetchResultSet(query: String)(implicit c: Connection): ResultSet =
    Await.result(c.sendQuery(query), 5.seconds).rows.get

  private[calldb] def fetchColumnsDefs(tableName: TableName)(implicit c: Connection): Option[IndexedSeq[DbColumnDef]] = {
    val rawDefs = columnDefParser.seq.parse(fetchResultSet(mkColumnsQuery(tableName)))
    if (rawDefs.nonEmpty) Some(rawDefs) else None
  }

  private[calldb] def fetchPrimaryKey(tableName: TableName)(implicit c: Connection): Option[PrimaryKey] = {
    val rawNames = RowDataParser.string(0).seq.parse(fetchResultSet(mkPrimaryKeysQuery(tableName)))
    if (rawNames.nonEmpty) Some(PrimaryKey(tableName.exactName.toLowerCase, rawNames)) else None
  }
}

class TableColumnsChecker[E](tableName: TableName)(implicit c: Connection) {
  import TableCheckError._

  private[calldb] def fetchColumnsDefs() = TableColumnsChecker.fetchColumnsDefs(tableName)
  private[calldb] def fetchPrimaryKey() = TableColumnsChecker.fetchPrimaryKey(tableName)

  def checkAndInjectColumns(allColumns: Traversable[TableColumn[E, _]]): Either[Seq[TableCheckError], Traversable[TableColumn[E, _]]] = {
    checkAllColumns(allColumns) match {
      case Left(errors) => Left(errors)
      case Right(columnsDefs) => {
        injectColumns(allColumns, columnsDefs)
        Right(allColumns)
      }
    }
  }

  private[calldb] def checkAllColumns(allColumns: Traversable[TableColumn[E, _]]): Either[Seq[TableCheckError], Map[String, DbColumnDef]] = {
    fetchColumnsDefs() match {
      case Some(columnsDefs) => checkAllColumns(allColumns, columnsDefs) match {
        case Some(errors) => Left(errors)
        case None => Right(columnsDefs.groupBy(_.name.toLowerCase()).mapValues(_.head))
      }
      case None => Left(Seq(CantFetchDBDefs()))
    }
  }

  private[calldb] def checkAllColumns(allColumns: Traversable[TableColumn[E, _]], defs: Traversable[DbColumnDef]) = {
    val codeColumnsNames: Traversable[String] = allColumns.map(_.name)
    val dbColumnsNames: Traversable[String] = defs.map(_.name)
    MissingColumns.fromNames(codeColumnsNames, dbColumnsNames) match {
      case someErrors @ Some(_) => someErrors
      case _ => checkColumnsTypes(allColumns, defs)
    }
  }

  def checkColumnType(codeColumn: TableColumn[E, _], defsByName: Map[String, DbColumnDef]): Option[TableCheckError] = {
    val dbColumnDef = defsByName(codeColumn.name.toLowerCase(Locale.ROOT))
    val codeTypeTraits = codeColumn.typeTraits.asOptBasic.get
    codeTypeTraits.storedInType.getOrFetchOid() match {
      case None => Some(CantGetTypeOid(codeTypeTraits.storedInType))
      case Some(oid) if !oid.conforms(dbColumnDef.typeOid) => Some(ColumnTypeMismatch(codeTypeTraits, dbColumnDef.typeOid))
      case Some(_) if codeTypeTraits.isNullable != dbColumnDef.isNullable => Some(ColumnNullityMismatch(codeTypeTraits))
      case _ => None
    }
  }

  private[calldb] def checkColumnsTypes(allColumns: Traversable[TableColumn[E, _]], defs: Traversable[DbColumnDef]) = {
    val defsByName = defs.groupBy(_.name.toLowerCase(Locale.ROOT)).mapValues(_.head)
    val accumErrors = for (column <- allColumns; e <- checkColumnType(column, defsByName)) yield e
    if (accumErrors.nonEmpty) Some(accumErrors.toSeq) else None
  }

  private[calldb] def injectColumns(allColumns: Traversable[TableColumn[E, _]], dbDefsByName: Map[String, DbColumnDef]): Unit = {
    for (column <- allColumns) {
      column.columnIndex = dbDefsByName(column.name.toLowerCase).number
    }
  }

  def checkKeyColumns(keyColumns: Traversable[TableColumn[E, _]]): Option[Seq[TableCheckError]] =
    checkKeyColumns(keyColumns, fetchPrimaryKey())

  private[calldb] def checkKeyColumns(keyColumns: Traversable[TableColumn[E, _]], dbPrimaryKey: Option[PrimaryKey]) = {
    dbPrimaryKey match {
      case Some(primaryKey) => MissingKeys.fromNames(keyColumns.map(_.name), primaryKey.columns)
      case None if keyColumns.isEmpty => None
      case None => Some(Seq(NoKeysInDB(keyColumns.map(_.name.toLowerCase(Locale.ROOT)).toSet)))
    }
  }
}

sealed abstract class TableCheckError {
  def description: String
}

object TableCheckError {
  case class CantFetchDBDefs() extends TableCheckError {
    def description: String = s"Can't fetch DB columns defs"
  }

  case class CantGetTypeOid(codeType: PgType) extends TableCheckError {
    def description: String = s"Can't retrieve a type OID for code type $codeType"
  }

  case class ColumnTypeMismatch(codeOid: PgType, dbTypeName: String) extends TableCheckError {
    def description: String = s"The code column of type $codeOid does not conform to the DB type $dbTypeName"
  }

  object ColumnTypeMismatch {
    def apply(codeTraits: BasicTypeTraits, dbOid: Int)(implicit c: Connection): ColumnTypeMismatch =
      ColumnTypeMismatch(codeTraits.storedInType, PgType.fetchTypeName(dbOid).get)
  }

  case class ColumnNullityMismatch(codeOid: PgType, isCodeColumnNullable: Boolean) extends TableCheckError {
    def description: String = {
      val desiredNullity = if (isCodeColumnNullable) "should be" else "shouldn't be"
      s"The column of type $codeOid $desiredNullity in the DB"
    }
  }

  object ColumnNullityMismatch {
    def apply(codeTraits: BasicTypeTraits): ColumnNullityMismatch =
      apply(codeTraits.storedInType, isCodeColumnNullable = codeTraits.isNullable)
  }

  case class NoKeysInCode(columnNames: Set[String]) extends TableCheckError {
    def description: String = s"There are key columns $columnNames in the DB while no keys are defined in the code"
  }

  case class NoKeysInDB(columnNames: Set[String]) extends TableCheckError {
    def description: String = s"There are key columns $columnNames in the code while no keys are defined in the DB"
  }

  sealed abstract class MissingSomeColumns(tag: String, names: Set[String], wherePresent: String, whereAbsent: String)
    extends TableCheckError {
    def description: String = s"$tag columns $names are present only in $wherePresent and not in $whereAbsent"
  }

  sealed class Companion[E1 <: MissingInCode, E2 <: MissingInDB](cons1: Set[String] => E1, cons2: Set[String] => E2) {
    def fromNames(codeNames: Traversable[String], dbNames: Traversable[String]): Option[Seq[MissingSomeColumns]] = {
      val codeNamesSet = codeNames.map(_.toLowerCase(Locale.ROOT)).toSet
      val dbNamesSet = dbNames.map(_.toLowerCase(Locale.ROOT)).toSet
      if (codeNamesSet == dbNamesSet) None else {
        def maybeSingleErr(set: Set[String], c: Set[String] => MissingSomeColumns) =
          if (set.nonEmpty) Seq(c(set)) else Seq()
        val missingInCodeSet = dbNamesSet diff codeNamesSet
        val missingInDBSet = codeNamesSet diff dbNamesSet
        Some(maybeSingleErr(missingInCodeSet, cons1) ++ maybeSingleErr(missingInDBSet, cons2))
      }
    }
  }

  sealed abstract class MissingInDB(tag: String, columnNames: Set[String])
    extends MissingSomeColumns(tag, columnNames, "the code", "the DB")

  sealed class MissingInCode(tag: String, columnNames: Set[String])
    extends MissingSomeColumns(tag, columnNames, "the DB", "the code")

  case class MissingColumnsInDB(columnNames: Set[String]) extends MissingInDB("Columns", columnNames)
  case class MissingColumnsInCode(columnNames: Set[String]) extends MissingInCode("Columns", columnNames)
  object MissingColumns extends Companion(MissingColumnsInCode, MissingColumnsInDB)

  case class MissingKeysInDB(keyColumnNames: Set[String]) extends MissingInDB("Keys", keyColumnNames)
  case class MissingKeysInCode(keyColumnNames: Set[String]) extends MissingInCode("Keys", keyColumnNames)
  object MissingKeys extends Companion(MissingKeysInCode, MissingKeysInDB)
}
