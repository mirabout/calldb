package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.mutable

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
       |SELECT lower(table_name), lower(column_name), pg_type.oid::integer, is_nullable='YES', ordinal_position
       |FROM information_schema.columns
       | JOIN pg_type
       |   ON information_schema.columns.udt_name = pg_type.typname
       |WHERE lower(table_name) = lower('${tableName.exactName}') AND table_schema = 'public'
       |ORDER BY ordinal_position
    """.stripMargin
  }

  private val columnDefParser = new RowDataParser[DbColumnDef](row => {
    (row(0), row(1), row(2), row(3), row(4)) match {
      case (table: String, name: String, oid: Int, isNullable: Boolean, number: Int) =>
        DbColumnDef(table, name, oid, isNullable, number - 1)
    }
  }) {
    def expectedColumnsNames: Option[IndexedSeq[String]] = None // Unused
  }

  private def mkPrimaryKeysQuery(tableName: TableName): String = {
    s"""
       |SELECT lower(a.attname)
       |FROM pg_index i
       |  JOIN pg_attribute a
       |    ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
       |WHERE i.indrelid = lower('${tableName.exactName}')::regclass AND i.indisprimary
     """.stripMargin
  }

  private[calldb] def fetchColumnsDefs(tableName: TableName)(implicit c: Connection): Option[IndexedSeq[DbColumnDef]] = {
    val queryResult = Await.result(c.sendQuery(mkColumnsQuery(tableName)), 5.seconds)
    val rawColumnsDefs = columnDefParser.*.parse(queryResult.rows.get)
    if (rawColumnsDefs.nonEmpty) Some(rawColumnsDefs) else None
  }

  private[calldb] def fetchPrimaryKey(tableName: TableName)(implicit c: Connection): Option[PrimaryKey] = {
    val queryResult = Await.result(c.sendQuery(mkPrimaryKeysQuery(tableName)), 5.seconds)
    val rawColumnNames = RowDataParser.string(0).*.parse(queryResult.rows.get)
    if (rawColumnNames.nonEmpty) Some(PrimaryKey(tableName.exactName.toLowerCase, rawColumnNames)) else None
  }
}

class TableColumnsChecker[E](tableName: TableName)(implicit c: Connection) {

  private[calldb] def fetchColumnsDefs() = TableColumnsChecker.fetchColumnsDefs(tableName)
  private[calldb] def fetchPrimaryKey() = TableColumnsChecker.fetchPrimaryKey(tableName)

  private def mkError(message: String): String = s"${tableName.exactName}: $message"

  def checkAndInjectColumns(allColumns: Traversable[TableColumn[E, _]]): Either[Seq[String], Traversable[TableColumn[E, _]]] = {
    checkAllColumns(allColumns) match {
      case Left(errors) => Left(errors)
      case Right(columnsDefs) => {
        injectColumns(allColumns, columnsDefs)
        Right(allColumns)
      }
    }
  }

  private[calldb] def checkAllColumns(allColumns: Traversable[TableColumn[E, _]]): Either[Seq[String], Map[String, DbColumnDef]] = {
    fetchColumnsDefs() match {
      case Some(columnsDefs) => {
        checkAllColumns(allColumns, columnsDefs) match {
          case Some(errors) => Left(errors)
          case None => Right(columnsDefs.groupBy(_.name.toLowerCase()).mapValues(_.head))
        }
      }
      case None => {
        Left(Seq(mkError(s"Can't fetch DB columns defs")))
      }
    }
  }

  private[calldb] def checkAllColumns(
    allColumns: Traversable[TableColumn[E, _]],
    columnsDefs: Traversable[DbColumnDef])
  : Option[Seq[String]] = {
    val codeColumnsNames: Set[String] = allColumns.map(_.name.toLowerCase).toSet
    val dbColumnsNames: Set[String] = columnsDefs.map(_.name.toLowerCase).toSet

    if (codeColumnsNames != dbColumnsNames) {
      val errors = new mutable.ArrayBuffer[String]
      val onlyInCode = codeColumnsNames diff dbColumnsNames
      val onlyInDb = dbColumnsNames diff codeColumnsNames
      if (onlyInCode.nonEmpty) {
        errors += mkError(s"These columns are present only in code, not in DB: $onlyInCode")
      }
      if (onlyInDb.nonEmpty) {
        errors += mkError(s"These columns are present only in DB, not in code: $onlyInDb")
      }
      Some(errors)
    } else {
      checkColumnsTypes(allColumns, columnsDefs.groupBy(_.name.toLowerCase).mapValues(_.head))
    }
  }

  private[calldb] def checkColumnsTypes(
    allColumns: Traversable[TableColumn[E, _]],
    defsByName: Map[String, DbColumnDef])
  : Option[Seq[String]] = {
    val accumErrors = new mutable.ArrayBuffer[String]
    for (codeColumn <- allColumns) {
      def mkColumnError(message: String) = mkError(s"Column $codeColumn: $message")
      // We have ensured this won't fail
      val dbColumnDef = defsByName(codeColumn.name.toLowerCase)
      val codeTypeTraits = codeColumn.typeTraits.asOptBasic.get
      codeTypeTraits.storedInType.getOrFetchOid() match {
        case None => {
          accumErrors += mkColumnError(s"Can't get type OID for $codeTypeTraits")
        }
        case Some(codeTypeOid) => {
          if (!codeTypeOid.conforms(dbColumnDef.typeOid)) {
            val codeTypeRepr = s"OID=${codeTypeOid.exactOid}, name=${codeTypeTraits.storedInType.sqlName}"
            val dbTypeRepr = s"OID=${dbColumnDef.typeOid}, name=${PgType.fetchTypeName(dbColumnDef.typeOid).get}"
            accumErrors += mkColumnError(s"Code type $codeTypeRepr does conform to DB type $dbTypeRepr")
          } else if (codeTypeTraits.isNullable != dbColumnDef.isNullable) {
            val isCodeColumnNullable = if (codeTypeTraits.isNullable) "is" else "is not"
            val isDbColumnNullable = if (dbColumnDef.isNullable) "is" else "is not"
            accumErrors += mkColumnError(s"Code column $isCodeColumnNullable nullable, DB column $isDbColumnNullable")
          }
        }
      }
    }
    if (accumErrors.nonEmpty) Some(accumErrors) else None
  }

  private[calldb] def injectColumns(allColumns: Traversable[TableColumn[E, _]], dbDefsByName: Map[String, DbColumnDef]): Unit = {
    for (column <- allColumns) {
      column.columnIndex = dbDefsByName(column.name.toLowerCase).number
    }
  }

  def checkKeyColumns(keyColumns: Traversable[TableColumn[E, _]]): Option[Seq[String]] =
    checkKeyColumns(keyColumns, fetchPrimaryKey())

  private[calldb] def checkKeyColumns(keyColumns: Traversable[TableColumn[E, _]], dbPrimaryKey: Option[PrimaryKey]): Option[Seq[String]] = {
    dbPrimaryKey match {
      case Some(primaryKey) =>
        checkExpectedSomeKeyColumns(keyColumns, primaryKey)
      case None =>
        checkExpectedEmptyKeyColumns(keyColumns)
    }
  }

  private[calldb] def checkExpectedSomeKeyColumns(keyColumns: Traversable[TableColumn[E, _]], primaryKey: PrimaryKey): Option[Seq[String]] = {
    if (keyColumns.isEmpty)
      return Some(Seq(mkError(s"Code key columns are absent, DB key columns ${primaryKey.columns} are present")))

    val codeColumnsSet = keyColumns.map(_.name.toLowerCase).toSet
    val dbColumnsSet = primaryKey.columns.map(_.toLowerCase).toSet
    if (codeColumnsSet == dbColumnsSet)
      return None

    val errors = new mutable.ArrayBuffer[String]
    val onlyInCode = codeColumnsSet diff dbColumnsSet
    val onlyInDb = dbColumnsSet diff codeColumnsSet
    if (onlyInCode.nonEmpty) {
      errors += mkError(s"These key columns are present only in code: $onlyInCode")
    }
    if (onlyInDb.nonEmpty) {
      errors += mkError(s"These key columns are present only in DB: $onlyInDb")
    }
    Some(errors)
  }

  private[calldb] def checkExpectedEmptyKeyColumns(keyColumns: Traversable[TableColumn[E, _]]): Option[Seq[String]] = {
    if (keyColumns.isEmpty) None else {
      Some(Seq(mkError(s"Code key columns $keyColumns are present, DB key columns are absent")))
    }
  }
}
