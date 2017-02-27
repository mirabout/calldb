package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection
import java.util.UUID
import org.specs2.mutable._

class TableColumnsCheckerTest extends Specification {
  sequential

  private case class DummyWithPK(
    storeId: UUID,
    foreignId: UUID,
    description: String,
    weight: Option[Double],
    cost: Option[Double],
    images: Option[Traversable[Array[Byte]]])

  private case class DummyNoPK(
    latitude: Double,
    longitude: Double,
    altitude: Option[Double],
    gpsTime: Long,
    attributes: Option[Map[String, Option[String]]])

  private class DummyWithPKTable extends GenericEntityTable[DummyWithPK] {
    lazy val StoreId = column('storeId, _.storeId)
    lazy val ForeignId = column('foreignId, _.foreignId)
    lazy val Description = column('description, _.description)
    lazy val Weight = column('weight, _.weight)
    lazy val Cost = column('cost, _.cost)
    lazy val Images = column('images, _.images)

    override def tableName = TableName("tDummyWithPK")
    override def entityParser = ???

    _allColumns = IndexedSeq(StoreId, ForeignId, Description, Weight, Cost, Images)
    _keyColumns = IndexedSeq(StoreId, ForeignId)
  }

  private object DummyWithPKTable extends DummyWithPKTable

  private class DummyNoPKTable extends GenericEntityTable[DummyNoPK] {
    lazy val Latitude = column('latitude, _.latitude)
    lazy val Longitude = column('longitude, _.longitude)
    lazy val Altitude = column('altitude, _.altitude)
    lazy val GpsTime = column('gpsTime, _.gpsTime)
    lazy val Attributes = column('attributes, _.attributes)

    override def tableName = TableName("tDummyNoPK")
    override def entityParser = ???

    _allColumns = IndexedSeq(Latitude, Longitude, Altitude, GpsTime, Attributes)
    _keyColumns = IndexedSeq()
  }

  private object DummyNoPKTable extends DummyNoPKTable

  private def newTableColumnsChecker[E](table: GenericEntityTable[E])(implicit c: Connection) =
    new TableColumnsChecker[E](table.tableName)

  private def getOrFetchColumnTypeOid(column: TableColumn[_, _])(implicit c: Connection): Int = {
    column.typeTraits.storedInType.getOrFetchOid() match {
      case Some(oid) => oid.exactOid
      case None =>
        // This error can lead to goosebumps for non-experienced users, describe a solution
        if (column.typeTraits.storedInType == PgType.Hstore)
          throw new AssertionError(s"Can't get column type traits for $column, have you installed HStore extension?")
        throw new AssertionError(s"Can't get column type traits for $column")
    }
  }

  private def mkColumnDef(column: TableColumn[_, _], number: Int)(implicit c: Connection) = DbColumnDef(
    table = column.tableName.get.toLowerCase,
    name = column.name.toLowerCase,
    typeOid = getOrFetchColumnTypeOid(column),
    isNullable = column.typeTraits.isNullable,
    number = number)

  class WithTestEnvironment extends WithTestConnectionAndSqlExecuted("TableColumnsCheckerTest")

  "TableColumnsChecker" should {

    "run these examples sequentially" >> {
      "fetch columns defs for a given table from database" in new WithTestEnvironment {

        val expectedDefs1 = Traversable(
          mkColumnDef(DummyWithPKTable.StoreId, 0),
          mkColumnDef(DummyWithPKTable.ForeignId, 1),
          mkColumnDef(DummyWithPKTable.Description, 2),
          mkColumnDef(DummyWithPKTable.Weight, 3),
          mkColumnDef(DummyWithPKTable.Cost, 4),
          mkColumnDef(DummyWithPKTable.Images, 5))

        val expectedDefs2 = Traversable(
          mkColumnDef(DummyNoPKTable.Latitude, 0),
          mkColumnDef(DummyNoPKTable.Longitude, 1),
          mkColumnDef(DummyNoPKTable.Altitude, 2),
          mkColumnDef(DummyNoPKTable.GpsTime, 3),
          mkColumnDef(DummyNoPKTable.Attributes, 4))

        val readActualDefs1 = TableColumnsChecker.fetchColumnsDefs(TableName("tDummyWithPK"))
        val readActualDefs2 = TableColumnsChecker.fetchColumnsDefs(TableName("tDummyNoPK"))

        readActualDefs1 must beSome
        readActualDefs2 must beSome

        val (Some(actualDefs1), Some(actualDefs2)) = (readActualDefs1, readActualDefs2)

        // Ensure we may iterate over names and never miss a column
        actualDefs1.map(_.name).toSet must_=== expectedDefs1.map(_.name).toSet
        actualDefs2.map(_.name).toSet must_=== expectedDefs2.map(_.name).toSet

        def mkExpectedTestResult(expectedDefs: Traversable[DbColumnDef]): Set[(String, Boolean)] = {
          (for (theDef <- expectedDefs)
            yield (theDef.name, true)).toSet
        }

        def mkActualTestResult(actualDefs: Traversable[DbColumnDef], expectedDefs: Traversable[DbColumnDef]): Set[(String, Boolean)] = {
          val expectedDefsByName = (for (d <- expectedDefs) yield (d.name, d)).toMap
          (for (actualDef <- actualDefs; expectedDef = expectedDefsByName(actualDef.name))
            yield (actualDef.name, actualDef.conforms(expectedDef))).toSet
        }

        mkActualTestResult(actualDefs1, expectedDefs1) must_=== mkExpectedTestResult(expectedDefs1)
        mkActualTestResult(actualDefs2, expectedDefs2) must_=== mkExpectedTestResult(expectedDefs2)
      }

      "fetch primary key for a given table from database" in new WithTestEnvironment {
        val expectedPK = Some(PrimaryKey("tdummywithpk", IndexedSeq("storeid", "foreignid")))
        TableColumnsChecker.fetchPrimaryKey(TableName("tDummyWithPK")) must_=== expectedPK
        TableColumnsChecker.fetchPrimaryKey(TableName("tDummyNoPK")) must beNone
      }

      "detect column names mismatch when some columns names are present in code but absent in DB" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyWithPKTable)
        val dbDefs: Traversable[DbColumnDef] = Traversable(
          mkColumnDef(DummyWithPKTable.StoreId, 0),
          mkColumnDef(DummyWithPKTable.ForeignId, 1),
          mkColumnDef(DummyWithPKTable.Description, 2))
        val checkResult = checker.checkAllColumns(DummyWithPKTable.allColumns, dbDefs)
        checkResult must beSome
      }

      "detect column names mismatch when some columns names are absent in code but present in DB" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyNoPKTable)
        val dbDefs: Traversable[DbColumnDef] = Traversable(
          mkColumnDef(DummyNoPKTable.Latitude, 0),
          mkColumnDef(DummyNoPKTable.Longitude, 1),
          mkColumnDef(DummyNoPKTable.Altitude, 2),
          mkColumnDef(DummyNoPKTable.GpsTime, 3),
          mkColumnDef(DummyNoPKTable.Attributes, 4),
          DbColumnDef("tdummynopk", "id", PgType.Uuid.pgNumericOid.get, isNullable = false, 5))
        val checkResult = checker.checkAllColumns(DummyNoPKTable.allColumns, dbDefs)
        checkResult must beSome
      }

      "detect mismatched column types" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyWithPKTable)
        val dbDefs = Traversable(
          mkColumnDef(DummyWithPKTable.StoreId, 0),
          mkColumnDef(DummyWithPKTable.ForeignId, 1),
          mkColumnDef(DummyWithPKTable.Description, 2),
          mkColumnDef(DummyWithPKTable.Weight, 3),
          mkColumnDef(DummyWithPKTable.Cost, 4),
          mkColumnDef(DummyWithPKTable.Images, 5).copy(typeOid = PgType.TextArray.pgNumericOid.get))
        checker.checkColumnsTypes(DummyWithPKTable.allColumns, (for (d <- dbDefs) yield (d.name, d)).toMap) must beSome
      }

      "yield successful columns type check result for well-formed columns" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyNoPKTable)
        val dbDefs = Traversable(
          mkColumnDef(DummyNoPKTable.Latitude, 0),
          mkColumnDef(DummyNoPKTable.Longitude, 1),
          mkColumnDef(DummyNoPKTable.Altitude, 2),
          mkColumnDef(DummyNoPKTable.GpsTime, 3),
          mkColumnDef(DummyNoPKTable.Attributes, 4))
        checker.checkColumnsTypes(DummyNoPKTable.allColumns, (for (d <- dbDefs) yield (d.name, d)).toMap) must beNone
      }

      "inject columns indices based on DB attribute numbers" in new WithTestEnvironment {
        // Make a local instance to prevent global state change
        val table = new DummyWithPKTable()
        val checker = newTableColumnsChecker(table)
        val dbDefs = Traversable(
          mkColumnDef(DummyWithPKTable.StoreId, 0),
          mkColumnDef(DummyWithPKTable.ForeignId, 1),
          mkColumnDef(DummyWithPKTable.Description, 2),
          mkColumnDef(DummyWithPKTable.Weight, 3),
          mkColumnDef(DummyWithPKTable.Cost, 4),
          mkColumnDef(DummyWithPKTable.Images, 5))
        val defsByNameMap = (for (d <- dbDefs) yield (d.name, d)).toMap
        assert(table.allColumns.forall(_.columnIndex == -1))
        checker.injectColumns(table.allColumns, defsByNameMap)
        val expectedColumnIndices = for (d <- dbDefs) yield (d.name, d.number)
        val actualColumnIndices = for (c <- table.allColumns) yield (c.name.toLowerCase, c.columnIndex)
        actualColumnIndices.toSet must_=== expectedColumnIndices.toSet
      }

      "detect key columns completely absent in code but present in DB" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyWithPKTable)
        val checkResult = checker.checkKeyColumns(IndexedSeq())
        checkResult must beSome
      }

      "detect key columns partially absent in code but present in DB" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyWithPKTable)
        val checkResult = checker.checkKeyColumns(DummyWithPKTable.keyColumns.drop(1))
        checkResult must beSome
      }

      "detect key columns present in code but completely absent in DB" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyWithPKTable)
        val checkResult = checker.checkKeyColumns(DummyWithPKTable.keyColumns, None)
        checkResult must beSome
      }

      "detect key columns present in code but partially absent in DB" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyWithPKTable)
        val malformedPrimaryKey = PrimaryKey("tdummywithpk", IndexedSeq("storeid", "foreignid", "description"))
        val checkResult = checker.checkKeyColumns(DummyWithPKTable.keyColumns, Some(malformedPrimaryKey))
        checkResult must beSome
      }

      "yield successful check result for empty key columns if the DB table does not have keys" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyNoPKTable)
        assert(DummyNoPKTable.keyColumns.isEmpty)
        checker.checkKeyColumns(DummyNoPKTable.keyColumns) must beNone
      }

      "yield successful check result for non-empty key columns if the DB table keys match" in new WithTestEnvironment {
        val checker = newTableColumnsChecker(DummyWithPKTable)
        assert(DummyWithPKTable.keyColumns.nonEmpty)
        checker.checkKeyColumns(DummyWithPKTable.keyColumns) must beNone
      }
    }
  }
}
