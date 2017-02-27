package com.github.mirabout.calldb

import org.specs2.mutable._

trait EntityParamsDefTestSupport {
  // Just for testing, do not use it for real-world pg data (requires proper state handling and tags symmetry check)
  def replacePgDollarQuotes(input: String): String = {
    input.replaceAll("\\$[^\\$]*\\$", "'")
  }

  case class DummyEntity(name: String, latitude: Long, longitude: Long, altitude: Option[Long])

  class DummyEntityTable extends GenericTable with WithAllColumns[DummyEntity] {
    val Name = TableColumn.apply('name, (d: DummyEntity) => d.name, "DE")
    val Latitude = TableColumn.apply('latitude, (d: DummyEntity) => d.latitude, "DE")
    val Longitude = TableColumn.apply('longitude, (d: DummyEntity) => d.longitude, "DE")
    val Altitude = TableColumn.apply('altitude, (d: DummyEntity) => d.altitude, "DE")

    val allColumns = IndexedSeq(Name, Latitude, Longitude, Altitude)

    // Column indices should be set based on database metadata. For testing purposes we set it manually
    for ((column, index) <- allColumns.zipWithIndex)
      column.columnIndex = index
  }

  object DummyEntityTable extends DummyEntityTable
}

class EntityParamsDefTest extends Specification with EntityParamsDefTestSupport {

  "EntityParamsDef" should {

    "provide entity compound database type" in {
      val epd = EntityParamsDef(DummyEntityTable.allColumns, TableName("tDummy"))
      val columnsTraits = epd.typeTraits.columnsTraits

      columnsTraits must beDefinedAt(0, 1, 2, 3)

      columnsTraits(0).asOptBasic must beSome
      columnsTraits(0).asOptBasic.get.isNullable must beFalse
      columnsTraits(0).asOptBasic.get.storedInType must beOneOf(PgType.Varchar, PgType.Text)

      columnsTraits(1).asOptBasic must beSome
      columnsTraits(1).asOptBasic.get.isNullable must beFalse
      columnsTraits(1).asOptBasic.get.storedInType must_=== PgType.Bigint

      columnsTraits(2).asOptBasic must beSome
      columnsTraits(2).asOptBasic.get.isNullable must beFalse
      columnsTraits(2).asOptBasic.get.storedInType must_=== PgType.Bigint

      columnsTraits(3).asOptBasic must beSome
      columnsTraits(3).asOptBasic.get.isNullable must beTrue
      columnsTraits(3).asOptBasic.get.storedInType must_=== PgType.Bigint
    }

    "encode entity as a procedure parameter" in {

      val epd = EntityParamsDef(DummyEntityTable.allColumns, TableName("tDummy"))

      val actualEncoded1 = epd.encodeParam(DummyEntity("Dummy", 48, 44, None))
      replacePgDollarQuotes(actualEncoded1).toLowerCase must_== "entity := row('dummy',48,44,null)::tdummy"
      val actualEncoded2 = epd.encodeParam(DummyEntity("Dummy", 48, 44, Some(102)))
      replacePgDollarQuotes(actualEncoded2).toLowerCase must_== "entity := row('dummy',48,44,102)::tdummy"
    }
  }
}

class EntitiesArrayParamsDefTest extends Specification with EntityParamsDefTestSupport {

  "EntitiesArrayParamsDef" should {

    "provide entities array compound database type" in {
      val epd = EntitiesArrayParamsDef(DummyEntityTable.allColumns, TableName("tDummy"))
      val columnsTraits = epd.typeTraits.columnsTraits
      epd.typeTraits.rowTypeTraits.columnsTraits must_== columnsTraits

      columnsTraits must beDefinedAt(0, 1, 2, 3)

      columnsTraits(0).asOptBasic must beSome
      columnsTraits(0).asOptBasic.get.isNullable must beFalse
      columnsTraits(0).asOptBasic.get.storedInType must beOneOf(PgType.Varchar, PgType.Text)

      columnsTraits(1).asOptBasic must beSome
      columnsTraits(1).asOptBasic.get.isNullable must beFalse
      columnsTraits(1).asOptBasic.get.storedInType must_=== PgType.Bigint

      columnsTraits(2).asOptBasic must beSome
      columnsTraits(2).asOptBasic.get.isNullable must beFalse
      columnsTraits(2).asOptBasic.get.storedInType must_=== PgType.Bigint

      columnsTraits(3).asOptBasic must beSome
      columnsTraits(3).asOptBasic.get.isNullable must beTrue
      columnsTraits(3).asOptBasic.get.storedInType must_=== PgType.Bigint
    }

    "encode entities as a procedure parameter" in {
      val epd = EntitiesArrayParamsDef(DummyEntityTable.allColumns, TableName("tDummy"))

      val entities = Seq(
        DummyEntity("Dummy1", 48, 44, None),
        DummyEntity("Dummy2", 48, 44, Some(102)),
        DummyEntity("Dummy3", 48, 44, None))

      val expectedEncodedEntities1 =
        "entities := ARRAY[" +
        "ROW('Dummy1',48,44,NULL)::tDummy," +
        "ROW('Dummy2',48,44,102)::tDummy," +
        "ROW('Dummy3',48,44,NULL)::tDummy" +
        "]"

      val actualEncodedEntities1 = epd.encodeParam(entities)
      val actualEncodedEntities2 = epd.encodeParam(Seq())

      replacePgDollarQuotes(actualEncodedEntities1).toLowerCase must_=== expectedEncodedEntities1.toLowerCase()
      replacePgDollarQuotes(actualEncodedEntities2).toLowerCase must_=== "entities := ARRAY[]".toLowerCase()
    }
  }
}
