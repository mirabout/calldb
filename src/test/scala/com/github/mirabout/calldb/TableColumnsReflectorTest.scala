package com.github.mirabout.calldb

import java.util.UUID
import org.specs2.mutable._

class TableColumnsReflectorTest extends Specification {

  private case class Dummy(storeId: UUID, foreignId: UUID, latitude: Double, longitude: Double, altitude: Option[Double])

  private object DummyTable extends GenericEntityTable[Dummy] {

    @keyColumn val StoreId = column('storeId, _.storeId)
    @keyColumn val ForeignId = column('foreignId, _.foreignId)
    lazy val Latitude = column('latitude, _.latitude)
    lazy val Longitude = column('longitude, _.longitude)
    var Altitude = column('altitude, _.altitude)

    override def tableName = TableName("tDummy")
    override def entityParser = ??? // Unused

    // Do not call _allColumns and _keyColumns, they are not injected
    lazy val AllColumns: Set[TableColumn[Dummy,_]] = Set(StoreId, ForeignId, Latitude, Longitude, Altitude)
    lazy val KeyColumns: Set[TableColumn[Dummy,_]] = Set(StoreId, ForeignId)
  }

  "TableColumnsReflector" should {
    "reflect all columns" in {
      (new TableColumnsReflector).reflectAllColumns[Dummy](DummyTable) must_=== DummyTable.AllColumns
    }

    "reflect key columns" in {
      (new TableColumnsReflector).reflectKeyColumns[Dummy](DummyTable) must_=== DummyTable.KeyColumns
    }
  }
}
