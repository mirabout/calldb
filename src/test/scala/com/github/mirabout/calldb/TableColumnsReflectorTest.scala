package com.github.mirabout.calldb

import java.util.UUID

import org.specs2.mutable._

class TableColumnsReflectorTest extends Specification {
  import TableColumnsReflector.{reflectAllColumns, reflectKeyColumns}

  class BaseLocation(val latitude: Double, val longitude: Double)

  class Location3D(override val latitude: Double, override val longitude: Double, val altitude: Double)
    extends BaseLocation(latitude, longitude)

  class LocationAndTag(override val latitude: Double, override val longitude: Double, val tag: String)
    extends BaseLocation(latitude, longitude)

  class Meeting(override val latitude: Double, override val longitude: Double, val userId1: UUID, val userId2: UUID)
    extends BaseLocation(latitude, longitude)

  trait SyntheticColumnMixin[L <: BaseLocation] { self: GenericEntityTable[L] =>
    // Some random member for superclass reflection testing.
    // Note: there are language restrictions on some non-lazy val's
    // (as they might be treated as constructor local variables)
    @keyColumn lazy val SyntheticId = column('syntheticId, _ => UUID.randomUUID())
  }

  class BaseTable[L <: BaseLocation](name: String) extends GenericEntityTable[L] with SyntheticColumnMixin[L] {
    val tableName = TableName(name)
    def entityParser: RowDataParser[L] = ???

    var Latitude = column('latitude, _.latitude)
    val Longitude = column('longitude, _.longitude)

    // Avoid a name clash with _allColumns/allColumns
    def AllColumns: Set[TableColumn[_, _]] = Set(Latitude, Longitude, SyntheticId)
    // Avoid a name clash with _keyColumns/keyColumns
    def KeyColumns: Set[TableColumn[_, _]] = Set(SyntheticId)

    def allColumnNames: Set[String] = AllColumns.map(_.qualifiedName)
    def keyColumnNames: Set[String] = KeyColumns.map(_.qualifiedName)
  }

  object BaseTable extends BaseTable("tBase")

  class Location3DTable extends BaseTable[Location3D]("tLocation3D") {
    lazy val Altitude = column('altitude, _.altitude)
    override def AllColumns = super.AllColumns ++ Set(Altitude)
  }

  object Location3DTable extends Location3DTable

  class MeetingTable extends BaseTable[Meeting]("tMeeting") {
    @keyColumn val UserId1 = column('userId1, _.userId1)
    @keyColumn val UserId2 = column('userId2, _.userId2)
    override def AllColumns: Set[TableColumn[_, _]] = super.AllColumns ++ Set(UserId1, UserId2)
    override val KeyColumns: Set[TableColumn[_, _]] = super.KeyColumns ++ Set(UserId1, UserId2)
  }

  object MeetingTable extends MeetingTable

  class LocationAndTagTable extends BaseTable[LocationAndTag]("tLocationAndTag") {
    @keyColumn val Tag = column('tag, _.tag)
    @ignoredColumn val Ignored = TableColumn('ignored, 0L)
    override val AllColumns: Set[TableColumn[_, _]] = super.AllColumns ++ Set(Tag)
    override val KeyColumns: Set[TableColumn[_, _]] = super.KeyColumns ++ Set(Tag)
  }

  object LocationAndTagTable extends LocationAndTagTable

  private def reflectAllColumnNames(value: AnyRef): Set[String] =
    reflectAllColumns[Nothing](value).map(_.qualifiedName)

  private def reflectKeyColumnNames(value: AnyRef): Set[String] =
    reflectKeyColumns[Nothing](value).map(_.qualifiedName)

  "TableColumnsReflector" should {
    /*
    "reflect all columns" in {
      // Use anonymous subtypes intentionally to check supertype fields discovery
      reflectAllColumnNames(new BaseTable("tBase") {}) must_=== BaseTable.allColumnNames
      reflectAllColumnNames(new Location3DTable {}) must_=== Location3DTable.allColumnNames
      reflectAllColumnNames(new LocationAndTagTable {}) must_== LocationAndTagTable.allColumnNames
      reflectAllColumnNames(new MeetingTable {}) must_=== MeetingTable.allColumnNames
    }*/

    "reflect key columns" in {
      // Use anonymous subtypes intentionally to check supertype fields discovery
      //reflectKeyColumnNames(new BaseTable("tBase") {}) must_=== BaseTable.keyColumnNames
      reflectKeyColumnNames(new Location3DTable {}) must_=== Location3DTable.keyColumnNames
      reflectKeyColumnNames(new LocationAndTagTable {}) must_== LocationAndTagTable.keyColumnNames
      reflectKeyColumnNames(new MeetingTable {}) must_=== MeetingTable.keyColumnNames
    }
  }
}
