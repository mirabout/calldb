package com.github.mirabout.calldb

import com.github.mauricio.async.db.{ResultSet, RowData}
import java.util.UUID
import org.specs2.mutable._

/**
  * Tests primitive [[RowDataParser]]s defined in [[RowDataParser]] object
  */
class RowDataParsersTest extends Specification {
  def mkDummyResultSet(value: Any): ResultSet =
    DummyResultSet(Seq(DummyRowData(1, Seq(value), IndexedSeq())), IndexedSeq())

  def mkDummyResultSet(value: Any, name: String): ResultSet =
    DummyResultSet(Seq(DummyRowData(1, Seq(value), IndexedSeq(name))), IndexedSeq(name))

  "RowDataParser" should {
    "provide single-row parsers for Boolean type" in {
      RowDataParser.bool(0).!.parse(mkDummyResultSet(true)) must_=== true
      RowDataParser.bool("foo").!.parse(mkDummyResultSet(false, "foo")) must_=== false
    }

    "provide single-row parsers for Int type" in {
      RowDataParser.int(0).!.parse(mkDummyResultSet(1337)) must_=== 1337
      RowDataParser.int("foo").!.parse(mkDummyResultSet(1337, "foo")) must_=== 1337
    }

    "provide single-row parsers for Long type" in {
      RowDataParser.long(0).!.parse(mkDummyResultSet(42L)) must_=== 42L
      RowDataParser.long("foo").!.parse(mkDummyResultSet(42L, "foo")) must_=== 42L
    }

    "provide single-row parsers for Double type" in {
      RowDataParser.double(0).!.parse(mkDummyResultSet(0.0)) must_=== 0.0
      RowDataParser.double("foo").!.parse(mkDummyResultSet(0.0, "foo")) must_=== 0.0
    }

    "provide single-row parsers for String type" in {
      RowDataParser.string(0).!.parse(mkDummyResultSet("Hello, world!")) must_=== "Hello, world!"
      RowDataParser.string("foo").!.parse(mkDummyResultSet("Hello, world!", "foo")) must_=== "Hello, world!"
    }

    "provide single-row parsers for UUID type" in {
      val (uuid1, uuid2) = (UUID.randomUUID(), UUID.randomUUID())
      RowDataParser.uuid(0).!.parse(mkDummyResultSet(uuid1)) must_=== uuid1
      RowDataParser.uuid("foo").!.parse(mkDummyResultSet(uuid2, "foo")) must_=== uuid2
    }
  }
}
