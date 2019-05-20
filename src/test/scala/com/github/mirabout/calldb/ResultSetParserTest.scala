package com.github.mirabout.calldb

import com.github.mauricio.async.db.{ResultSet, RowData}

import org.joda.time.DateTime
import org.specs2.mutable._

case class DummyResultSet(underlying: Seq[RowData], columnNames: IndexedSeq[String]) extends ResultSet {
  def length: Int = underlying.length
  def apply(idx: Int): RowData = underlying(idx)
}

case class DummyRowData(rowNumber: Int, underlying: Seq[Any], columnNames: IndexedSeq[String]) extends RowData {
  def length: Int = underlying.length
  def apply(columnNumber: Int): Any = underlying(columnNumber)
  def apply(columnName: String): Any = underlying(columnNames.indexOf(columnName))
}

trait RowParsersTestSupport {
  protected case class DummyEntity(tag: String, from: Option[DateTime], to: Option[DateTime])

  protected val entities: Seq[DummyEntity] =
    DummyEntity("first", Some(DateTime.now()), Some(DateTime.now().plusDays(1))) ::
    DummyEntity("second", Some(DateTime.now()), None) ::
    DummyEntity("third", None, Some(DateTime.now().plusDays(1))) ::
    DummyEntity("fourth", None, None) ::
    Nil

  protected val columnNames = IndexedSeq("tag", "from", "to")

  protected def entityToRow(entity: DummyEntity): Seq[Any] = Seq(entity.tag, entity.from.orNull, entity.to.orNull)

  protected def mkResultSet(): DummyResultSet = mkResultSet(entities.size)

  protected def mkResultSet(size: Int): DummyResultSet = {
    val rowDataSeq: Seq[RowData] = {
      for ((entity, index) <- entities.take(size).zipWithIndex)
        yield DummyRowData(index, entityToRow(entity), columnNames)
    }
    DummyResultSet(rowDataSeq, columnNames)
  }

  protected val parser = new RowDataParser[DummyEntity](row =>
    DummyEntity(
      row("tag").asInstanceOf[String],
      Option(row("from")).map(_.asInstanceOf[DateTime]),
      Option(row("to")).map(_.asInstanceOf[DateTime]))) {
    def expectedColumnsNames = Some(IndexedSeq("tag", "from", "to"))
  }
}

class SingleRowParserTest extends Specification with RowParsersTestSupport {
  "SingleRowParser" should {
    "parse a single entity from result set that consists of a single row" in {
      parser.!.parse(mkResultSet(1)) must_=== entities.head
    }

    "trigger an IllegalStateException when result set is empty" in {
      parser.!.parse(mkResultSet(0)) must throwAn[IllegalStateException]
    }

    "trigger an IllegalStateException when result set contains more that 1 row" in {
      parser.!.parse(mkResultSet()) must throwAn[IllegalStateException]
    }
  }
}

class MaybeSingleRowParserTest extends Specification with RowParsersTestSupport {
  "MaybeSingleRowParserTest" should {
    "parse a single entity from result set that consists of a single row" in {
      parser.?.parse(mkResultSet(1)) must beSome(entities.head)
    }

    "parse a None value from result set that is empty" in {
      parser.?.parse(mkResultSet(0)) must beNone
    }

    "trigger an IllegalStateException when result set contains more than 1 row" in {
      parser.?.parse(mkResultSet()) must throwA[IllegalStateException]
    }
  }
}

class RowSetParserTest extends Specification with RowParsersTestSupport {
  "RowSetParserTest" should {
    "parse a Seq of a single entity that from result set that consists of a single row" in {
      parser.*.parse(mkResultSet(1)) must_== entities.take(1)
    }

    "parse an empty Seq from result set that is empty" in {
      parser.*.parse(mkResultSet(0)) must_== Seq.empty
    }

    "parse a Seq of entities from result set that consists of many rows" in {
      parser.*.parse(mkResultSet()) must_== entities
    }
  }
}
