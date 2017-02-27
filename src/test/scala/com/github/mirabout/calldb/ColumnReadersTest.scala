package com.github.mirabout.calldb

import java.util.UUID

import org.joda.time.{Period, Duration, DateTime, LocalDateTime}

import org.specs2.mutable._
import org.specs2.specification.Scope

class ColumnReadersTest extends Specification {
  private def asAny(any: Any) = any

  class WithReaders extends Scope with ColumnReaders

  "ColumnReaders" should {

    "provide an implicit Boolean reader" in new WithReaders {
      implicitly[ColumnReader[Boolean]].read(asAny(false)) must_=== false
    }

    "provide an implicit Byte reader" in new WithReaders {
      implicitly[ColumnReader[Byte]].read(asAny(1.toByte)) must_=== 1.toByte
    }

    "provide an implicit Short reader" in new WithReaders {
      implicitly[ColumnReader[Short]].read(asAny(1.toShort)) must_=== 1.toShort
    }

    "provide an implicit Int reader" in new WithReaders {
      implicitly[ColumnReader[Int]].read(asAny(1)) must_=== 1
    }

    "provide an implicit Long reader" in new WithReaders {
      implicitly[ColumnReader[Long]].read(asAny(1L)) must_=== 1L
    }

    "provide an implicit Float reader" in new WithReaders {
      implicitly[ColumnReader[Float]].read(asAny(1.0f)) must_=== 1.0f
    }

    "provide an implicit Double reader" in new WithReaders {
      implicitly[ColumnReader[Double]].read(asAny(1.0)) must_=== 1.0
    }

    "provide an implicit String reader" in new WithReaders {
      implicitly[ColumnReader[String]].read(asAny("Hello")) must_=== "Hello"
    }

    "provide an implicit Array[Byte] reader" in new WithReaders {
      val theArray = Array(1.toByte, 2.toByte, 3.toByte)
      // We have to add a workaround for default arrays equality that compares instance references
      implicitly[ColumnReader[Array[Byte]]].read(asAny(theArray)).toSeq must_=== theArray.toSeq
    }

    "provide an implicit UUID reader" in new WithReaders {
      val theUuid = UUID.randomUUID()
      implicitly[ColumnReader[UUID]].read(asAny(theUuid)) must_=== theUuid
    }

    "provide an implicit DateTime reader" in new WithReaders {
      val theDate = DateTime.now()
      implicitly[ColumnReader[DateTime]].read(asAny(theDate)) must_=== theDate
    }

    "provide an implicit LocalDateTime reader" in new WithReaders {
      val theDate = LocalDateTime.now()
      implicitly[ColumnReader[LocalDateTime]].read(asAny(theDate)) must_=== theDate
    }

    "provide an implicit Duration reader" in new WithReaders {
      val duration = Duration.millis(1000)
      implicitly[ColumnReader[Duration]].read(asAny(duration.getMillis)) must_=== duration
    }

    "provide an implicit Period reader" in new WithReaders {
      val period = Period.days(1).withHours(12)
      implicitly[ColumnReader[Period]].read(asAny(period.toString())) must_=== period
    }

    "provide an implicit hstore (Map[String, Option[String]) reader" in new WithReaders {
      // Hstore format is so fragile...
      val hstore = "\"foo\"=>\"bar\", \"baz\"=>NULL"
      val readMap = implicitly[ColumnReader[Map[String, Option[String]]]].read(hstore)
      readMap must beDefinedAt("foo", "baz")
      readMap("foo") must beSome("bar")
      readMap("baz") must beNone
    }

    "provide an implicit Option reader" in new WithReaders {
      implicitly[ColumnReader[Option[Int]]].read(asAny(1)) must beSome(1)
      implicitly[ColumnReader[Option[Int]]].read(null) must beNone
    }

    "provide an implicit Traversable reader" in new WithReaders {
      // The database library returns a Traversable as an IndexedSeq of boxed values
      // TODO: Test for zero-copy cast?
      implicitly[ColumnReader[Traversable[Int]]].read(asAny(IndexedSeq(1, 2, 3))).toSet must_=== Set(1, 2, 3)
    }

    "provide an implicit IndexedSeq reader" in new WithReaders {
      // TODO: Test for zero-copy cast?
      implicitly[ColumnReader[IndexedSeq[Int]]].read(asAny(IndexedSeq(1, 2, 3))) must_=== IndexedSeq(1, 2, 3)
    }
  }
}
