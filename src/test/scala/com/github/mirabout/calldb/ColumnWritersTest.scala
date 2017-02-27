package com.github.mirabout.calldb

import java.util.UUID

import org.joda.time.{DateTime, LocalDateTime, Duration, Period}

import org.specs2.mutable._
import org.specs2.specification.Scope

class ColumnWritersTest extends Specification {

  class WithWriters extends Scope with ColumnWriters

  "ColumnWriters" should {
    "provide an implicit Boolean writer" in new WithWriters {
      implicitly[ColumnWriter[Boolean]].write(true) must_== true
      implicitly[ColumnWriter[Boolean]].write(false) must_== false
    }

    "provide an implicit Byte writer" in new WithWriters {
      implicitly[ColumnWriter[Byte]].write((-1).toByte) must_== (-1).toByte
      implicitly[ColumnWriter[Byte]].write((+1).toByte) must_== (+1).toByte
    }

    "provide an implicit Short writer" in new WithWriters {
      implicitly[ColumnWriter[Short]].write((-1).toShort) must_== (-1).toByte
      implicitly[ColumnWriter[Short]].write((+1).toShort) must_== (+1).toByte
    }

    "provide an implicit Int writer" in new WithWriters {
      implicitly[ColumnWriter[Int]].write(-1) must_== -1
      implicitly[ColumnWriter[Int]].write(+1) must_== +1
    }

    "provide an implicit Long writer" in new WithWriters {
      implicitly[ColumnWriter[Long]].write(-1L) must_== -1L
      implicitly[ColumnWriter[Long]].write(+1L) must_== +1L
    }

    "provide an implicit Float writer" in new WithWriters {
      implicitly[ColumnWriter[Float]].write(-1.0f) must_== -1.0f
      implicitly[ColumnWriter[Float]].write(+1.0f) must_== +1.0f
    }

    "provide an implicit Double writer" in new WithWriters {
      implicitly[ColumnWriter[Double]].write(-1.0) must_== -1.0
      implicitly[ColumnWriter[Double]].write(+1.0) must_== +1.0
    }

    "provide an implicit String writer" in new WithWriters {
      implicitly[ColumnWriter[String]].write("foo") must_== "foo"
      implicitly[ColumnWriter[String]].write("bar") must_== "bar"
    }

    "provide an implicit java.util.UUID writer" in new WithWriters {
      val (uuid1, uuid2) = (UUID.randomUUID(), UUID.randomUUID())
      implicitly[ColumnWriter[UUID]].write(uuid1) must_== uuid1
      implicitly[ColumnWriter[UUID]].write(uuid2) must_== uuid2
    }

    "provide an implicit org.joda.time.DateTime writer" in new WithWriters {
      val now: DateTime = DateTime.now()
      implicitly[ColumnWriter[DateTime]].write(now) must_== now
    }

    "provide an implicit org.joda.time.LocalDateTime writer" in new WithWriters {
      val now: LocalDateTime = LocalDateTime.now()
      implicitly[ColumnWriter[LocalDateTime]].write(now) must_== now
    }

    "provide an implicit org.joda.time.Duration writer" in new WithWriters {
      val duration: Duration = Duration.standardDays(1)
      implicitly[ColumnWriter[Duration]].write(duration) must_== duration.getMillis()
    }

    "provide an implicit org.joda.time.Period writer" in new WithWriters {
      val period: Period = Duration.standardHours(3).toPeriod()
      implicitly[ColumnWriter[Period]].write(period) must_== period.toStandardDuration().getMillis()
    }

    "provide an implicit hstore (Map[String, Option[String]) writer" in new WithWriters {
      type HStore = Map[String, Option[String]]
      val hstoreTuples: List[(String, Option[String])] = "foo" -> Some("bar") :: "baz" -> None :: Nil
      val hstoreMap: HStore = Map("foo" -> Some("bar"), "baz" -> None)
      val actualHstoreString: Any = implicitly[ColumnWriter[HStore]].write(hstoreMap)
      actualHstoreString must beAnInstanceOf[String]
      // Ordering of tuples is not defined. Check (unordered) pairs set
      val actualHstoreStringTokens: Seq[String] = actualHstoreString.asInstanceOf[String].split(',').map(_.trim())
      val expectedHStoreStringTokens: Seq[String] = hstoreTuples collect {
        case (key, Some(value)) => key + "=>" + value
        case (key, None) => key + "=>NULL"
      }
      actualHstoreStringTokens.toSet must_=== actualHstoreStringTokens.toSet
    }

    "provide an implicit Option writer" in new WithWriters {
      implicitly[ColumnWriter[Option[String]]].write(Some("")) must_== ""
      implicitly[ColumnWriter[Option[String]]].write(None) must_== null
    }
  }
}
