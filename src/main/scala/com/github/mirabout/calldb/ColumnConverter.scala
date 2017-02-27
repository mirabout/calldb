package com.github.mirabout.calldb

import java.util.UUID
import org.joda.time.DateTime
import org.joda.time.LocalDateTime
import org.postgresql.util.HStoreConverter
import scala.reflect.ClassTag

/**
  * Converts a raw value of user-defined type to a value of type that is known for string encoders
  * and might be encoded into a recognizable for PostgreSQL string.
  */
trait ColumnWriter[-A] {
  def write[B <: A](value: B): Any

  /**
   * Indicates that a value may be written directly without any conversions (aside from pointer cast)
   * @note The default implementation that is provided for convenience is conservative.
   *       Override it for performance improvement if direct conversion may be applied
   */
  def mayWriteDirectly[B <: A](value: B) = false
}

/**
  * Mixin this trait into another class to use provided implicit column writers
  */
trait ColumnWriters {
  final class NoOpWriter[-A] extends ColumnWriter[A] {
    def write[B <: A](value: B): Any = value
    override def mayWriteDirectly[B <: A](value: B) = true
  }

  implicit final val booleanWriter = new NoOpWriter[Boolean]
  implicit final val byteWriter = new NoOpWriter[Byte]
  implicit final val shortWriter = new NoOpWriter[Short]
  implicit final val charWriter = new NoOpWriter[Char]
  implicit final val intWriter = new NoOpWriter[Int]
  implicit final val longWriter = new NoOpWriter[Long]
  implicit final val floatWriter = new NoOpWriter[Float]
  implicit final val doubleWriter = new NoOpWriter[Double]

  implicit final val stringWriter = new NoOpWriter[String]
  implicit final val bytesWriter = new NoOpWriter[Array[Byte]]
  implicit final val uuidWriter = new NoOpWriter[UUID]

  implicit final val dateTimeWriter = new NoOpWriter[DateTime]
  implicit final val localDateTimeWriter = new NoOpWriter[LocalDateTime]

  implicit final def optionWriter[A](implicit writesJust: ColumnWriter[A]) = new ColumnWriter[Option[A]] {
    def write[B <: Option[A]](value: B): Any = value match {
      case Some(justValue) => writesJust.write(justValue)
      case None => null
    }
  }

  implicit final def traversableWriter[A](implicit writesElem: ColumnWriter[A]) = new ColumnWriter[Traversable[A]] {
    def write[B <: Traversable[A]](value: B): Any = {
      if (value forall writesElem.mayWriteDirectly) value else {
        value map writesElem.write
      }
    }
  }

  implicit final val periodWriter = new ColumnWriter[org.joda.time.Period] {
    def write[B <: org.joda.time.Period](value: B): Any = {
      /**
       * @note Period.getMillis returns only millis part of period
       *       (it may be absent while the period is non-zero)
       */
      value.toStandardDuration.getMillis
    }
  }

  implicit final val durationWriter = new ColumnWriter[org.joda.time.Duration] {
    def write[B <: org.joda.time.Duration](value: B): Any = value.getMillis
  }

  implicit final val hStoreWriter = new ColumnWriter[Map[String, Option[String]]] {
    type JStringHashMap = java.util.HashMap[String, String]
    def write[B <: Map[String, Option[String]]](value: B): Any = {
      val javaMap = new java.util.HashMap[String, String](value.size)
      val iterator = value.iterator
      while (iterator.hasNext) {
        val pair = iterator.next()
        javaMap.put(pair._1, pair._2.orNull)
      }
      HStoreConverter.toString(javaMap)
    }
  }
}

/**
  * Converts a raw value of [[com.github.mauricio.async.db.ResultSet]] row element
  * returned by the database connection library to an expected user-defined type for the row element.
  * @tparam A An expected row element type
  */
trait ColumnReader[+A] extends BugReporting {
  def read(rawValue: Any): A

  // We choose distinct names for ColumnReader and ColumnWriter methods
  // (Being mixed in together, especially under erasure, they may clash otherwise)
  def mayReadDirectly(rawValue: Any): Boolean = false

  @inline protected final def nonNull(rawValue: Any): Any = {
    if (rawValue == null) {
      BUG("Expected non-null value, got null one")
    }
    rawValue
  }

  @inline protected final def asString(rawValue: Any): String = rawValue match {
    case s: String => s
    case null => BUG(s"Expected non-null string value, got null value")
    case _ => BUG(s"Expected string value, got $rawValue of class ${rawValue.getClass}")
  }
}

/**
  * Mixin this trait into another class to use provided implicit readers
  */
trait ColumnReaders extends BugReporting {
  final class NoOpReader[+A](implicit classTag: ClassTag[A]) extends ColumnReader[A] {
    def read(rawValue: Any): A = nonNull(rawValue) match {
      case convertedValue: A => convertedValue
      case _ => BUG(s"Expected value of type $classTag, got $rawValue of class ${rawValue.getClass}")
    }
    override def mayReadDirectly(rawValue: Any): Boolean = { nonNull(rawValue); true }
  }

  implicit final val booleanReader = new NoOpReader[Boolean]
  implicit final val byteReader = new NoOpReader[Byte]
  implicit final val shortReader = new NoOpReader[Short]
  implicit final val charReader = new NoOpReader[Char]
  implicit final val intReader = new NoOpReader[Int]
  implicit final val longReader = new NoOpReader[Long]
  implicit final val floatReader = new NoOpReader[Float]
  implicit final val doubleReader = new NoOpReader[Double]

  implicit final val stringReader = new NoOpReader[String]
  implicit final val bytesReader = new NoOpReader[Array[Byte]]

  implicit final val uuidReader = new ColumnReader[UUID] {
    final def read(rawValue: Any): UUID = nonNull(rawValue) match {
      case s: String => try {
        UUID.fromString(s)
      } catch {
        case ex: Throwable => BUG(s"Can't convert string value `$s` to UUID", ex)
      }
      case uuid: UUID => uuid
      case weirdValue => BUG(s"Expected uuid-like value (String or UUID), got $weirdValue")
    }
  }

  implicit final val dateTimeReader = new NoOpReader[DateTime]
  implicit final val localDateTimeReader = new NoOpReader[LocalDateTime]

  implicit final def optionReader[A](implicit justReader: ColumnReader[A]) = new ColumnReader[Option[A]] {
    final def read(rawValue: Any): Option[A] =
      if (rawValue == null) None else Some(justReader.read(rawValue))

    override final def mayReadDirectly(rawValue: Any): Boolean =
      if (rawValue == null) true else justReader.mayReadDirectly(rawValue)
  }

  final def setReader[A](implicit elemReader: ColumnReader[A]) = new ColumnReader[Set[A]] {
    final def read(rawValue: Any): Set[A] = {
      nonNull(rawValue) match {
        case indexedSeq: IndexedSeq[_] => readSet(indexedSeq)
        case weirdValue => BUG(s"Expected IndexedSeq value, got value of class ${weirdValue.getClass}")
      }
    }

    // Atm we use database arrays to store scala sets
    private def readSet(indexedSeq: IndexedSeq[_]): Set[A] = {
      val builder = Set.newBuilder[A]
      var i = -1
      while ({i += 1; i < indexedSeq.length}) {
        builder += elemReader.read(indexedSeq(i))
      }
      builder.result()
    }
  }

  final class IndexedSeqReader[A](elemReader: ColumnReader[A]) extends ColumnReader[IndexedSeq[A]] {
    def read(rawValue: Any): IndexedSeq[A] = {
      nonNull(rawValue) match {
        case indexedSeq: IndexedSeq[_] =>
          readIndexedSeq(indexedSeq)
        // Don't bother about other types, looks like the library always returns array as IndexedSeq
        case weirdValue =>
          BUG(s"Expected IndexedSeq[_], got $weirdValue")
      }
    }

    protected def readIndexedSeq(indexedSeq: IndexedSeq[_]): IndexedSeq[A] = {
      var i = 0
      var mayCast = true
      while (i < indexedSeq.size && mayCast) {
        mayCast &&= elemReader.mayReadDirectly(indexedSeq(i))
        i += 1
      }
      if (mayCast) {
        indexedSeq.asInstanceOf[IndexedSeq[A]]
      } else {
        val buffer = new scala.collection.mutable.ArrayBuffer[A](indexedSeq.size)
        var i = 0
        while (i < indexedSeq.size) {
          buffer += elemReader.read(indexedSeq(i))
          i += 1
        }
        buffer
      }
    }
  }

  implicit final def indexedSeqReader[A](implicit elemReader: ColumnReader[A]): ColumnReader[IndexedSeq[A]] =
    new IndexedSeqReader(elemReader)

  implicit final def traversableReader[A](implicit elemReader: ColumnReader[A]): ColumnReader[Traversable[A]] =
    new IndexedSeqReader(elemReader)

  implicit final val periodReader = new ColumnReader[org.joda.time.Period] {
    final def read(rawValue: Any): org.joda.time.Period = {
      org.joda.time.Period.parse(asString(rawValue))
    }
  }

  implicit final val durationReader = new ColumnReader[org.joda.time.Duration] {
    final def read(rawValue: Any): org.joda.time.Duration = {
      rawValue match {
        case longValue: Long => new org.joda.time.Duration(longValue)
        case intValue: Int => new org.joda.time.Duration(intValue.toLong)
        case null => BUG(s"Can't convert null to org.joda.time.Duration")
        case weirdValue => BUG(s"Can't convert `$weirdValue` of class ${weirdValue.getClass} to org.joda.time.Duration")
      }
    }
  }

  implicit final val hStoreReader = new ColumnReader[Map[String, Option[String]]] {
    def read(rawValue: Any): Map[String, Option[String]] = {
      val stringValue: String = asString(rawValue)
      val javaMap = HStoreConverter.fromString(stringValue).asInstanceOf[java.util.HashMap[String, String]]
      val mapBuilder = Map.newBuilder[String, Option[String]]
      mapBuilder.sizeHint(javaMap.size)
      val iterator = javaMap.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val key: String = entry.getKey
        val nullableValue: String = entry.getValue
        mapBuilder += Tuple2(key, Option(nullableValue))
      }
      mapBuilder.result()
    }
  }
}

/**
  * This class is a helper column converter provided for convenience for types that have a bijection with [[Int]].
  * @param reads A function that converts from Int to [[A]]
  * @param writes A function that converts from [[A]] to Int
  * @tparam A An expected row element type
  */
final class GenericEnumLikeConverter[A: ClassTag](private[this] val reads: Int => A, private[this] val writes: A => Int)
  extends ColumnReader[A] with ColumnWriter[A] with BugReporting {

  private def tag = implicitly[ClassTag[A]].runtimeClass.getSimpleName

  def read(rawValue: Any): A = rawValue match {
    case intValue: Int =>
      reads.apply(intValue)
    case null =>
      BUG(s"Expected non-null integer value that may be converted to $tag, got null")
    case weirdValue =>
      BUG(s"Expected integer value that may be converted to $tag, got `$weirdValue` of class ${weirdValue.getClass}")
  }

  def write[B <: A](value: B): Any = writes.apply(value)
}
