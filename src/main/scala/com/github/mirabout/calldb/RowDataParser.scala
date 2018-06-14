package com.github.mirabout.calldb

import com.github.mauricio.async.db.{ResultSet, RowData}

import java.util.UUID

import scala.reflect.ClassTag

/**
  * Constructs a [[Row]] type from result set row of type [[com.github.mauricio.async.db.RowData]]
  * @param parseFunc An actual construction function
  * @tparam Row An expected type instances of which a result set row yields, usually compound.
  */
abstract class RowDataParser[Row](private[this] val parseFunc: RowData => Row) extends (RowData => Row) {

  /**
   * Extracts an entity of type [[Row]] from a row
   *
   * @note May be overridden for performance reasons, so left open for it
   * @param row Query result row
   * @return extracted [[Row]] entity
   */
  def fromRow(row: RowData): Row = parseFunc.apply(row)

  @deprecated("use fromRow() instead", "0.0.19")
  def ofRow(row: RowData): Row = fromRow(row)

  override def apply(row: RowData) = parseFunc.apply(row)

  /**
   * Expects a single row in a query result
   */
  final def single : ResultSetParser[Row] = new SingleRowParser[Row](this)

  /**
   * Expects an empty query result, or a single row in it
   */
  final def option : ResultSetParser[Option[Row]] = new MaybeSingleRowParser[Row](this)

  /**
   * Expects any count of rows in a query result, empty or some rows
    *
    * @return A [[Seq]] of [[Row]] that may be empty
   */
  final def seq : ResultSetParser[IndexedSeq[Row]] = new RowSeqParser[Row](this)

  @deprecated("use .single instead", "0.0.17")
  final def ! : ResultSetParser[Row] = single

  @deprecated("use .option instead", "0.0.17")
  final def ? : ResultSetParser[Option[Row]] = option

  @deprecated("use .seq instead", "0.0.17")
  final def * : ResultSetParser[IndexedSeq[Row]] = seq

  final def zip[B](that: RowDataParser[B]): RowDataParser[(Row, B)] = RowDataParser.zip(this, that)

  final def map[B](mapper: Row => B): RowDataParser[B] = RowDataParser.map(this, mapper)

  def expectedColumnsNames: Option[IndexedSeq[String]]
}

object RowDataParser extends BugReporting {

  abstract class SingleColumnNamedParser[A](name: String)(parseFunc: RowData => A) extends RowDataParser[A](parseFunc) {
    val expectedColumnsNames: Option[IndexedSeq[String]] = Some(IndexedSeq(name))
  }

  abstract class SingleColumnIndexedParser[A](index: Int)(parseFunc: RowData => A) extends RowDataParser[A](parseFunc) {
    val expectedColumnsNames: Option[IndexedSeq[String]] = None
  }

  final class SingleColumnCastNamedParser[A: ClassTag](name: String)
    extends SingleColumnNamedParser[A](name)(row => row(name).asInstanceOf[A])

  final class SingleColumnCastIndexedParser[A: ClassTag](index: Int)
    extends SingleColumnIndexedParser[A](index)(row => row(index).asInstanceOf[A])

  // Tbh its very error prone since parameter type may be omitted and result parser will be of Nothing type
  private def indexed[A: ClassTag](index: Int) = new SingleColumnCastIndexedParser[A](index)
  private def named[A : ClassTag](name: String) = new SingleColumnCastNamedParser[A](name.toLowerCase)

  @deprecated("use boolean() instead", "0.0.17")
  def bool(name: String) = named[Boolean](name)
  @deprecated("use boolean() instead", "0.0.17")
  def bool(index: Int) = indexed[Boolean](index)

  def boolean(name: String): RowDataParser[Boolean] = named[Boolean](name)
  def boolean(index: Int): RowDataParser[Boolean] = indexed[Boolean](index)

  def int(name: String): RowDataParser[Int] = named[Int](name)
  def int(index: Int): RowDataParser[Int] = indexed[Int](index)

  def long(name: String): RowDataParser[Long] = named[Long](name)
  def long(index: Int): RowDataParser[Long] = indexed[Long](index)

  def double(name: String): RowDataParser[Double] = named[Double](name)
  def double(index: Int): RowDataParser[Double] = indexed[Double](index)

  def string(name: String): RowDataParser[String] = named[String](name)
  def string(index: Int): RowDataParser[String] = indexed[String](index)

  def stringKVMap(name: String): RowDataParser[Map[String, Option[String]]] =
    new SingleColumnNamedParser[Map[String, Option[String]]](name)(row => parseHStore(row(name.toLowerCase))) {}
  def stringKVMap(index: Int): RowDataParser[Map[String, Option[String]]] =
    new SingleColumnIndexedParser[Map[String, Option[String]]](index)(row => parseHStore(row(index))) {}

  private object parseHStore extends ColumnReaders {
    def apply(o: Any): Map[String, Option[String]] = implicitly[ColumnReader[Map[String, Option[String]]]].read(o)
  }

  private[this] def parseUuid(o: Any) = o match {
    case s: java.lang.String => UUID.fromString(s)
    case u: java.util.UUID => u
    case _ => BUG(s"Can't parse $o as UUID")
  }

  def uuid(name: String): RowDataParser[UUID] =
    new SingleColumnNamedParser[UUID](name)(row => parseUuid(row.apply(name.toLowerCase))) {}

  def uuid(index: Int): RowDataParser[UUID] =
    new SingleColumnIndexedParser[UUID](index)(row => parseUuid(row.apply(index))) {}

  private class ZippedParser[A, B](parserA: RowDataParser[A], parserB: RowDataParser[B])
    extends RowDataParser[(A, B)](row => (parserA.fromRow(row), parserB.fromRow(row))) {

    private def failOnAbsentNames(token: RowDataParser[_]) = BUG(
      s"Can't merge column names for a zipped parser: " +
      s"names for parser $token are absent, names for other one are present")

    private def mergeNames(namesA: IndexedSeq[String], namesB: IndexedSeq[String]) = {
      val mergedNames = (namesA ++ namesB).map(_.toLowerCase)
      if (mergedNames.toSet.size != mergedNames.size) {
        BUG(s"Can't merge column names for $parserA ($namesA) and $parserB ($namesB): duplicate names exist")
      }
      Some(mergedNames)
    }

    private def getExpectedColumnNames = (parserA.expectedColumnsNames, parserB.expectedColumnsNames) match {
      case (Some(namesA), Some(namesB)) =>
        mergeNames(namesA, namesB)
      case (Some(_), None) =>
        failOnAbsentNames(parserB)
      case (None, Some(_)) =>
        failOnAbsentNames(parserA)
      case (None, None) =>
        None
    }

    override val expectedColumnsNames: Option[IndexedSeq[String]] = getExpectedColumnNames
  }

  def zip[A, B](parserA: RowDataParser[A], parserB: RowDataParser[B]): RowDataParser[(A, B)] =
    new ZippedParser(parserA, parserB)

  def map[A, B](parser: RowDataParser[A], mapper: A => B): RowDataParser[B] = {
    new RowDataParser[B](row => mapper.apply(parser.fromRow(row))) {
      val expectedColumnsNames: Option[IndexedSeq[String]] = parser.expectedColumnsNames
    }
  }

  /**
    * Might be extended for custom types
    */
  trait returns {
    def boolean(name: String): RowDataParser[Boolean] = RowDataParser.boolean(name)
    def boolean(index: Int): RowDataParser[Boolean] = RowDataParser.boolean(index)

    def int(name: String): RowDataParser[Int] = RowDataParser.int(name)
    def int(index: Int): RowDataParser[Int] = RowDataParser.int(index)

    def long(name: String): RowDataParser[Long] = RowDataParser.long(name)
    def long(index: Int): RowDataParser[Long] = RowDataParser.long(index)

    def double(name: String): RowDataParser[Double] = RowDataParser.double(name)
    def double(index: Int): RowDataParser[Double] = RowDataParser.double(index)

    def string(name: String): RowDataParser[String] = RowDataParser.string(name)
    def string(index: Int): RowDataParser[String] = RowDataParser.string(index)

    def stringKVMap(name: String): RowDataParser[Map[String, Option[String]]] =
      RowDataParser.stringKVMap(name)
    def stringKVMap(index: Int): RowDataParser[Map[String, Option[String]]] =
      RowDataParser.stringKVMap(index)

    def uuid(name: String): RowDataParser[UUID] = RowDataParser.uuid(name)
    def uuid(index: Int): RowDataParser[UUID] = RowDataParser.uuid(index)

    def apply[R](parser: RowDataParser[R]): RowDataParser[R] = parser
  }

  /**
    * A default instance of the corresponding trait that allows
    * a fluent specification of database function return types
    */
  object returns extends returns
}

/**
  * Yields a [[Result]] from [[com.github.mauricio.async.db.ResultSet]]
  * @tparam Result A single element, an optional element or a sequence of elements produced from each result set row
  */
trait ResultSetParser[Result] {
  def mapWith[NewResult](newMapper: Result => NewResult): ResultSetParser[NewResult]

  def parse(resultSet: ResultSet): Result

  def expectedColumnsNames: Option[IndexedSeq[String]]
}

class ProxyResultSetParser[ParsedResult, MappedResult](
    parser: ResultSetParser[ParsedResult],
    mapper: ParsedResult => MappedResult)
  extends ResultSetParser[MappedResult] {

  def parse(resultSet: ResultSet): MappedResult = mapper(parser.parse(resultSet))

  def mapWith[NewMappedResult](newMapper: MappedResult => NewMappedResult) =
    new ProxyResultSetParser[ParsedResult, NewMappedResult](parser, newMapper compose mapper) {}

  def expectedColumnsNames: Option[IndexedSeq[String]] = parser.expectedColumnsNames
}

// This class is introduced to break circular dependency between RowDataParser and its !, ?, * ResultSetParsers
private case class ResultSetParseOps[Row](parseFunc: RowData => Row) extends BugReporting {
  private def bugUnexpectedSize(expectedSize: String, resultSet: ResultSet): Nothing = {
    BUG(
      s"Expected $expectedSize as a result set size, got ${resultSet.size} " +
      s"while parsing result set of columns ${resultSet.columnNames}")
  }

  def parseSingle(resultSet: ResultSet): Row = {
    if (resultSet.size != 1) {
      bugUnexpectedSize("1", resultSet)
    } else {
      parseFunc.apply(resultSet.head)
    }
  }

  def parseSingleOpt(resultSet: ResultSet): Option[Row] = {
    if (resultSet.size > 1) {
      bugUnexpectedSize("0 or 1", resultSet)
    } else {
      resultSet.headOption map parseFunc.apply
    }
  }
}

trait ParserSizeBugSupport extends BugReporting {
  def BUGunexpectedSize(resultSet: ResultSet, expectedSize: String): Nothing =
    BUG(s"Expected result set of size $expectedSize, got a result set of size ${resultSet.size}")
}

final class SingleRowParser[Row](rowDataParser: RowDataParser[Row])
  extends ResultSetParser[Row] with ParserSizeBugSupport {

  def parse(resultSet: ResultSet): Row = {
    if (resultSet.size == 1) {
      rowDataParser.apply(resultSet.head)
    } else {
      BUGunexpectedSize(resultSet, "1")
    }
  }

  def expectedColumnsNames: Option[IndexedSeq[String]] = rowDataParser.expectedColumnsNames

  def mapWith[NewResult](mapper: Row => NewResult): ResultSetParser[NewResult] =
    new ProxyResultSetParser(this, mapper)
}

final class MaybeSingleRowParser[Row](rowDataParser: RowDataParser[Row])
  extends ResultSetParser[Option[Row]] with ParserSizeBugSupport {

  def parse(resultSet: ResultSet): Option[Row] = {
    if (resultSet.size <= 1) {
      resultSet.headOption map rowDataParser.apply
    } else {
      BUGunexpectedSize(resultSet, "0 or 1")
    }
  }

  def expectedColumnsNames: Option[IndexedSeq[String]] = rowDataParser.expectedColumnsNames

  def mapWith[NewResult](mapper: Option[Row] => NewResult): ResultSetParser[NewResult] =
    new ProxyResultSetParser(this, mapper)
}

final class RowSeqParser[Row](rowDataParser: RowDataParser[Row]) extends ResultSetParser[IndexedSeq[Row]] {
  def parse(resultSet: ResultSet): IndexedSeq[Row] = {
    if (resultSet.isEmpty) IndexedSeq.empty else {
      val result = new scala.collection.mutable.ArrayBuffer[Row](resultSet.size)
      var i = -1
      while ({i += 1; i < resultSet.length}) {
        result += rowDataParser.apply(resultSet(i))
      }
      result
    }
  }

  def expectedColumnsNames: Option[IndexedSeq[String]] = rowDataParser.expectedColumnsNames

  def mapWith[NewResult](mapper: IndexedSeq[Row] => NewResult): ResultSetParser[NewResult] =
    new ProxyResultSetParser(this, mapper)
}
