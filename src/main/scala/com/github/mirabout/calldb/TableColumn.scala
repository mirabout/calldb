package com.github.mirabout.calldb

import java.security.SecureRandom

import com.github.mauricio.async.db.RowData
import com.github.mauricio.async.db.postgresql.column.PostgreSQLColumnEncoderRegistry

/**
  * Describes an item that is a named parameter in a procedure call
  */
trait ParamNameProvider {
  def name: String
}

trait ParamsEncoder[A] extends ParamNameProvider {
  /**
    * @todo Deprecate in favor of [[encodeParam(A, StringAppender)]]
    */
  def encodeParam(value: A): String = {
    val output = new StringAppender()
    encodeParam(value, output)
    output.result()
  }

  /**
    * Writes a value as a part of a procedure call with the value
    * as a named parameter to a [[StringAppender]] that builds a call SQL
    */
  def encodeParam(value: A, output: StringAppender): Unit
}

/**
  * Describes an item that is a procedure call parameter and has [[TypeTraits]]
  */
trait ParamsTypeTraitsProvider {
  def typeTraits: TypeTraits
}

/**
  * Mark [[TableColumn]] fields in a [[GenericEntityTable]] that are corresponding to primary keys using this annotation.
  * Note that multiple key columns in a single [[GenericEntityTable]] are supported.
  */
final class keyColumn extends scala.annotation.StaticAnnotation

/**
  * Mark [[TableColumn]] fields in a [[GenericEntityTable]] that should be ignored by [[TableColumnsReflector]]
  * and consequently by [[TableColumnsChecker]], [[BoilerplateSqlGenerator]], etc using this annotation.
  */
final class ignoredColumn extends scala.annotation.StaticAnnotation

/**
  * A helper that appends a safe database string representation of a value to a [[StringAppender]]
  */
private[calldb] object PostgreSQLScalarValueEncoder {

  private[this] val encoderRegistry = PostgreSQLColumnEncoderRegistry.Instance

  /**
    * Appends a safe database string representation of a value to a [[StringAppender]]
    */
  def encodeValue(value: Any, output: StringAppender): Unit = value match {
    case cs: CharSequence =>
      encodeCharSequence(cs, output)
    case null =>
      output += "NULL"
    case other =>
      output += encoderRegistry.encode(other)
  }

  /**
    * There is a way to avoid SQL injection while passing string literals without using prepared statement placeholders.
    * A string literal might be bounded by so-called dollar-quotes that consist of pair of '$' characters
    * with an optional tag identifier between these symbols.
    * If a tag is used, tags must match for opening and closing dollar quotes.
    * By using a separated tag based on a secure random number we mitigate an opportunity for an SQL injection.
    */
  private def encodeCharSequence(value: CharSequence, output: StringAppender) = {
    val randomTag = randomDollarQuoteTag()
    output += '$' += randomTag += '$'
    output += value
    output += '$' += randomTag += '$'
  }

  private[this] val secureRandom = new SecureRandom()

  /**
    * @todo [[SecureRandom]] calls might block, precache a value
    *      for several parts of a second. Use an Akka scheduler for tag updates.
    */
  private def randomDollarQuoteTag(): String = {
    // Avoid two expensive calls to nextLong()
    val randomLong = secureRandom.nextLong()
    val (hiPart, loPart) = ((randomLong >>> 32).toInt, (randomLong & Int.MaxValue).toInt)
    // A tag must be a valid identifier, so start it with '_'
    f"_$hiPart%08x$loPart%08x"
  }
}

/**
  * A definition of table column corresponding to a database table column.
  * Instances of this class should be used as members of [[GenericEntityTable]] sublclasses
  * @param name a column unqualified name corresponding to a the database column name
  * @param fromEntity_ an extractor of an entity field value corresponding to the column
  * @param tableName_ a supplier of table name of enclosing [[GenericEntityTable]].
  *                   We use call-by-name table name supplier to avoid initialization order issues
  * @tparam E An entity type of an enclosing [[GenericEntityTable]] table definition
  * @tparam C A Scala column type corresponding to the entity field
  * @todo Fix and specify type parameters variance
  */
final class TableColumn[E, C](
    val name                               : String,
    private[this]  val fromEntity_         : E => C,
    private[this]  val tableName_          : => Option[String])
    (private[this] implicit val reader     : ColumnReader[C],
    private[this]  implicit val writer     : ColumnWriter[C],
    private[this]  val columnTypeProvider  : ColumnTypeProvider[C])
  extends TypedCallable.ParamsDef[C] {

  /**
    * This field is set when checking database metadata
    * @see [[TableColumnsChecker]], [[GenericEntityTable]]
    */
  private[calldb] var columnIndex: Int = -1

  override def toString: String = s"TableColumn($qualifiedName,$typeTraits)"

  /**
    * @inheritdoc
    */
  override val typeTraits: BasicTypeTraits = columnTypeProvider.typeTraits

  /**
    * Extracts a column value from an entity and returns it as [[Any]]
    */
  def apply(e: E): Any = writer.write(fromEntity_(e))

  /**
    * @todo describe why it is optional (better totally remove using table columns this way in favor of [[Param]])
    */
  lazy val tableName: Option[String] = tableName_
  /**
    * Returns a fully qualified name of the column containing the table name and the column name separated by a dot
    */
  lazy val qualifiedName: String = tableName.map(_ + "." + name).getOrElse(name).toLowerCase
  /**
    * Returns a label that the column is expected to have in a [[RowData]]
    * that contains of the table name and the column name separated by double underscore
    */
  lazy val columnLabel: String = tableName.map(_ + "__" + name).getOrElse(name).toLowerCase

  /**
    * Reads a column value from a [[RowData]] row.
    * Relies on implicitly provided [[ColumnReader]] via the class constructor
    */
  def fromRow(rowData: RowData): C = reader.read(rowData(columnLabel))

  @deprecated("use fromRow() instead", "0.0.10")
  def ofRow(rowData: RowData): C = fromRow(rowData)

  /**
    * @inheritdoc
    */
  override def encodeParam(value: C, output: StringAppender): Unit = {
    output += name += ":="
    PostgreSQLScalarValueEncoder.encodeValue(value, output)
  }

  /**
    * Writes a string representation of a value that conforms to the PostgreSQL ROW() format to a [[StringAppender]]
    */
  private[calldb] def encodeRowValue(value: Any, output: StringAppender): Unit =
    PostgreSQLScalarValueEncoder.encodeValue(value, output)
}

object TableColumn {
  /**
    * Provides an [[Ordering]] of columns by an injected column index (acquired from a database)
    */
  val orderingByIndex: Ordering[TableColumn[_, _]] = new Ordering[TableColumn[_, _]] {
    def compare(x: TableColumn[_, _], y: TableColumn[_, _]): Int = {
      if (x.columnIndex < 0) {
        throw new AssertionError(s"A database index of $x has not been injected")
      }
      if (y.columnIndex < 0) {
        throw new AssertionError(s"A database index of $y has not been injected")
      }
      Integer.compare(x.columnIndex, y.columnIndex)
    }
  }

  def apply[E, C](name: String, fromEntity: E => C, tableName: => String)
    (implicit r: ColumnReader[C], w: ColumnWriter[C], tp: ColumnTypeProvider[C]): TableColumn[E, C] =
      new TableColumn(name, fromEntity, Some(tableName))

  def apply[E, C](name: Symbol, fromEntity: E => C, tableName: => String)
    (implicit r: ColumnReader[C], w: ColumnWriter[C], tp: ColumnTypeProvider[C]): TableColumn[E, C] =
      new TableColumn(name.name, fromEntity, Some(tableName))

  def apply[C](name: String, value: C)
    (implicit r: ColumnReader[C], w: ColumnWriter[C], tp: ColumnTypeProvider[C]): TableColumn[Unit, C] =
      new TableColumn(name, (_: Unit) => value, None)

  def apply[C](name: Symbol, value: C)
    (implicit r: ColumnReader[C], w: ColumnWriter[C], tp: ColumnTypeProvider[C]): TableColumn[Unit, C] =
      new TableColumn(name.name, (_: Unit) => value, None)
}
