package com.github.mirabout.calldb

import java.security.SecureRandom

import com.github.mauricio.async.db.RowData
import com.github.mauricio.async.db.postgresql.column.PostgreSQLColumnEncoderRegistry

final case class NamedParamValue(name: String, value: Any)

trait ParamNameProvider {
  def name: String
}

trait ParamsEncoder[A] extends ParamNameProvider {
  def encodeParam(value: A): String = {
    val output = new StringAppender()
    encodeParam(value, output)
    output.result()
  }
  def encodeParam(value: A, output: StringAppender): Unit
}

trait ParamsTypeTraitsProvider {
  def typeTraits: TypeTraits
}

final class keyColumn extends scala.annotation.StaticAnnotation
final class ignoredColumn extends scala.annotation.StaticAnnotation

private[calldb] object PostgreSQLScalarValueEncoder {

  private[this] val encoderRegistry = PostgreSQLColumnEncoderRegistry.Instance

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
    val randomTag = PostgreSQLScalarValueEncoder.randomDollarQuoteTag()
    output += '$' += randomTag += '$'
    output += value
    output += '$' += randomTag += '$'
  }

  private val secureRandom = new SecureRandom()

  def randomDollarQuoteTag(): String = {
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
  * @param name Column unqualified name corresponding to a the database column name
  * @param fromEntity An extractor of entity field value corresponding to the column
  * @param tableNameSupplier A supplier of table name of enclosing [[GenericEntityTable]].
  *                          We use call-by-name table name supplier to avoid initialization order issues
  * @tparam E Entity type of an enclosing [[GenericEntityTable]] table definition
  * @tparam C Scala column type corresponding to the entity field
  * @todo Fix and specify type parameters variance
  */
final class TableColumn[E, C](
    val name: String,
    private[this] val fromEntity: E => C,
    private[this] val tableNameSupplier: => Option[String])(
    private[this] implicit val reader: ColumnReader[C],
    private[this] implicit val writer: ColumnWriter[C],
    private[this] val columnTypeProvider: ColumnTypeProvider[C]
  ) extends TypedCallable.ParamsDef[C] {

  // This field is set when checking database metadata
  var columnIndex: Int = -1

  override def toString = s"TableColumn($qualifiedName,$typeTraits)"

  override val typeTraits: BasicTypeTraits = columnTypeProvider.typeTraits

  def isNullable: Boolean = typeTraits.isNullable
  def isArray: Boolean = typeTraits.isInstanceOf[ArrayTypeTraits]

  def apply(e: E): NamedParamValue = NamedParamValue(qualifiedName, writer.write(fromEntity(e)))

  def extractAsAny(e: E): Any = writer.write(fromEntity(e))

  def of(value: C): NamedParamValue = NamedParamValue(qualifiedName, writer.write(value))

  lazy val tableName = tableNameSupplier
  lazy val qualifiedName: String = tableName.map(_ + "." + name).getOrElse(name).toLowerCase
  lazy val columnLabel = tableName.map(_ + "__" + name).getOrElse(name).toLowerCase

  def ofRow(rowData: RowData): C = reader.read(rowData(columnLabel))

  override def encodeParam(value: C, output: StringAppender): Unit = {
    output += name += ":="
    PostgreSQLScalarValueEncoder.encodeValue(value, output)
  }

  private[calldb] def encodeRowValue(value: Any, output: StringAppender): Unit =
    PostgreSQLScalarValueEncoder.encodeValue(value, output)
}

object TableColumn {
  type CR[C] = ColumnReader[C]
  type CW[C] = ColumnWriter[C]
  type CTP[C] = ColumnTypeProvider[C]

  def apply[E, C](name: String, fromEntity: E => C, tableName: => String)(implicit r: CR[C], w: CW[C], tp: CTP[C]) =
    new TableColumn(name, fromEntity, Some(tableName))

  def apply[E, C](name: Symbol, fromEntity: E => C, tableName: => String)(implicit r: CR[C], w: CW[C], tp: CTP[C]) =
    new TableColumn(name.name, fromEntity, Some(tableName))

  def apply[C](name: String, value: C)(implicit r: CR[C], w: CW[C], tp: CTP[C]): TableColumn[Unit, C] =
    new TableColumn(name, (u: Unit) => value, None)

  def apply[C](name: Symbol, value: C)(implicit r: CR[C], w: CW[C], tp: CTP[C]): TableColumn[Unit, C] =
    new TableColumn(name.name, (u: Unit) => value, None)
}
