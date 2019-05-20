package com.github.mirabout.calldb

import com.github.mauricio.async.db.{Connection, RowData}

import scala.concurrent.Future
import scala.collection.mutable

final case class TableName(exactName: String) {
  if (!exactName.startsWith("t")) {
    throw new IllegalArgumentException(s"Table name `$exactName` does not start with `t`")
  }
  if (exactName.length == 1) {
    throw new IllegalArgumentException(s"Table name consists of only `t` prefix")
  }

  val withoutPrefix = exactName.substring(1)
}

trait NamedTable {
  def tableName: TableName
}

trait GenericTable extends ColumnReaders with ColumnWriters with ColumnTypeProviders

trait GenericEntityTable[E] extends GenericTable with NamedTable with WithAllColumns[E] with WithAllProcedures[E] {

  def column[Column](name: Symbol, extractor: E => Column)(implicit r: ColumnReader[Column],
                                                           w: ColumnWriter[Column],
                                                           tp: BasicTypeProvider[Column]) =
    TableColumn.apply(name, extractor, this.tableName.exactName)

  protected var _allProcedures: Seq[ProceduresReflector.DefinedProcedure[_]] = null

  def allProcedures: Seq[ProceduresReflector.DefinedProcedure[_]] = {
    if (_allProcedures eq null) {
      throw new IllegalStateException(s"$this: `_allProcedures` var has not been set or injected")
    }
    _allProcedures
  }

  protected var _allColumns: IndexedSeq[TableColumn[E, _]] = null

  def allColumns: IndexedSeq[TableColumn[E, _]] = {
    if (_allColumns eq null) {
      throw new IllegalStateException(s"$this: `_allColumns` var has not been set or injected")
    }
    _allColumns
  }

  protected var _keyColumns: IndexedSeq[TableColumn[E, _]] = null

  def keyColumns: IndexedSeq[TableColumn[E, _]] = {
    if (_keyColumns eq null) {
      throw new IllegalStateException(s"$this: `_keyColumns` var has not been set or injected")
    }
    _keyColumns
  }

  final def nonKeyColumns: IndexedSeq[TableColumn[E, _]] = _nonKeyColumns

  final lazy val _nonKeyColumns: IndexedSeq[TableColumn[E, _]] = allColumns.diff(keyColumns)

  def entityParser: RowDataParser[E]

  protected def mkRowParser(parseFunc: RowData => E): RowDataParser[E] = {
    new RowDataParser[E](parseFunc) {
      override def expectedColumnsNames: Option[IndexedSeq[String]] = Some(allColumns.map(_.columnLabel))
    }
  }

  private lazy val EntityDef = EntityParamsDef(allColumns, tableName)
  private lazy val EntitiesArrayDef = EntitiesArrayParamsDef(allColumns, tableName)

  implicit lazy val entityTypeProvider: EntityParamsDef[E] = EntityDef

  lazy val pInsert = Procedure1(EntityDef).mapWith(_ == 1)

  def insert(entity: E)(implicit c: Connection): Future[Boolean] = pInsert(entity)

  lazy val pInsertMany = Procedure1(EntitiesArrayDef)

  def insertMany(entities: Traversable[E])(implicit c: Connection): Future[Boolean] = {
    if (entities.hasDefiniteSize) {
      val entitiesSize = entities.size
      pInsertMany.mapWith(_ == entitiesSize).apply(entities)
    } else {
      pInsertMany.mapWith(_ > 0).apply(entities)
    }
  }

  lazy val pUpdate = Procedure1(EntityDef).mapWith(_ == 1)

  def update(entity: E)(implicit c: Connection): Future[Boolean] = pUpdate(entity)

  lazy val pUpdateMany = Procedure1(EntitiesArrayDef)

  def updateMany(entities: Traversable[E])(implicit c: Connection): Future[Boolean] = {
    if (entities.hasDefiniteSize) {
      val entitiesSize = entities.size
      pUpdateMany.mapWith(_ == entitiesSize).apply(entities)
    } else {
      pUpdateMany.mapWith(_ > 0).apply(entities)
    }
  }

  lazy val pInsertOrUpdate = Procedure1(EntityDef).mapWith(_ == 1)
  lazy val pInsertOrIgnore = Procedure1(EntityDef).mapWith(_ == 1)

  def insertOrUpdate(entity: E)(implicit c: Connection): Future[Boolean] = pInsertOrUpdate(entity)
  def insertOrIgnore(entity: E)(implicit c: Connection): Future[Boolean] = pInsertOrIgnore(entity)

  def put(entity: E)(implicit c: Connection): Future[Boolean] = insertOrUpdate(entity)

  lazy val pInsertOrUpdateMany = Procedure1(EntitiesArrayDef)
  lazy val pInsertOrIgnoreMany = Procedure1(EntitiesArrayDef)

  def insertOrUpdateMany(entities: Traversable[E])(implicit c: Connection): Future[Boolean] = {
    if (entities.hasDefiniteSize) {
      val entitiesSize = entities.size
      pInsertOrUpdateMany.mapWith(_ == entitiesSize).apply(entities)
    } else {
      pInsertOrUpdateMany.mapWith(_ > 0).apply(entities)
    }
  }

  def putMany(entities: Traversable[E])(implicit c: Connection): Future[Boolean] = insertOrUpdateMany(entities)

  def insertOrIgnoreMany(entities: Traversable[E])(implicit c: Connection): Future[Boolean] = {
    if (entities.hasDefiniteSize) {
      val entitiesSize = entities.size
      pInsertOrIgnoreMany.mapWith(_ == entitiesSize).apply(entities)
    } else {
      pInsertOrIgnoreMany.mapWith(_ > 0).apply(entities)
    }
  }
}

trait WithAllColumns[+E] {
  def allColumns: IndexedSeq[TableColumn[E, _]]
}

trait WithAllProcedures[E] {
  def allProcedures: Traversable[TypedCallable[_]]
}
  
trait EntityTypeProvider[+E] extends TypeProvider[E] { self: WithAllColumns[E] =>
  val typeTraits = RowTypeTraits(allColumns map (_.typeTraits))
}

trait RowEncoder[+E] { self: WithAllColumns[E] with NamedTable =>
  // To avoid boxing, use specialized collection
  private[this] val columnByIndexMap: mutable.LongMap[TableColumn[E, Any]] = {
    val result = new mutable.LongMap[TableColumn[E, Any]](allColumns.size)
    allColumns.foreach(column => result += Tuple2(column.columnIndex, column.asInstanceOf[TableColumn[E, Any]]))
    result
  }

  protected final def encodeRow[EE >: E](value: EE, output: StringAppender): Unit = {
    output += "ROW("
    var i = -1
    while ({i += 1; i < allColumns.size}) {
      val column = columnByIndexMap(i)
      column.encodeRowValue(column.apply(value), output)
      output += ','
    }
    // Chop the last comma
    output.chopLast()
    // Close ROW(
    output += ")::" += tableName.exactName
  }
}

trait EntityParamsEncoder[+E] extends ParamsEncoder[E] with RowEncoder[E] { self: WithAllColumns[E] with NamedTable =>

  def encodeParam[EE >: E](value: EE, output: StringAppender): Unit = {
    output += s"$name := "
    encodeRow(value, output)
  }
}

final case class EntityParamsDef[+E](allColumns: IndexedSeq[TableColumn[E, _]], tableName: TableName)
  extends TypedCallable.ParamsDef[E] with EntityTypeProvider[E] with EntityParamsEncoder[E] with WithAllColumns[E] with NamedTable {
  override def name = "entity"
}

trait EntitiesArrayTypeProvider[+E] extends TypeProvider[Traversable[E]] { self: WithAllColumns[E] =>
  val typeTraits = RowSeqTypeTraits(RowTypeTraits(allColumns map (_.typeTraits)))
}

trait EntitiesArrayParamsEncoder[+E] extends ParamsEncoder[Traversable[E]] with RowEncoder[E] { self: WithAllColumns[E] with NamedTable =>

  def encodeParam[T >: Traversable[E]](values: T, output: StringAppender): Unit = {
    output += "entities := ARRAY["
    values match {
      case isq: IndexedSeq[E] => encodeSeq(isq, output)
      case itr: Iterable[E] => encodeItr(itr, output)
      case tvs: Traversable[E] => encodeTvs(tvs, output)
    }
    // Close ARRAY[
    output += ']'
  }

  private def encodeTvs[EE >: E](values: Traversable[EE], output: StringAppender): Unit = {
    var commaSet = false
    for (value <- values) {
      encodeRow(value, output)
      output += ','
      commaSet = true
    }
    // Chop last comma (if it has been set)
    if (commaSet)
      output.chopLast()
  }

  private def encodeItr[EE >: E](values: Iterable[EE], output: StringAppender): Unit = {
    val iterator = values.iterator
    val commaSet = iterator.hasNext
    while (iterator.hasNext) {
      encodeRow(iterator.next(), output)
      output += ','
    }
    // Chop last comma (if it has been set)
    if (commaSet)
      output.chopLast()
  }

  private def encodeSeq[EE >: E](values: Seq[EE], output: StringAppender): Unit = {
    var i = -1
    while ({i += 1; i < values.length}) {
      encodeRow(values(i), output)
      output += ','
    }
    // Chop last comma (if it has been set)
    if (values.nonEmpty)
      output.chopLast()
  }
}

final case class EntitiesArrayParamsDef[+E](allColumns: IndexedSeq[TableColumn[E, _]], tableName: TableName)
  extends TypedCallable.ParamsDef[Traversable[E]]
    with EntitiesArrayParamsEncoder[E] with EntitiesArrayTypeProvider[E]
    with WithAllColumns[E] with NamedTable {
  override val name = "entities"
}


