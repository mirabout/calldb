package com.github.mirabout.calldb.generators

import com.github.mirabout.calldb.{GenericEntityTable, TableColumn, TableName}

/**
  * A common supertype for statements generators that are intended
  * to belong to a [[GenericEntityTable]] but instantiated outside of a table scope.
  * @note Keeping these classes in the inner scope of [[GenericEntityTable]] might seem better,
  *       but we want to do the opposite deliberately.
  *       It has been proven in practice that a user of table descendants
  *       might want to filter generators by their exact type and not by a string tag.
  *       Moreover, the implementation of generators is cleaner even if it requires a bit more code.
  */
class TableStatementsGenerator(parent: GenericEntityTable[_]) extends StatementsGenerator {
  protected def allColumns: IndexedSeq[TableColumn[_, _]] = parent.allColumns
  protected def keyColumns: IndexedSeq[TableColumn[_, _]] = parent.keyColumns
  protected def nonKeyColumns: IndexedSeq[TableColumn[_, _]] = parent.nonKeyColumns
  protected def tableName: TableName = parent.tableName

  /**
    * It's better to avoid executing these tests right on trait construction to avoid initialization order issues
    */
  protected def checkKeyColumns() {
    if (keyColumns.isEmpty) {
      throw new AssertionError(s"GenericEntityTable(${parent.tableName}): Key columns are not defined")
    }
    if (keyColumns.size > 2) {
      throw new AssertionError(s"GenericEntityTable(${parent.tableName}): Multiple key columns are unsupported")
    }
  }

  protected def primaryKeyName: String = {
    checkKeyColumns()
    keyColumns.head.name
  }
}

/**
  * This should be a common supertype for all statements generators that generate procedures.
  */
trait ProcedureSqlGenerator extends StatementsGenerator {
  /**
    * A procedure name without "pTable_" prefix
    */
  protected def simpleProcedureName: String
}

/**
  * This trait contains implemented utility methods that might be useful for concrete generator implementations.
  */
trait TableProcedureGenerator extends ProcedureSqlGenerator { self: TableStatementsGenerator =>
  /**
    * Given a simple procedure name, constructs a qualified name
    * by adding a prefix that consists of "p", a table name
    * (without "t" prefix) and an underscore.
    */
  protected def qualifiedProcedureName(simpleProcedureName: String) =
    s"p${tableName.withoutPrefix}_$simpleProcedureName"

  /**
    * Similar to [[qualifiedProcedureName(String)]], uses [[simpleProcedureName]].
    */
  protected def qualifiedProcedureName: String =
    qualifiedProcedureName(this.simpleProcedureName)

  /**
    * Given a simple procedure name, constructs a drop statement
    * that drops a procedure that operates on a single entity.
    */
  protected def dropScalarProcedureSql(simpleProcedureName: String) =
    s"drop function ${scalarProcedureSignature(simpleProcedureName)}"

  /**
    * Given a simple procedure name, constructs a signature of a procedure
    * that operates on a single entity. The signature does not include a return type.
    */
  protected def scalarProcedureSignature(simpleProcedureName: String) =
    s"${qualifiedProcedureName(simpleProcedureName)}(entity ${tableName.exactName})"

  /**
    * Given a simple procedure name, constructs a drop statement
    * that drops a procedure that operates on array of entities.
    */
  protected def dropArrayProcedureSql(simpleProcedureName: String) =
    s"drop function ${arrayProcedureSignature(simpleProcedureName)}"

  /**
    * Given a simple procedure name, constructs a signature of a procedure
    * that operates on a single entity. The signature does not include a return type.
    */
  protected def arrayProcedureSignature(simpleProcedureName: String) =
    s"${qualifiedProcedureName(simpleProcedureName)}(entities ${tableName.exactName}[])"
}

/**
  * A trait that is ready to be mixed in to an implementation of a [[TableStatementsGenerator]]
  * to provide a [[TableStatementsGenerator.dropSqlImpl]] code that drops a procedure that operates on a single entity
  * and provide some other utilities.
  */
trait ScalarProcedureGenerator extends TableProcedureGenerator { self: TableStatementsGenerator =>
  def dropScalarProcedureSql: String = dropScalarProcedureSql(simpleProcedureName)
  def scalarProcedureSignature: String = scalarProcedureSignature(simpleProcedureName)

  override def dropSqlImpl: String = dropScalarProcedureSql(simpleProcedureName)
}

/**
  * A trait that is ready to be mixed in to an implementation of a [[TableStatementsGenerator]]
  * to provide a [[TableStatementsGenerator.dropSqlImpl]] code that drops a procedure that operates on many entities
  * and provide some other utilities.
  */
trait ArrayProcedureGenerator extends TableProcedureGenerator { self: TableStatementsGenerator =>
  def arrayProcedureSignature: String = arrayProcedureSignature(simpleProcedureName)
  def dropArrayProcedureSql: String = dropArrayProcedureSql(simpleProcedureName)

  override def dropSqlImpl: String = dropArrayProcedureSql(simpleProcedureName)
}

/**
  * A [[BoilerplateSqlGenerator]] that helps to create default procedures
  * expected by [[GenericEntityTable]], as well as views providing column names
  * expected by row data parsers.
  */
trait GenericEntityTableSqlGenerator extends BoilerplateSqlGenerator { self: GenericEntityTable[_] =>
  /**
    * Creates a generator that manages INSERT procedure that accepts a single entity.
    */
  def generatorOfInsert: StatementsGenerator = new InsertGenerator(this)

  /**
    * Creates a generator that manages UPDATE procedure that accepts a single entity.
    */
  def generatorOfUpdate: StatementsGenerator = new UpdateGenerator(this)

  /**
    * Creates a generator that manages INSERT procedure that accept many entities (and may accept none).
    */
  def generatorOfInsertMany: StatementsGenerator = new InsertManyGenerator(this)

  /**
    * Creates a generator that manages UPDATE procedure that accept many entities (and may accept none).
    */
  def generatorOfUpdateMany: StatementsGenerator = new UpdateManyGenerator(this)

  /**
    * Creates a generator that manages INSERT procedure that does nothing on conflict.
    */
  def generatorOfInsertOrIgnore: StatementsGenerator = new InsertOrIgnoreGenerator(this)

  /**
    * Creates a generator that manages INSERT procedure that updates an entity on conflict.
    */
  def generatorOfInsertOrUpdate: StatementsGenerator = new InsertOrUpdateGenerator(this)

  /**
    * Creates a generator that manages INSERT procedure that accepts many entities (and may accept none).
    * Nothing is performed on conflict for an inserted entity.
    */
  def generatorOfInsertOrIgnoreMany: StatementsGenerator = new InsertOrIgnoreManyGenerator(this)

  /**
    * Creates a generator that manages INSERT procedure that accepts many entities (and may accept none).
    * An entity gets updated on conflict for an inserted entity.
    */
  def generatorOfInsertOrUpdateMany: StatementsGenerator = new InsertOrUpdateManyGenerator(this)

  /**
    * @see [[QualifiedViewGenerator]]
    */
  def generatorOfQualifiedViews: StatementsGenerator = new QualifiedViewGenerator(this)

  /**
    * @see [[QualifyColumnsGenerator]]
    */
  def generatorOfQualifyColumns: StatementsGenerator = new QualifyColumnsGenerator(this)

  /**
    * Returns all known generator props that manage database schema state.
    * Descendants should override this call if addition of own generators is needed
    * and add their own generator props to a result of this call.
    * Also some of these default props can be excluded by descendants if necessarily.
    */
  def generatorProps(): Traversable[GeneratorProps] = Traversable(
    GeneratorProps("insert procedure", generatorOfInsert, order = 3),
    GeneratorProps("update procedure", generatorOfUpdate, order = 3),
    GeneratorProps("insert many procedure", generatorOfInsertMany, order = 3),
    GeneratorProps("update many procedure", generatorOfUpdateMany, order = 3),
    GeneratorProps("insert or ignore procedure", generatorOfInsertOrIgnore, order = 3),
    GeneratorProps("insert or update procedure", generatorOfInsertOrUpdate, order = 3),
    GeneratorProps("insert or ignore many procedure", generatorOfInsertOrIgnoreMany, order = 3),
    GeneratorProps("insert or update many procedure", generatorOfInsertOrUpdateMany, order = 3),
    GeneratorProps("qualify columns procedure", generatorOfQualifyColumns, order = 2),
    GeneratorProps("fully qualified fields view", generatorOfQualifiedViews, order = 1))
}

private[generators] trait InsertOrUpdateHelpers { self: TableStatementsGenerator =>
  private[generators] def mkUpdatePairs(insertNewLines: Boolean = true, indentSpaces: Int = 2): String = {
    val sb = new StringBuilder()
    for (column <- nonKeyColumns) {
      sb.append(" " * indentSpaces).append(column.name).append(" = ").append("entity.").append(column.name).append(',')
      if (insertNewLines) {
        sb.append('\n')
      }
    }
    if (sb.nonEmpty) {
      // Chop last comma and/or \n
      sb.setLength(sb.length - (if (insertNewLines) 2 else 1))
    }
    sb.toString()
  }
}

/**
  * Manages generation of an INSERT procedure that accepts a single entity
  */
class InsertGenerator(table: GenericEntityTable[_])
  extends TableStatementsGenerator(table)
    with ScalarProcedureGenerator {
  val simpleProcedureName = "Insert"

  override protected def createSqlImpl(): String = {
    checkKeyColumns()
    s"create function $scalarProcedureSignature returns bigint as $$$$\n" +
    s"declare\n" +
    s"  retval bigint;\n" +
    s"begin\n" +
    s"  insert into ${tableName.exactName} select * from entity;\n" +
    s"  get diagnostics retval = row_count;\n" +
    s"  return retval;\n" +
    s"end;\n" +
    s"$$$$ language plpgsql"
  }
}

/**
  * Manages generation of UPDATE procedure that accepts a single entity
  */
class UpdateGenerator(table: GenericEntityTable[_])
  extends TableStatementsGenerator(table)
    with InsertOrUpdateHelpers
      with ScalarProcedureGenerator {
  val simpleProcedureName = "Update"

  override def createSqlImpl: String = {
    checkKeyColumns()
    s"create function $scalarProcedureSignature returns bigint as $$$$\n" +
    s"declare\n" +
    s"  retval bigint;\n" +
    s"begin\n" +
    s"  update ${tableName.exactName} set\n" +
    s"${mkUpdatePairs(indentSpaces = 4)}\n" +
    s"  where id = entity.id;\n" +
    s"  get diagnostics retval = row_count;\n" +
    s"  return retval;\n" +
    s"end;\n" +
    s"$$$$ language plpgsql"
  }
}

/**
  * Manages generation of INSERT procedure that accepts multiple entities (and may accept zero)
  */
class InsertManyGenerator(table: GenericEntityTable[_])
  extends TableStatementsGenerator(table)
    with InsertOrUpdateHelpers
      with ArrayProcedureGenerator {
  val simpleProcedureName = "InsertMany"

  override protected def createSqlImpl: String = {
    checkKeyColumns()
    s"create function $arrayProcedureSignature returns bigint as $$$$\n" +
    s"declare\n" +
    s"  retval bigint;\n" +
    s"begin\n" +
    s"  insert into ${tableName.exactName} select * from unnest(entities);\n" +
    s"  get diagnostics retval = row_count;\n" +
    s"  return retval;\n" +
    s"end;\n" +
    s"$$$$ language plpgsql"
  }
}

/**
  * Manages generation of UPDATE procedure that accepts multiple entities (and may accept zero)
  */
class UpdateManyGenerator(table: GenericEntityTable[_])
  extends TableStatementsGenerator(table)
    with InsertOrUpdateHelpers
      with ArrayProcedureGenerator {
  val simpleProcedureName = "UpdateMany"

  override protected def createSqlImpl: String = {
    checkKeyColumns()
    s"create function $arrayProcedureSignature returns bigint as $$$$" +
    s"declare\n" +
    s"  retval bigint;\n" +
    s"begin\n" +
    s"  retval := 0;\n" +
    s"  for i in 1..array_length(entities, 1) loop\n" +
    s"    retval := retval + ${qualifiedProcedureName("Update")}(entities[i]);\n" +
    s"  end loop;\n" +
    s"  return retval;\n" +
    s"end;\n" +
    s"$$$$ language plpgsql"
  }
}

/**
  * Manages generation of INSERT procedure that accepts a single entity and does nothing on conflict.
  */
class InsertOrIgnoreGenerator(table: GenericEntityTable[_])
  extends TableStatementsGenerator(table)
    with ScalarProcedureGenerator {
  val simpleProcedureName = "InsertOrIgnore"

  override protected def createSqlImpl: String = {
    checkKeyColumns()
    s"create function $scalarProcedureSignature returns bigint as $$$$" +
    s"declare\n" +
    s"  retval bigint;\n" +
    s"begin\n" +
    s"  insert into ${tableName.exactName} select * from entity\n" +
    s"  on conflict($primaryKeyName) do nothing;\n" +
    s"  get diagnostics retval = row_count;\n" +
    s"  return retval;\n" +
    s"end;\n" +
    s"$$$$ language plpgsql"
  }
}

/**
  * Manages generation of UPDATE procedure that accepts a single entity and updates one on conflict.
  */
class InsertOrUpdateGenerator(table: GenericEntityTable[_])
  extends TableStatementsGenerator(table)
    with ScalarProcedureGenerator
      with InsertOrUpdateHelpers {
  val simpleProcedureName = "InsertOrUpdate"

  override protected def createSqlImpl: String = {
    checkKeyColumns()
    s"create function $scalarProcedureSignature returns bigint as $$$$" +
    s"declare\n" +
    s"  retval bigint;\n" +
    s"begin\n" +
    s"  insert into ${tableName.exactName} select * from entity\n" +
    s"  on conflict($primaryKeyName) do\n" +
    s"  update set\n" +
    s"${mkUpdatePairs(indentSpaces = 4)};\n" +
    s"  get diagnostics retval = row_count;\n" +
    s"  return retval;\n" +
    s"end;\n" +
    s"$$$$ language plpgsql"
  }
}

/**
  * Manages generation of INSERT procedure that accepts multiple entities (and may accept zero).
  * Nothing is performed on conflict for an inserted entity.
  */
class InsertOrIgnoreManyGenerator(table: GenericEntityTable[_])
  extends TableStatementsGenerator(table)
    with ArrayProcedureGenerator {
  val simpleProcedureName = "InsertOrIgnoreMany"

  override protected def createSqlImpl: String = {
    checkKeyColumns()
    s"create function $arrayProcedureSignature returns bigint as $$$$" +
    s"declare\n" +
    s"  retval bigint;\n" +
    s"begin\n" +
    s"  retval := 0;\n" +
    s"  for i in 1..array_length(entities, 1) loop\n" +
    s"    retval := retval + ${qualifiedProcedureName("InsertOrIgnore")}(entities[i]);\n" +
    s"  end loop;\n" +
    s"  return retval;\n" +
    s"end;\n" +
    s"$$$$ language plpgsql"
  }
}

/**
  * Manages generation of INSERT procedure that accepts multiple entities (and may accept zero).
  * An entity gets updated on conflict for an inserted entity.
  */
class InsertOrUpdateManyGenerator(table: GenericEntityTable[_])
  extends TableStatementsGenerator(table)
    with ArrayProcedureGenerator {
  val simpleProcedureName = "InsertOrUpdateMany"

  override def createSqlImpl: String = {
    checkKeyColumns()
    s"create function $arrayProcedureSignature returns bigint as $$$$" +
    s"declare\n" +
    s"  retval bigint;\n" +
    s"begin\n" +
    s"  retval := 0;\n" +
    s"  for i in 1..array_length(entities, 1) loop\n" +
    s"    retval := retval + ${qualifiedProcedureName("InsertOrUpdate")}(entities[i]);\n" +
    s"  end loop;\n" +
    s"  return retval;\n" +
    s"end;\n" +
    s"$$$$ language plpgsql"
  }
}

/**
  * Creates a "qualified view" for a table.
  * Assuming the table must have its name starting with "t",
  * a "qualified view" is a view of the entire table with the same name (but with "v" prefix)
  * that has table column names prefixed by the full name of table and double underscores.
  * This is useful to make a distinction of columns in a result of a join.
  * Even if there is no disambiguation required, this should be preferred for operating on data in uniform fashion.
  * [[GenericEntityTable]]-related stuff assumes that every column has a name decorated this way.
  */
class QualifiedViewGenerator(table: GenericEntityTable[_]) extends TableStatementsGenerator(table) {
  override def createSqlImpl: String = {
    val b = new StringBuilder()
    b ++= s"create view v${tableName.withoutPrefix} as select\n"
    for (column <- allColumns) {
      b ++= (" " * 2) ++= column.name ++= s" as ${tableName.exactName}__" ++= column.name ++= ",\n"
    }
    if (b.nonEmpty) {
      // Chop last comma \n
      b.setLength(b.length - 2)
    }
    b ++= s" from ${tableName.exactName}"
    b.toString()
  }

  override def dropSqlImpl: String =
    s"drop view v${tableName.withoutPrefix}"
}

/**
  * A generator for procedures like `pQualifyColumn(t tTable) returns vTable`
  * where `vTable` is a "qualified view" generated by the [[QualifiedViewGenerator]].
  * This procedure allows convenient conversion from a record of a table row type
  * to a record of the "qualified view" type in custom procedures implementation.
  */
class QualifyColumnsGenerator(table: GenericEntityTable[_]) extends TableStatementsGenerator(table) {
  override protected def createSqlImpl: String = {
    val b = new StringBuilder()
    b ++= s"create function pQualifyColumns(t ${tableName.exactName}) "
    b ++= s"returns v${tableName.withoutPrefix} as $$$$\n"
    b ++= s"declare\n"
    b ++= s"  v v${tableName.withoutPrefix};\n"
    b ++= "begin\n"
    for (column <- allColumns) {
      b ++= s"  v.${tableName.exactName}__${column.name} = t.${column.name};\n"
    }
    b ++= "  return v;\n"
    b ++= "end;\n"
    b ++= s"$$$$ language plpgsql"
    b.toString()
  }

  override protected def dropSqlImpl: String =
    s"drop function pQualifyColumns(t ${tableName.exactName})"
}