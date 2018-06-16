package com.github.mirabout.calldb

import java.util.regex.Pattern

/**
  * A generator for a database entity (a view, a table, a procedure, a trigger, etc)
  * that provides SQL string statements for CREATE and DROP operations.
  */
trait StatementsGenerator {
  /**
    * @return A SQL statement for a database entity CREATE statement if it can be and is defined
    */
  def createSql: Option[String] = Option(createSqlImpl)

  /**
    * @return A SQL statement for a database entity DROP statement if it can be and is defined
    */
  def dropSql: Option[String] = Option(dropSqlImpl)

  // These string operations are a bit lame, an AST ideally should be supplied and processed

  /**
    * @return A SQL statement for a database entity CREATE OR REPLACE statement if it can be and is defined
    */
  def createOrReplaceSql: Option[String] = createSql map { sql =>
    val trimmed = sql.trim()
    if (trimmed.indexOf("create ") != 0) {
      throw new AssertionError(s"The given SQL string `$trimmed` does not start with `create `")
    }
    "create or replace " + trimmed.drop("create ".length)
  }

  /**
    * @return A SQL statement for a database entity DROP IF EXISTS statement if it can be and is defined
    */
  def dropIfExistsSql: Option[String] = dropSql map { sql =>
    val matcher = Pattern.compile("\\s*drop\\s+\\b\\w+\\b\\s+").matcher(sql)
    if (!matcher.find()) {
      throw new AssertionError(s"Can't find the `drop <token> ` pattern at the start of the given SQL string `$sql`")
    }
    matcher.group(0) + "if exists " + sql.substring(matcher.end())
  }

  /**
    * Should be overridden in descendants if one needs to provide an actual CREATE statement.
    * We do not wrap results in [[Option]] to reduce boilerplate in actual implementations.
    */
  protected def createSqlImpl: String = null

  /**
    * Should be overridden in descendants if one needs to provide an actual DROP statement.
    * We do not wrap results in [[Option]] to reduce boilerplate in actual implementations.
    */
  protected def dropSqlImpl: String = null
}

object StatementsGenerator {
  /**
    * A helper for avoiding manual subclassing of [[StatementsGenerator]] trait.
    * @param create an expression producing "create" SQL statement
    * @param drop an expression producing "drop" SQL statement
    * @return a new [[StatementsGenerator]] that yields supplied statements
    */
  def apply(create: => String, drop: => String): StatementsGenerator = {
    new StatementsGenerator {
      override def createSqlImpl: String = getCheckingTokens(create, "create")
      override def dropSqlImpl: String = getCheckingTokens(drop, "drop")

      private def getCheckingTokens(sql: String, tokens: String*): String = {
        val lowercaseSql = sql.toLowerCase()
        if (!tokens.forall(t => lowercaseSql.contains(t.toLowerCase))) {
          throw new AssertionError(s"Some of $tokens are missing in $sql. Did you confuse create and drop statements?")
        }
        lowercaseSql
      }
    }
  }
}

/**
  * A container for instances of [[StatementsGenerator]] that adds a description
  * and allows ordering of generators to manually resolve generator dependencies.
  *
  * @param description A string description for the generator
  * @param generator_ A call-by-name generator provider.
  *                   Non-eager evaluation of the generator is important since they usually
  *                   refer to instances of [[GenericEntityTable]] which are prone to initialization order issues.
  * @param order      An order group of the generator. Must be a positive integer.
  *                   Lower the `order` is, earlier the generated SQL for entities creation should be executed.
  *                   Lower the `order` is, later the generated SQL for entities drop should be executed.
  *                   Having multiple [[StatementsGeneratorProps]] with the same order in a list is legal.
  *                   The order of execution of statements with the same order is not specified.
  *
  */
class StatementsGeneratorProps(val description: String, generator_ : => StatementsGenerator, val order: Int)
  extends Ordered[StatementsGeneratorProps] {
  if (order < 1) {
    throw new IllegalArgumentException(s"An order $order must be positive")
  }

  def generator: StatementsGenerator = generator_

  override def compare(that: StatementsGeneratorProps): Int =
    java.lang.Integer.compare(this.order, that.order)

  def createSql: Option[String] = generator.createSql
  def createOrReplaceSql: Option[String] = generator.createOrReplaceSql
  def dropSql: Option[String] = generator.dropSql
  def dropIfExistsSql: Option[String] = generator.dropIfExistsSql
}

object StatementsGeneratorProps {
  /**
    * A helper for [[StatementsGeneratorProps]] instances creation
    * (The class can't and shouldn't be defined as a case class)
    */
  def apply(description: String, generator: => StatementsGenerator, order: Int): StatementsGeneratorProps =
    new StatementsGeneratorProps(description, generator, order)

  /**
    * @note it has been found to be useful
    */
  def apply(descriptionAndGenerator: (String, () => StatementsGenerator), order: Int): StatementsGeneratorProps =
    new StatementsGeneratorProps(descriptionAndGenerator._1, descriptionAndGenerator._2(), order)
}

/**
  * A common supertype for helpers that aid reducing manual implementation
  * of trivial database entities (views, tables, procedures, triggers, etc).
  * If somebody needs to use SQL generation of this kind, he or she should
  * provide an instance of this trait (usually in a [[GenericEntityTable]] subclass)
  * and execute needed SQL statements provided by this instance in a transaction.
  * Each SQL statement is provided by [[StatementsGenerator]]
  * that joins dual SQL statements (CREATE/DROP) for a single entity.
  * Statements generators are joint in groups having string tags useful for logging/debugging.
  */
trait BoilerplateSqlGenerator {
 /**
  * @return A [[Traversable]] of [[StatementsGeneratorProps]].
  */
  def generatorProps(): Traversable[StatementsGeneratorProps]
}

trait RetrieveByConditionProcedureGenerator extends StatementsGenerator {
  val simpleProcedureName: String
  val queryCondition: String
  val procedureArgs: String
  val tableName: TableName
  val allColumns: Traversable[TableColumn[_,_]]
  val additionalVarDecls: Option[String] = None

  private def fullProcedureName = s"p${tableName.withoutPrefix}_$simpleProcedureName($procedureArgs)"

  private def mkQualifiedResultColumns(indentSpaces: Int = 4): String = {
    val sb = new StringBuilder()
    for(column <- allColumns) {
      sb.append(" " * indentSpaces)
      sb.append("v.").append(tableName.exactName).append("__").append(column.name)
      sb.append(" = r.").append(column.name)
      sb.append(";\n")
    }
    // Chop last ;\n
    if (allColumns.nonEmpty) {
      sb.setLength(sb.length - 2)
    }
    sb.toString()
  }

  override protected def createSqlImpl: String = {
    s"create function $fullProcedureName returns setof v${tableName.withoutPrefix} as $$$$\n" +
      s"declare\n" +
      s"  r ${tableName.exactName}%rowtype;\n" +
      s"  v v${tableName.withoutPrefix}%rowtype;\n" +
      additionalVarDecls.getOrElse("") +
      s"begin\n" +
      s"  for r in select * from ${tableName.exactName}\n" +
      s"  where $queryCondition\n" +
      s"  loop\n" +
      s"${mkQualifiedResultColumns()};\n" +
      s"    return next v;\n" +
      s"  end loop;\n" +
      s"  return;\n" +
      s"end;\n" +
      s"$$$$ language plpgsql"
  }

  override protected def dropSqlImpl: String =
    s"drop function $fullProcedureName"
}

trait RetrieveByColumnsProcedureGenerator extends RetrieveByConditionProcedureGenerator {
  val tableName: TableName
  /**
    * @note Should be overridden using a function or a lazy value to avoid initialization order issues
    */
  val conditionColumns: Traversable[TableColumn[_,_]]

  override val queryCondition: String = {
    assert(conditionColumns.nonEmpty)
    val sb = new StringBuilder()
    sb.append("(")
    for (column <- conditionColumns) {
      sb.append(s"(${tableName.exactName}.${column.name} = param_${column.name}) and ")
    }
    // Chop last " and "
    sb.setLength(sb.length - 5)
    sb.append(")")
    sb.toString()
  }

  override val procedureArgs: String = {
    assert(conditionColumns.nonEmpty)
    val sb = new StringBuilder()
    for (column <- conditionColumns) {
      column.typeTraits.asEitherBasicOrCompound match {
        case Left(basicTypeTraits) =>
          sb.append(s"${column.name} ${basicTypeTraits.storedInType.sqlName}, ")
        case Right(compoundTypeTraits) =>
          throw new AssertionError(s"Column $column has non-basic type traits $compoundTypeTraits")
      }
    }
    // Chop last comma and space
    sb.setLength(sb.length - 2)
    sb.toString()
  }

  override val additionalVarDecls: Option[String] = {
    assert(conditionColumns.nonEmpty)
    val sb = new StringBuilder()
    for (column <- conditionColumns) {
      column.typeTraits.asEitherBasicOrCompound match {
        case Left(basicTypeTraits) =>
          sb.append(s"  param_${column.name} ${basicTypeTraits.storedInType.sqlName} := ${column.name};\n")
        case Right(compoundTypeTraits) =>
          throw new AssertionError(s"Column $column has non-basic type traits $compoundTypeTraits")
      }
    }
    // Last ;\n are kept as the expected formatting requires
    Some(sb.toString())
  }
}

/**
  * A [[BoilerplateSqlGenerator]] that helps to create default procedures
  * expected by [[GenericEntityTable]], as well as views providing column names
  * expected by row data parsers.
  */
trait GenericEntityTableBoilerplateSqlGenerator extends BoilerplateSqlGenerator { self: GenericEntityTable[_] =>
  /**
    * It's better to avoid executing these tests right on trait construction to avoid initialiation order issues
    */
  private def checkKeyColumns() {
    if (self.keyColumns.isEmpty) {
      throw new AssertionError(s"GenericEntityTable($tableName): Key columns are not defined")
    }
    if (self.keyColumns.size > 2) {
      throw new AssertionError(s"GenericEntityTable($tableName): Multiple key columns are unsupported")
    }
  }

  private def primaryKeyName: String = {
    checkKeyColumns()
    keyColumns.head.name
  }

  private def qualifiedProcName(simpleProcName: String) =
    s"p${tableName.withoutPrefix}_$simpleProcName"

  private def scalarProcSignature(simpleProcName: String) =
    s"${qualifiedProcName(simpleProcName)}(entity ${tableName.exactName})"

  private def arrayProcSignature(simpleProcName: String) =
    s"${qualifiedProcName(simpleProcName)}(entities ${tableName.exactName}[])"

  private def dropScalarProcedureSql(simpleProcName: String) =
    s"drop function ${scalarProcSignature(simpleProcName)}"

  private def dropArrayProcedureSql(simpleProcName: String) =
    s"drop function ${arrayProcSignature(simpleProcName)}"

  private def mkUpdatePairs(insertNewLines: Boolean = true, indentSpaces: Int = 2): String = {
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

  def insertSql: StatementsGenerator = new StatementsGenerator {
    override protected def createSqlImpl(): String = {
      checkKeyColumns()
      s"create function ${scalarProcSignature("Insert")} returns bigint as $$$$\n" +
      s"declare\n" +
      s"  retval bigint;" +
      s"begin\n" +
      s"  insert into ${tableName.exactName} select * from entity;\n" +
      s"  get diagnostics retval = row_count;\n" +
      s"  return retval;\n" +
      s"end;\n" +
      s"$$$$ language plpgsql"
    }

    override protected def dropSqlImpl: String = dropScalarProcedureSql("Insert")
  }

  def updateSql: StatementsGenerator = new StatementsGenerator {
    override protected def createSqlImpl: String = {
      checkKeyColumns()
      s"create function ${scalarProcSignature("Update")} returns bigint as $$$$\n" +
      s"declare\n" +
      s"  retval bigint;\n" +
      s"begin\n" +
      s"  update ${tableName.exactName} set\n" +
      s"${mkUpdatePairs()};\n" +
      s"  get diagnostics retval = row_count;\n" +
      s"  return retval;\n" +
      s"end;\n" +
      s"$$$$ language plpgsql"
    }

    override protected def dropSqlImpl: String = dropScalarProcedureSql("Update")
  }

  def insertManySql: StatementsGenerator = new StatementsGenerator {
    override protected def createSqlImpl: String = {
      checkKeyColumns()
      s"create function ${arrayProcSignature("InsertMany")} returns bigint as $$$$\n" +
      s"declare\n" +
      s"  retval bigint;\n" +
      s"begin\n" +
      s"  insert into ${tableName.exactName} select * from unnest(entities);\n" +
      s"  get diagnostics retval = row_count;\n" +
      s"  return retval;\n" +
      s"end;\n" +
      s"$$$$ language plpgsql"
    }

    override protected def dropSqlImpl: String = dropArrayProcedureSql("InsertMany")
  }

  def updateManySql: StatementsGenerator = new StatementsGenerator {
    override protected def createSqlImpl: String = {
      checkKeyColumns()
      s"create function ${arrayProcSignature("UpdateMany")} returns bigint as $$$$" +
      s"declare\n" +
      s"  retval bigint;\n" +
      s"begin\n" +
      s"  retval := 0;\n" +
      s"  for i in 1..array_length(entities) loop\n" +
      s"    retval := retval + ${qualifiedProcName("Update")}(entities[i]);\n" +
      s"  end loop;\n" +
      s"  return retval;\n" +
      s"end;\n" +
      s"$$$$ language plpgsql"
    }

    override protected def dropSqlImpl: String = dropArrayProcedureSql("UpdateMany")
  }

  def insertOrIgnoreSql: StatementsGenerator = new StatementsGenerator {
    override protected def createSqlImpl: String = {
      checkKeyColumns()
      s"create function ${scalarProcSignature("InsertOrIgnore")} returns bigint as $$$$" +
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

    override protected def dropSqlImpl: String = dropScalarProcedureSql("InsertOrIgnore")
  }

  def insertOrUpdateSql: StatementsGenerator = new StatementsGenerator {
    override protected def createSqlImpl: String = {
      checkKeyColumns()
      s"create function ${scalarProcSignature("InsertOrUpdate")} returns bigint as $$$$" +
      s"declare\n" +
      s"  retval bigint;\n" +
      s"begin\n" +
      s"  insert into ${tableName.exactName} select * from entity\n" +
      s"  on conflict($primaryKeyName) do\n" +
      s"  update set\n" +
      s"${mkUpdatePairs()};\n" +
      s"  get diagnostics retval = row_count;\n" +
      s"  return retval;\n" +
      s"end;\n" +
      s"$$$$ language plpgsql"
    }

    override protected def dropSqlImpl: String = dropScalarProcedureSql("InsertOrUpdate")
  }

  def insertOrIgnoreManySql: StatementsGenerator = new StatementsGenerator {
    override protected def createSqlImpl: String = {
      checkKeyColumns()
      s"create function ${arrayProcSignature("InsertOrIgnoreMany")} returns bigint as $$$$" +
      s"declare\n" +
      s"  retval bigint;\n" +
      s"begin\n" +
      s"  retval := 0;\n" +
      s"  for i in 1..array_size(entities) loop\n" +
      s"    retval := retval + ${qualifiedProcName("InsertOrIgnore")}(entities[i]);\n" +
      s"  end loop;\n" +
      s"  return retval;\n" +
      s"end;\n" +
      s"$$$$ language plpgsql"
    }

    override protected def dropSqlImpl: String = dropArrayProcedureSql("InsertOrIgnoreMany")
  }

  def insertOrUpdateManySql: StatementsGenerator = new StatementsGenerator {
    override def createSqlImpl: String = {
      checkKeyColumns()
      s"create function ${arrayProcSignature("InsertOrUpdateMany")} returns bigint as $$$$" +
      s"declare\n" +
      s"  retval bigint;\n" +
      s"begin\n" +
      s"  retval := 0;\n" +
      s"  for i in 1..array_size(entities) loop\n" +
      s"    retval := retval + ${qualifiedProcName("InsertOrUpdate")}(entities[i]);\n" +
      s"  end loop;\n" +
      s"  return retval;\n" +
      s"end;\n" +
      s"$$$$ language plpgsql"
    }

    override protected def dropSqlImpl: String = dropArrayProcedureSql("InsertOrUpdateMany")
  }

  def viewSql: StatementsGenerator = new StatementsGenerator {
    override def createSqlImpl: String = {
      val sb = new StringBuilder()
      sb.append(s"create view v${tableName.withoutPrefix} as select\n")
      for (column <- allColumns) {
        sb.append(" " * 2)
        sb.append(column.name).append(s" as ${tableName.exactName}__").append(column.name).append(",\n")
      }
      if (sb.nonEmpty) {
        // Chop last comma \n
        sb.setLength(sb.length - 2)
      }
      sb.append(s" from ${tableName.exactName}")
      sb.toString()
    }

    override def dropSqlImpl: String =
      s"drop view v${tableName.withoutPrefix}"
  }

  /**
    * Provides a generator for procedures like `pQualifyColumn(t tTable) returns vTable`
    * where `vTable` is a view generated by the [[viewSql]] generator.
    */
  def qualifyColumnsSql: StatementsGenerator = new StatementsGenerator {
    override protected def createSqlImpl: String = {
      val sb = new StringBuilder()
      sb.append(s"create function pQualifyColumns(t ${tableName.exactName}) ")
      sb.append(s"returns v${tableName.withoutPrefix} as $$$$\n")
      sb.append(s"declare\n")
      sb.append(s"  v v${tableName.withoutPrefix};\n")
      sb.append("begin\n")
      for (column <- allColumns) {
        sb.append(s"  v.${tableName.exactName}__${column.name} = t.${column.name};\n")
      }
      sb.append("  return v;\n")
      sb.append("end;\n")
      sb.append(s"$$$$ language plpgsql")
      sb.toString()
    }

    override protected def dropSqlImpl: String =
      s"drop function pQualifyColumns(t ${tableName.exactName})"
  }

  private def newProps(description: String, generator: => StatementsGenerator, order: Int) =
    new StatementsGeneratorProps(description, generator, order)

  def generatorProps(): Traversable[StatementsGeneratorProps] = Traversable(
    newProps("insert procedure", insertSql, 3),
    newProps("update procedure", updateSql, 3),
    newProps("insert many procedure", insertManySql, 3),
    newProps("update many procedure", updateManySql, 3),
    newProps("insert or ignore procedure", insertOrIgnoreSql, 3),
    newProps("insert or update procedure", insertOrUpdateSql, 3),
    newProps("insert or ignore many procedure", insertOrIgnoreManySql, 3),
    newProps("insert or update many procedure", insertOrUpdateManySql, 3),
    newProps("qualify columns procedure", qualifyColumnsSql, 2),
    newProps("fully qualified fields view", viewSql, 1))
}
