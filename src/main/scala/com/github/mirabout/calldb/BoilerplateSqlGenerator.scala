package com.github.mirabout.calldb

import java.util.regex.Pattern

/**
  * A common supertype for helpers that aid reducing manual implementation
  * of trivial database entities (views, tables, procedures, triggers, etc).
  * If somebody needs to use SQL generation of this kind, he or she should
  * provide an instance of this trait (usually in a [[GenericEntityTable]] subclass)
  * and execute needed SQL statements provided by this instance in a transaction.
  * Each SQL statement is provided by [[BoilerplateSqlGenerator#StatementsGenerator]]
  * that joins dual SQL statements (CREATE/DROP) for a single entity.
  * Statements generators are joint in groups having string tags useful for logging/debugging.
  */
trait BoilerplateSqlGenerator {

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
      if (sql.indexOf("create ") != 0) {
        throw new AssertionError(s"The given SQL string `$sql` does not start with `create `")
      }
      "create or replace " + sql.drop("create ".length)
    }

    /**
      * @return A SQL statement for a database entity DROP IF EXISTS statement if it can be and is defined
      */
    def dropIfExistsSql: Option[String] = dropSql map { sql =>
      val matcher = Pattern.compile("drop\\s+\\b\\w+\\b\\s+").matcher(sql)
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

/**
  * @return A [[Seq]] of generator groups.
  *         Each group consists of a string description and an actual list of generators tagged by strings as well.
  *         Making separated groups tagged by strings helps in providing useful log/debug info.
  * @note We use a [[Seq]] type because generation order might be important.
  */
  def boilerplateGeneratorGroups(): Seq[(String, Seq[(String, StatementsGenerator)])]
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

  def proceduresGenerators: Seq[(String, StatementsGenerator)] = Seq(
    ("insert procedure", insertSql),
    ("update procedure", updateSql),
    ("insert many procedure", insertManySql),
    ("update many procedure", updateManySql),
    ("insert or ignore procedure", insertOrIgnoreSql),
    ("insert or update procedure", insertOrUpdateSql),
    ("insert or ignore many procedure", insertOrIgnoreManySql),
    ("insert or update many procedure", insertOrUpdateManySql))

  def viewsGenerators: Seq[(String, StatementsGenerator)] = Seq(("fully qualified fields view", viewSql))

  override def boilerplateGeneratorGroups: Seq[(String, Seq[(String, StatementsGenerator)])] = {
    Seq(("procedures", proceduresGenerators), ("views", viewsGenerators))
  }
}
