package com.github.mirabout.calldb.generators

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