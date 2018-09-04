package com.github.mirabout.calldb.generators

import com.github.mirabout.calldb.{TableColumn, TableName}

trait RetrieveByConditionGenerator extends StatementsGenerator with ProcedureSqlGenerator {
  /**
    * A condition of a SQL query as a string, e.g. `tFoo_createdAt < now() - '1 hour'::interval`
    */
  val queryCondition: String
  /**
    * Arguments of a procedure as a string, e.g. `latitude_ float8, longitude_ float8`
    */
  val procedureArgs: String

  val tableName: TableName
  /**
    * All columns that should be present in the result.
    * They will be returned as columns of a corresponding table "qualified view"
    * (Refer to [[QualifiedViewGenerator]] for explanation).
    */
  val allColumns: Traversable[TableColumn[_,_]]
  /**
    * Additional procedure variable declarations if needed.
    * These declarations are put in `DECLARE` PL/pgSQL block
    * after a semicolon and must end with a semicolon.
    */
  val additionalVarDecls: Option[String] = None

  private def fullProcedureName = s"p${tableName.withoutPrefix}_$simpleProcedureName($procedureArgs)"

  private def mkQualifiedResultColumns(indentSpaces: Int = 4): String = {
    val b = new StringBuilder()
    for(column <- allColumns) {
      b ++= " " * indentSpaces
      b ++= "v." ++= tableName.exactName ++= "__" ++= column.name
      b ++= " = r." ++= column.name
      b ++= ";\n"
    }
    // Chop last ;\n
    if (allColumns.nonEmpty) {
      b.setLength(b.length - 2)
    }
    b.toString()
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