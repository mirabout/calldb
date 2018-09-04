package com.github.mirabout.calldb.generators

import com.github.mirabout.calldb.{TableName, TableColumn}

trait RetrieveByColumnsGenerator extends RetrieveByConditionGenerator {
  /**
    * All columns that should form a query condition.
    * This condition is made of pairs of column and the corresponding procedure parameter
    * (that has the same unqualified name followed by an underscore) compared by using an operator.
    * Currently only strict equality (and not other comparison operators) is used.
    * These comparison results are joint using a logical operator.
    * Currently only AND logical operator is used.
    * @note Should be overridden using a function or a lazy value to avoid initialization order issues
    */
  val conditionColumns: Traversable[TableColumn[_,_]]

  override val queryCondition: String = {
    assert(conditionColumns.nonEmpty)
    val sb = new StringBuilder()
    sb.append("(")
    for (column <- conditionColumns) {
      sb.append(s"(${tableName.exactName}.${column.name} = ${column.name}_) and ")
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
          sb.append(s"${column.name}_ ${basicTypeTraits.storedInType.sqlName}, ")
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