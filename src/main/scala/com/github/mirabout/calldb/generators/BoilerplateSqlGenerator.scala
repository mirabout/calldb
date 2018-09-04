package com.github.mirabout.calldb.generators

import com.github.mirabout.calldb.GenericEntityTable

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
    * @return A [[Traversable]] of [[GeneratorProps]].
    */
  def generatorProps(): Traversable[GeneratorProps]
}