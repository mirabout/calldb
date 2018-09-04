package com.github.mirabout.calldb.generators

import com.github.mirabout.calldb.GenericEntityTable

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
  *                   Having multiple [[GeneratorProps]] with the same order in a list is legal.
  *                   The order of execution of statements with the same order is not specified.
  *
  */
class GeneratorProps(val description: String, generator_ : => StatementsGenerator, val order: Int)
  extends Ordered[GeneratorProps] {
  if (order < 1) {
    throw new IllegalArgumentException(s"An order $order must be positive")
  }

  def generator: StatementsGenerator = generator_

  override def compare(that: GeneratorProps): Int =
    java.lang.Integer.compare(this.order, that.order)

  def createSql: Option[String] = generator.createSql
  def createOrReplaceSql: Option[String] = generator.createOrReplaceSql
  def dropSql: Option[String] = generator.dropSql
  def dropIfExistsSql: Option[String] = generator.dropIfExistsSql
}

object GeneratorProps {
  /**
    * A helper for [[GeneratorProps]] instances creation
    * (The class can't and shouldn't be defined as a case class)
    */
  def apply(description: String, generator: => StatementsGenerator, order: Int): GeneratorProps =
    new GeneratorProps(description, generator, order)

  /**
    * @note it has been found to be useful
    */
  def apply(descriptionAndGenerator: (String, () => StatementsGenerator), order: Int): GeneratorProps =
    new GeneratorProps(descriptionAndGenerator._1, descriptionAndGenerator._2(), order)
}