package com.github.mirabout.calldb

/**
  * A routine parameter definition.
  * Use `apply()` methods of this object to construct a lite
  * [[TypedCallable.ParamsDef]] instead of heavy-weight and confusing [[TableColumn]]
  */
object Param {
  def apply[A](paramName: String, someValueOfType: A)(implicit tp: BasicTypeProvider[A], w: ColumnWriter[A])
    : TypedCallable.ParamsDef[A] = new NonColumnParamDef[A](paramName)
  def apply[A](paramName: Symbol, someValueOfType: A)(implicit tp: BasicTypeProvider[A], w: ColumnWriter[A])
    : TypedCallable.ParamsDef[A] = new NonColumnParamDef[A](paramName.name)
}

private class NonColumnParamDef[A](val name: String)(implicit tp: BasicTypeProvider[A], w: ColumnWriter[A])
  extends TypedCallable.ParamsDef[A] {
  override def encodeParam[AA >: A](value: AA, output: StringAppender): Unit = {
    output += name += "_ :="
    // TODO: This cast is suspicious... check this
    PostgreSQLScalarValueEncoder.encodeValue(w.write(value.asInstanceOf[A]), output)
  }
  override val typeTraits: BasicTypeTraits = tp.typeTraits
}
