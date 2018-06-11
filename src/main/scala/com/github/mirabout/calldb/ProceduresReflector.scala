package com.github.mirabout.calldb

import scala.reflect.runtime.universe

class ProceduresReflector extends ReflectionHelpers {

  import ProceduresReflector.DefinedProcedure

  private def isAProcedure(term: universe.TermSymbol) =
    doesVarLikeTermConformToType(term, universe.typeOf[DefinedProcedure[_]])

  def reflectAllProcedures(instance: AnyRef): Map[String, DefinedProcedure[_]] =
    reflectFieldsWithFilter(instance, isAProcedure).asInstanceOf[Map[String, DefinedProcedure[_]]]

  def injectProcedureMemberNames(procedures: Map[String, DefinedProcedure[_]]): Unit =
    for ((name, p) <- procedures) p._nameAsMember = name
}

object ProceduresReflector extends ProceduresReflector {
  type DefinedProcedure[R] = UntypedRoutine with TypedCallable[R]
}
