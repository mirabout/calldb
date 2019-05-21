package com.github.mirabout.calldb

import scala.reflect.runtime.universe

class ProceduresReflector extends ReflectionHelpers {
  private def isAProcedure(term: universe.TermSymbol) =
    doesVarLikeTermConformToType(term, universe.typeOf[Procedure[_]])

  def reflectAllProcedures(instance: AnyRef): Map[String, Procedure[_]] =
    reflectFieldsWithFilter(instance, isAProcedure).asInstanceOf[Map[String, Procedure[_]]]
}

object ProceduresReflector extends ProceduresReflector
