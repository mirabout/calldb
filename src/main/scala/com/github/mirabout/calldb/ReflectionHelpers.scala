package com.github.mirabout.calldb

import scala.reflect.runtime.universe
import scala.collection.mutable

trait ReflectionHelpers {
  /**
    * Gets terms of the type and its supertypes which conform to the predicate
    * @note Beware of the result iteration behavior!
    *       Convert to a list to be able to iterate again!
    */
  def getTermsWithFilter(t: universe.Type, p: universe.TermSymbol => Boolean): Iterable[universe.TermSymbol] = {
    val checkedClasses = new mutable.HashSet[universe.ClassSymbol]
    checkedClasses.add(t.typeSymbol.asClass)
    // Get the terms defined only within the current type
    def getOwnTermsWithFilter(t: universe.Type, p: universe.TermSymbol => Boolean) = {
      for (m <- t.members.view if m.isTerm && p(m.asTerm)) yield m.asTerm
    }
    // Recurse capturing mutable sets defined once in the non-recursive part
    def getTermsWithFilterRec(t: universe.Type, p: universe.TermSymbol => Boolean): Iterable[universe.TermSymbol] = {
      (for {
        c <- t.baseClasses.view if c.isClass && checkedClasses.add(c.asClass)
        t <- getTermsWithFilterRec(c.asClass.toType, p)
      } yield t) ++ getOwnTermsWithFilter(t, p)
    }

    getTermsWithFilterRec(t, p)
  }

  /**
    * Checks whether the type of this var, val or lazy val has an exact match with the specified type
    */
  def doesVarLikeTermMatchType(term: universe.TermSymbol, tpe: universe.Type): Boolean = {
    doesTermRelateToType(term, tpe, (tt, t) => tt =:= t)
  }

  /**
    * Checks whether the type of this var, val or lazy val has an conformance ("is-a") with the specified type
    */
  def doesVarLikeTermConformToType(term: universe.TermSymbol, tpe: universe.Type): Boolean =
    doesTermRelateToType(term, tpe, (tt, t) => tt <:< t)

  // Note: I do not want to add a weak conformance here since it is only confusing in reflection context

  private def doesTermRelateToType(term: universe.TermSymbol, tpe: universe.Type,
                                   op: (universe.Type, universe.Type) => Boolean) = {
    val typeErasure = tpe.erasure
    val termType = term.typeSignature
    op(termType.erasure, typeErasure) || (term.isLazy && op(termType.resultType.erasure, typeErasure))
  }

  /**
    * Reflects fields: val's, var's or lazy val's that match a specified condition.
    * @return A [[Map]] of fields names and corresponding fields
    */
  def reflectFieldsWithFilter(instance: AnyRef, filter: universe.TermSymbol => Boolean): Map[String, Any] = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = runtimeMirror.reflect(instance)
    val typeMirror = instanceMirror.symbol.toType

    val filteredTermSymbols = getTermsWithFilter(typeMirror, filter)

    (filteredTermSymbols collect {
      case sym if sym.isVar || sym.isVal =>
        (sym.getter.name.toString, instanceMirror.reflectField(sym).get)
      case sym if sym.isLazy =>
        (sym.getter.name.toString, instanceMirror.reflectMethod(sym.getter.asMethod).apply())
    }).toMap
  }

  def isAnnotatedWith(term: universe.TermSymbol, annotationType: universe.Type): Boolean =
    term.annotations.exists(_.tree.tpe.erasure <:< annotationType.erasure)
}
