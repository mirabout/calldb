package com.github.mirabout.calldb

import scala.reflect.runtime.universe

class TableColumnsReflector extends ReflectionHelpers {
  private def isAColumn(term: universe.TermSymbol) =
    doesVarLikeTermMatchType(term, universe.typeOf[TableColumn[_,_]])

  def reflectAllColumns[E](instance: AnyRef): Set[TableColumn[E, _]] =
    reflectFieldsWithFilter(instance, term => isAColumn(term) && !isAnnotatedWith(term, universe.typeOf[ignoredColumn])).
      values.toSet.asInstanceOf[Set[TableColumn[E, _]]]

  def reflectKeyColumns[E](instance: AnyRef): Set[TableColumn[E, _]] =
    reflectFieldsWithFilter(instance, term => isAColumn(term) && isAnnotatedWith(term, universe.typeOf[keyColumn])).
      values.toSet.asInstanceOf[Set[TableColumn[E, _]]]
}

object TableColumnsReflector extends TableColumnsReflector
