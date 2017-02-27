package com.github.mirabout.calldb

class TableColumnsReflector {

  import scala.reflect.runtime.universe.TermSymbol
  import scala.reflect.runtime.{universe => runtimeUniverse}

  def reflectAllColumns[E](instance: AnyRef): Set[TableColumn[E, _]] =
    reflectColumns(instance, !_.annotations.exists(_.tree.tpe =:= runtimeUniverse.typeOf[ignoredColumn]))

  def reflectKeyColumns[E](instance: AnyRef): Set[TableColumn[E, _]] =
    reflectColumns(instance, _.annotations.exists(_.tree.tpe =:= runtimeUniverse.typeOf[keyColumn]))

  private def reflectColumns[E](instance: AnyRef, columnFilter: TermSymbol => Boolean): Set[TableColumn[E, _]] = {
    import scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = runtimeMirror.reflect(instance)
    val typeMirror = instanceMirror.symbol.toType

    val termSymbols = for (m <- typeMirror.members if m.isTerm) yield m.asTerm
    val columnTermSymbols = termSymbols.view.filter(_.typeSignature.erasure =:= universe.typeOf[TableColumn[_,_]].erasure)
    val filteredTermSymbols = columnTermSymbols.filter(columnFilter)

    val columnValsOrVars = filteredTermSymbols.filter(sym => sym.isVal || sym.isVar)
    val columnLazyVals = filteredTermSymbols.filter(_.isLazy)

    val reflectedValues = {
      (for (sym <- columnValsOrVars) yield instanceMirror.reflectField(sym).get) ++
      (for (sym <- columnLazyVals) yield instanceMirror.reflectMethod(sym.getter.asMethod).apply())
    }

    reflectedValues.toSet.asInstanceOf[Set[TableColumn[E, _]]]
  }
}
