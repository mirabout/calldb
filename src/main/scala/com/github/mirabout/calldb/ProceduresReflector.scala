package com.github.mirabout.calldb

class ProceduresReflector {

  import ProceduresReflector.DefinedProcedure

  def reflectAllProcedures(instance: AnyRef): IndexedSeq[DefinedProcedure[_]] = {
    import scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = runtimeMirror.reflect(instance)
    val typeMirror = instanceMirror.symbol.toType

    val termMembers = for (member <- typeMirror.members if member.isTerm) yield member.asTerm
    val callableMembers = termMembers.filter(_.typeSignature.erasure <:< universe.typeOf[DefinedProcedure[_]].erasure)
    val callableValsOrVars = callableMembers.filter(m => m.isVal || m.isVar)
    val callableLazyVals = callableMembers.filter(m => m.isLazy)

    val reflectedNamedProcedures = {
      (for (termSymbol <- callableValsOrVars)
        yield (termSymbol.getter.name.toString, instanceMirror.reflectField(termSymbol).get)) ++
      (for (termSymbol <- callableLazyVals)
        yield (termSymbol.getter.name.toString, instanceMirror.reflectMethod(termSymbol.getter.asMethod).apply()))
    }.asInstanceOf[Traversable[(String, DefinedProcedure[_])]]

    for ((name, procedure) <- reflectedNamedProcedures)
      procedure._nameAsMember = name

    reflectedNamedProcedures.map(_._2).toIndexedSeq
  }
}

object ProceduresReflector {
  type DefinedProcedure[R] = UntypedRoutine with TypedCallable[R]
}
