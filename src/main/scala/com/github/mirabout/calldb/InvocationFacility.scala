package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.ResultSet

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

abstract class InvocationFacility {
  def invoke(callable: TypedCallable[_], args: IndexedSeq[String])(implicit c: Connection): Future[ResultSet] = {
    c.sendQuery(buildCallSql(callable, args)) map { queryResult =>
      queryResult.rows match {
        case Some(resultSet) => resultSet
        case None => throw new IllegalStateException("The query result misses a result set")
      }
    }
  }

  protected def buildCallSql(callable: TypedCallable[_], args: IndexedSeq[String]): String = {
    val appender = new StringAppender()
    appender += s"select * from ${getCallableName(callable)}("

    var i = -1
    while ({i += 1; i < args.length}) {
      appender += args(i) += ','
    }

    // Chop last comma
    if (args.nonEmpty) {
      appender.chopLast()
    }

    appender += ')'
    appender.result()
  }

  protected def getCallableName(value: TypedCallable[_]): String

  def needsExistingProcedure: Boolean
}

class ExistingProcedureInvocationFacility extends InvocationFacility {
  private val proceduresMap = new mutable.AnyRefMap[TypedCallable[_], String]

  override def needsExistingProcedure: Boolean = true

  def register(callable: TypedCallable[_], name: String): Unit = {
    assert(callable.hashCode() == System.identityHashCode(callable))
    synchronized {
      for (formerName <- proceduresMap.put(callable, name)) {
        throw new IllegalStateException(s"The procedure $callable with " +
          s"name `$name` was already registered under `$formerName` name")
      }
    }
  }

  protected override def getCallableName(callable: TypedCallable[_]): String = {
    assert(callable.hashCode() == System.identityHashCode(callable))
    // We want a nicer error reporting rather than one that apply() provides.
    // Try to avoid unnecessary boxing as well.
    proceduresMap.getOrNull(callable) match {
      case null => throw new IllegalStateException(s"Failed to get a name for $callable. Has it been registered?")
      case name => name
    }
  }

  def findCallableName(callable: TypedCallable[_]): Option[String] = {
    assert(callable.hashCode() == System.identityHashCode(callable))
    proceduresMap.get(callable)
  }
}

object ExistingProcedureInvocationFacility extends ExistingProcedureInvocationFacility


