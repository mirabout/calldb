package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.ResultSet

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

sealed abstract class UntypedRoutine extends BugReporting {
  // It is intended to be set during database schema check
  var _nameInDatabase: String = _

  def nameInDatabase: String = {
    if (_nameInDatabase eq null) {
      BUG(s"$this: _databaseName has not been set before name access")
    }
    _nameInDatabase
  }

  // It is intended to be set during retrieval of list of all routines
  private[calldb] var _nameAsMember: String = _

  private[calldb] def nameAsMember: String = {
    if (_nameAsMember eq null) {
      BUG(s"$this: _memberName has not been set before memberName access")
    }
    _nameAsMember
  }

  private[calldb] def mkQualifiedName(tableNameWithoutPrefix: String): String =
    'p' + tableNameWithoutPrefix + "_" + nameAsMember.substring(1)

  private def performCall(args: String*)(implicit c: Connection): Future[QueryResult] =
    c.sendQuery(buildCallSql(args :_*))

  // Visible for testing
  private[calldb] def appendSqlCallArgs(output: StringAppender, args: String*): Unit = {
    output += '('
    if (args.nonEmpty) {
      args match {
        case indexedSeqArgs: IndexedSeq[String] =>
          appendSqlCallIndexedSeqArgs(output, indexedSeqArgs)
        case seqArgs =>
          appendSqlCallSeqArgs(output, seqArgs)
      }
    }
    output += ')'
  }

  private def appendSqlCallIndexedSeqArgs(output: StringAppender, args: IndexedSeq[String]): Unit = {
    var i = -1
    while ({i += 1; i < args.length}) {
      output += args(i)
      output += ','
    }
    // Chop last comma
    output.chopLast()
  }

  private def appendSqlCallSeqArgs(output: StringAppender, args: Seq[String]): Unit = {
    val iterator = args.iterator
    while (iterator.hasNext) {
      output += iterator.next()
      output += ','
    }
    // Chop last comma
    output.chopLast()
  }

  protected def buildCallSql(args: String*): String

  protected final def callForResultSet(args: String*)(implicit c: Connection): Future[ResultSet] = {
    performCall(args :_*) map { queryResult =>
      if (queryResult.rows.isEmpty) {
        BUG(s"$this($args): Expected some (zero or more) rows in query result, " +
            s"got result $queryResult with no rows")
      }
      queryResult.rows.get
    }
  }

  override def toString: String = {
    if (_nameInDatabase ne null)
      s"${_nameInDatabase}(...)"
    else if (_nameAsMember ne null)
      s"${_nameAsMember}(...)"
    else
      super.toString
  }
}

/** For testing of sealed [[UntypedRoutine]]*/
private[calldb] class DummyUntypedRoutine(name: String) extends UntypedRoutine {
  _nameInDatabase = name

  // Since it is used in testing only for routines that return result set of records,
  // build sql is valid only for compound types
  def buildCallSql(args: String*): String = {
    val appender = new StringAppender()
    appender += s"SELECT * FROM $name"
    appendSqlCallArgs(appender, args :_*)
    appender.result()
  }

  def accessPerformCallForResultSet(args: String*)(implicit c: Connection): Future[ResultSet] =
    super.callForResultSet(args :_*)
}

/**
  * This is a common super-type for all procedures.
  * Procedures are functions that return affected rows count.
  * Procedures return affected rows count as functions do
  * (using single row, single column result set) to keep stored procedures code uniform
  * Procedures/functions distinction is kept for historical and backward compatibility reasons
  * as this code was extracted from a large codebase that used its predecessor and keeps using the calldb library.
  */
sealed abstract class UntypedProcedure extends UntypedRoutine {
  protected def call(args: String*)(implicit c: Connection): Future[Long] = {
    callForResultSet(args :_*) map { resultSet =>
      if (resultSet.size != 1) {
        BUG(s"Expected single-row result set, got ${resultSet.size}")
      }
      if (resultSet.head.size != 1) {
        BUG(s"Expected single-row, single column result set, got ${resultSet.head.size} columns in a single row")
      }
      resultSet.head.head.asInstanceOf[Long]
    }
  }
}

sealed trait TypedCallable[R] { self: UntypedRoutine =>
  import TypedCallable._

  def paramsDefs: Seq[ParamsDef[_]]
  def resultType: TypeTraits
  def resultColumnsNames: Option[IndexedSeq[String]]

  override def buildCallSql(args: String*): String = {
    val appender = new StringAppender()
    if (resultType.isBasic) {
      appender += "SELECT "
    } else {
      appender += "SELECT * FROM "
    }
    appender += nameInDatabase
    appendSqlCallArgs(appender, args :_*)
    println(appender.result())
    appender.result()
  }
}

object TypedCallable {
  trait ParamsDef[A] extends ParamsEncoder[A] with TypeProvider[A]
}

import TypedCallable._

/** For testing of sealed [[TypedCallable]] trait */
private[calldb] class DummyTypedCallable[T](name: String, val resultType: TypeTraits)
  extends UntypedRoutine with TypedCallable[T] {
  _nameInDatabase = name
  override def paramsDefs = Seq()
  override def resultColumnsNames = None
}

sealed abstract class AbstractTypedProcedure[R](val paramsDefs: ParamsDef[_]*)(implicit tp: TypeProvider[R])
  extends UntypedProcedure with TypedCallable[R] {
  override def resultType: TypeTraits = tp.typeTraits
  override def resultColumnsNames = None
}

sealed abstract class GenProcedure0[R](mapper: Long => R)(implicit tp: TypeProvider[R])
  extends AbstractTypedProcedure {

  def apply()(implicit c: Connection): Future[R] = call() map mapper

  def mapWith[RR](newMapper: R => RR)(implicit tp: TypeProvider[RR]) =
    new GenProcedure0[RR](newMapper compose mapper) {}
}

// Dummy implicit parameter suppresses empty parameter case class warning
final case class Procedure0(implicit tp: TypeProvider[Long]) extends GenProcedure0[Long](identity)

sealed abstract class GenProcedure1[R, T1](_1: ParamsDef[T1], mapper: Long => R)(implicit tp: TypeProvider[R])
  extends AbstractTypedProcedure(_1) {

  def apply(v1: T1)(implicit c: Connection): Future[R] = call(_1.encodeParam(v1)) map mapper

  def mapWith[RR](newMapper: R => RR)(implicit tp: TypeProvider[RR]) =
    new GenProcedure1[RR, T1](_1, newMapper compose mapper) {}
}

final case class Procedure1[T1](_1: ParamsDef[T1])(implicit tp: TypeProvider[Long])
  extends GenProcedure1[Long, T1](_1, identity)

sealed abstract class GenProcedure2[R, T1, T2](_1: ParamsDef[T1], _2: ParamsDef[T2], mapper: Long => R)(implicit tp: TypeProvider[R])
  extends AbstractTypedProcedure(_1, _2) {

  def apply(v1: T1, v2: T2)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1), _2.encodeParam(v2)) map mapper

  def mapWith[RR](newMapper: R => RR)(implicit tp: TypeProvider[RR]) =
    new GenProcedure2[RR, T1, T2](_1, _2, newMapper compose mapper) {}
}

final case class Procedure2[T1, T2](_1: ParamsDef[T1], _2: ParamsDef[T2])(implicit tp: TypeProvider[Long])
  extends GenProcedure2[Long, T1, T2](_1, _2, identity)

sealed abstract class GenProcedure3[R, T1, T2, T3](_1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], mapper: Long => R)
    (implicit tp: TypeProvider[R])
  extends AbstractTypedProcedure(_1, _2, _3) {

  def apply(v1: T1, v2: T2, v3: T3)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1), _2.encodeParam(v2), _3.encodeParam(v3)) map mapper

  def mapWith[RR](newMapper: R => RR)(implicit tp: TypeProvider[RR]) =
    new GenProcedure3[RR, T1, T2, T3](_1, _2, _3, newMapper compose mapper) {}
}

final case class Procedure3[T1, T2, T3](_1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3])
    (implicit tp: TypeProvider[Long])
  extends GenProcedure3(_1, _2, _3, identity)

sealed abstract class GenProcedure4[R, T1, T2, T3, T4](_1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4], mapper: Long => R)
  (implicit tp: TypeProvider[R])
  extends AbstractTypedProcedure(_1, _2, _3, _4) {

  def apply(v1: T1, v2: T2, v3: T3, v4: T4)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1), _2.encodeParam(v2), _3.encodeParam(v3), _4.encodeParam(v4)) map mapper

  def mapWith[RR](newMapper: R => RR)(implicit tp: TypeProvider[RR]) =
    new GenProcedure4[RR, T1, T2, T3, T4](_1, _2, _3, _4, newMapper compose mapper) {}
}

final case class Procedure4[T1, T2, T3, T4](_1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4])
    (implicit tp: TypeProvider[Long])
  extends GenProcedure4(_1, _2, _3, _4, identity)

sealed abstract class UntypedFunction[R](parser: ResultSetParser[R]) extends UntypedRoutine {
  protected final def call(args: String*)(implicit c: Connection): Future[R] = {
    callForResultSet(args :_*) map { resultSet =>
      parser.parse(resultSet)
    }
  }
}

sealed abstract class AbstractTypedFunction[R](parser: ResultSetParser[R], val paramsDefs: ParamsDef[_]*)
    (implicit classTag: ClassTag[R], tp: TypeProvider[R])
  extends UntypedFunction[R](parser) with TypedCallable[R] {

  def resultType: TypeTraits = tp.typeTraits
  def resultColumnsNames: Option[IndexedSeq[String]] = parser.expectedColumnsNames
}

final case class Function0[R](parser: ResultSetParser[R])(implicit classTag: ClassTag[R], tp: TypeProvider[R])
  extends AbstractTypedFunction[R](parser) {

  def apply()(implicit c: Connection): Future[R] = call()

  def mapWith[RR](mapper: R => RR)(implicit ct: ClassTag[RR], tp: TypeProvider[RR]) = Function0(parser.mapWith(mapper))
}

final case class Function1[R, T1](parser: ResultSetParser[R], _1: ParamsDef[T1])
    (implicit classTag: ClassTag[R], tp: TypeProvider[R])
  extends AbstractTypedFunction[R](parser, _1) {

  def apply(v1: T1)(implicit c: Connection): Future[R] = call(_1.encodeParam(v1))

  def mapWith[RR](mapper: R => RR)(implicit ct: ClassTag[RR], tp: TypeProvider[RR]) =
    Function1(parser.mapWith(mapper), _1)
}

final case class Function2[R, T1, T2](parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2])
    (implicit classTag: ClassTag[R], tp: TypeProvider[R])
  extends AbstractTypedFunction[R](parser, _1, _2) {

  def apply(v1: T1, v2: T2)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1), _2.encodeParam(v2))

  def mapWith[RR](mapper: R => RR)(implicit ct: ClassTag[RR], tp: TypeProvider[RR]) =
    Function2(parser.mapWith(mapper), _1, _2)
}

final case class Function3[R, T1, T2, T3](parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3])
    (implicit classTag: ClassTag[R], tp: TypeProvider[R])
  extends AbstractTypedFunction[R](parser, _1, _2, _3) {

  def apply(v1: T1, v2: T2, v3: T3)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1), _2.encodeParam(v2), _3.encodeParam(v3))

  def mapWith[RR](mapper: R => RR)(implicit ct: ClassTag[RR], tp: TypeProvider[RR]) =
    Function3(parser.mapWith(mapper), _1, _2, _3)
}


