package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.ResultSet

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

sealed abstract class UntypedRoutine {
  // It is intended to be set during database schema check
  var _nameInDatabase: String = _

  def nameInDatabase: String = {
    if (_nameInDatabase eq null) {
      throw new IllegalStateException(s"$this: _nameInDatabase has not been set before name access")
    }
    _nameInDatabase
  }

  // It is intended to be set during retrieval of list of all routines
  private[calldb] var _nameAsMember: String = _

  private[calldb] def nameAsMember: String = {
    if (_nameAsMember eq null) {
      throw new IllegalStateException(s"$this: _memberName has not been set before memberName access")
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
        throw new IllegalStateException(s"$this($args): Expected a rows list " +
          "in query result, got result $queryResult with no rows list at all")
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

sealed abstract class UntypedProcedure extends UntypedRoutine {
  protected def call(args: String*)(implicit c: Connection): Future[Long] = {
    callForResultSet(args :_*) map { resultSet =>
      if (resultSet.size != 1) {
        throw new IllegalStateException(s"Expected single-row result set, got ${resultSet.size}")
      }
      if (resultSet.head.size != 1) {
        throw new IllegalStateException(s"Expected single-row, single column " +
          " result set, got ${resultSet.head.size} columns in a single row")
      }
      resultSet.head.head.asInstanceOf[Long]
    }
  }
}

sealed trait TypedCallable[R] { self: UntypedRoutine =>
  import TypedCallable._

  def paramsDefs: IndexedSeq[ParamsDef[_]]
  def resultType: TypeTraits
  def resultColumnsNames: Option[IndexedSeq[String]]

  override def buildCallSql(args: String*): String = {
    val appender = new StringAppender()
    appender += (if (resultType.isBasic) "select " else "select * from ")
    appender += nameInDatabase
    appendSqlCallArgs(appender, args :_*)
    println(appender.result())
    appender.result()
  }
}

object TypedCallable {
  trait ParamsDef[+A] extends ParamsEncoder[A] with TypeProvider[A]
}

import TypedCallable._

/** For testing of sealed [[TypedCallable]] trait */
private[calldb] class DummyTypedCallable[T](name: String, val resultType: TypeTraits)
  extends UntypedRoutine with TypedCallable[T] {
  _nameInDatabase = name
  override def paramsDefs = IndexedSeq()
  override def resultColumnsNames: Option[IndexedSeq[String]] = None
}

abstract class UntypedFunction[R](parser: ResultSetParser[R]) extends UntypedRoutine {
  protected final def call(args: String*)(implicit c: Connection): Future[R] = {
    callForResultSet(args :_*) map { resultSet =>
      parser.parse(resultSet)
    }
  }
}

abstract class AbstractTypedProcedure[R]
  (parser: ResultSetParser[R], typeTraits: TypeTraits, defs: IndexedSeq[ParamsDef[_]])
    extends UntypedFunction[R](parser) with TypedCallable[R] {
  def paramsDefs: IndexedSeq[ParamsDef[_]] = defs
  def resultType: TypeTraits = typeTraits
  def resultColumnsNames: Option[IndexedSeq[String]] = parser.expectedColumnsNames
}

class Procedure0[R] private(parser: ResultSetParser[R], typeTraits: TypeTraits)
  extends AbstractTypedProcedure[R](parser, typeTraits, Array[ParamsDef[_]]()) {

  def apply()(implicit c: Connection): Future[R] =
    call()

  def mapWith[RR](mapper: R => RR): Procedure0[RR] =
    new Procedure0(parser.mapWith(mapper), typeTraits)
}

object Procedure0 {
  def apply[R](resultSetParser: ResultSetParser[R])(implicit tp: TypeProvider[R]) =
    new Procedure0(resultSetParser, tp.typeTraits)

  def returningLong()(implicit tp: TypeProvider[Long]) =
    apply(RowDataParser.returns.long(0).single)
}

class Procedure1[R, T1] private(parser: ResultSetParser[R], typeTraits: TypeTraits, _1: ParamsDef[T1])
  extends AbstractTypedProcedure[R](parser, typeTraits, Array(_1)) {

  def apply(v1: T1)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1))

  def mapWith[RR](mapper: R => RR): Procedure1[RR, T1] =
    new Procedure1(parser.mapWith(mapper), typeTraits, _1)
}

object Procedure1 {
  def apply[R, T1](parser: ResultSetParser[R], _1: ParamsDef[T1])(implicit tp: TypeProvider[R]) =
    new Procedure1(parser, tp.typeTraits, _1)

  def returningLong[R, T1](_1: ParamsDef[T1])(implicit tp: TypeProvider[Long]) =
    apply(RowDataParser.returns.long(0).single, _1)
}

class Procedure2[R, T1, T2] private
  (parser: ResultSetParser[R], typeTraits: TypeTraits, _1: ParamsDef[T1], _2: ParamsDef[T2])
    extends AbstractTypedProcedure[R](parser, typeTraits, Array(_1, _2)) {

  def apply(v1: T1, v2: T2)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1), _2.encodeParam(v2))

  def mapWith[RR](mapper: R => RR): Procedure2[RR, T1, T2] =
    new Procedure2(parser.mapWith(mapper), typeTraits, _1, _2)
}

object Procedure2 {
  def apply[R, T1, T2](parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2])(implicit tp: TypeProvider[R]) =
    new Procedure2(parser, tp.typeTraits, _1, _2)

  def returningLong[R, T1, T2](_1: ParamsDef[T1], _2: ParamsDef[T2])(implicit tp: TypeProvider[Long]) =
    apply(RowDataParser.returns.long(0).single, _1, _2)
}

class Procedure3[R, T1, T2, T3] private
  (parser: ResultSetParser[R], typeTraits: TypeTraits, _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3])
    extends AbstractTypedProcedure[R](parser, typeTraits, Array(_1, _2, _3)) {

  def apply(v1: T1, v2: T2, v3: T3)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1), _2.encodeParam(v2), _3.encodeParam(v3))

  def mapWith[RR](mapper: R => RR): Procedure3[RR, T1, T2, T3] =
    new Procedure3(parser.mapWith(mapper), typeTraits, _1, _2, _3)
}

object Procedure3 {
  def apply[R, T1, T2, T3](parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3])
    (implicit tp: TypeProvider[R]) =
      new Procedure3(parser, tp.typeTraits, _1, _2, _3)

  def returningLong[T1, T2, T3](_1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3])(implicit tp: TypeProvider[Long]) =
    apply(RowDataParser.returns.long(0).single, _1, _2, _3)
}

class Procedure4[R, T1, T2, T3, T4] private
  (parser: ResultSetParser[R], typeTraits: TypeTraits, _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4])
    extends AbstractTypedProcedure[R](parser, typeTraits, Array(_1, _2, _3, _4)) {

  def apply(v1: T1, v2: T2, v3: T3, v4: T4)(implicit c: Connection): Future[R] =
    call(_1.encodeParam(v1), _2.encodeParam(v2), _3.encodeParam(v3), _4.encodeParam(v4))

  def mapWith[RR](mapper: R => RR): Procedure4[RR, T1, T2, T3, T4] =
    new Procedure4(parser.mapWith(mapper), typeTraits, _1, _2, _3, _4)
}

object Procedure4 {
  def apply[R, T1, T2, T3, T4]
    (parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4])
      (implicit tp: TypeProvider[R]) =
        new Procedure4(parser, tp.typeTraits, _1, _2, _3, _4)

  def returningLong[T1, T2, T3, T4]
    (_1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4])
      (implicit tp: TypeProvider[Long]) =
          apply(RowDataParser.returns.long(0).single, _1, _2, _3, _4)
}

