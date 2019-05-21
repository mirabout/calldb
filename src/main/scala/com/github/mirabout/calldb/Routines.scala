package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.ResultSet

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

sealed trait TypedCallable[R] {
  import TypedCallable._

  def paramsDefs: IndexedSeq[ParamsDef[_]]
  def resultType: TypeTraits
  def resultColumnsNames: Option[IndexedSeq[String]]
}

object TypedCallable {
  trait ParamsDef[+A] extends ParamsEncoder[A] with TypeProvider[A]
}

import TypedCallable._

abstract class Procedure[R](parser : ResultSetParser[R],
                            traits : TypeTraits,
                            defs   : Seq[ParamsDef[_]],
                            ify    : InvocationFacility)
  extends TypedCallable[R] {
  val paramsDefs: IndexedSeq[ParamsDef[_]] = defs.toIndexedSeq
  def resultType: TypeTraits = traits
  def resultColumnsNames: Option[IndexedSeq[String]] = parser.expectedColumnsNames

  protected def invocationFacility: InvocationFacility = ify

  protected def parse(futureResultSet: Future[ResultSet]): Future[R] =
    futureResultSet.map(rs => parser.parse(rs))

  def withInvocationFacility(facility: InvocationFacility): Procedure[R]
}

class Procedure0[R] private(parser: ResultSetParser[R], typeTraits: TypeTraits, ify: InvocationFacility)
  extends Procedure[R](parser, typeTraits, Array[ParamsDef[_]](), ify) {

  def apply()(implicit c: Connection): Future[R] =
    parse(ify.invoke(this, IndexedSeq.empty))

  def mapWith[RR](mapper: R => RR): Procedure0[RR] =
    new Procedure0(parser.mapWith(mapper), typeTraits, ify)

  override def withInvocationFacility(facility: InvocationFacility): Procedure0[R] =
    new Procedure0(parser, typeTraits, facility)
}

object Procedure0 {
  def apply[R](resultSetParser: ResultSetParser[R])(implicit tp: TypeProvider[R]) =
    new Procedure0(resultSetParser, tp.typeTraits, ExistingProcedureInvocationFacility)

  def apply[R](resultSetParser: ResultSetParser[R], ify: InvocationFacility)(implicit tp: TypeProvider[R]) =
    new Procedure0(resultSetParser, tp.typeTraits, ify)

  def returningLong()(implicit tp: TypeProvider[Long]) =
    apply(RowDataParser.returns.long(0).single)
}

class Procedure1[R, T1] private
  (parser: ResultSetParser[R], typeTraits: TypeTraits, _1: ParamsDef[T1], ify: InvocationFacility)
    extends Procedure[R](parser, typeTraits, Array(_1), ify) {

  def apply(v1: T1)(implicit c: Connection): Future[R] =
    parse(ify.invoke(this, Array(_1.encodeParam(v1))))

  def mapWith[RR](mapper: R => RR): Procedure1[RR, T1] =
    new Procedure1(parser.mapWith(mapper), typeTraits, _1, ify)

  override def withInvocationFacility(facility: InvocationFacility): Procedure1[R, T1] =
    new Procedure1(parser, typeTraits, _1, facility)
}

object Procedure1 {
  def apply[R, T1](parser: ResultSetParser[R], _1: ParamsDef[T1])(implicit tp: TypeProvider[R]) =
    new Procedure1(parser, tp.typeTraits, _1, ExistingProcedureInvocationFacility)

  def apply[R, T1](parser: ResultSetParser[R], _1: ParamsDef[T1], ify: InvocationFacility)(implicit tp: TypeProvider[R]) =
    new Procedure1(parser, tp.typeTraits, _1, ify)

  def returningLong[R, T1](_1: ParamsDef[T1])(implicit tp: TypeProvider[Long]) =
    apply(RowDataParser.returns.long(0).single, _1)
}

class Procedure2[R, T1, T2] private
  (parser: ResultSetParser[R], typeTraits: TypeTraits, _1: ParamsDef[T1], _2: ParamsDef[T2], ify: InvocationFacility)
    extends Procedure[R](parser, typeTraits, Array(_1, _2), ify) {

  def apply(v1: T1, v2: T2)(implicit c: Connection): Future[R] =
    parse(ify.invoke(this, Array(_1.encodeParam(v1), _2.encodeParam(v2))))

  def mapWith[RR](mapper: R => RR): Procedure2[RR, T1, T2] =
    new Procedure2(parser.mapWith(mapper), typeTraits, _1, _2, ify)

  override def withInvocationFacility(facility: InvocationFacility): Procedure2[R, T1, T2] =
    new Procedure2(parser, typeTraits, _1, _2, facility)
}

object Procedure2 {
  def apply[R, T1, T2]
    (parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2])
      (implicit tp: TypeProvider[R]) =
        new Procedure2(parser, tp.typeTraits, _1, _2, ExistingProcedureInvocationFacility)

  def apply[R, T1, T2]
    (parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2], ify: InvocationFacility)
      (implicit tp: TypeProvider[R]) =
        new Procedure2(parser, tp.typeTraits, _1, _2, ify)

  def returningLong[R, T1, T2](_1: ParamsDef[T1], _2: ParamsDef[T2])(implicit tp: TypeProvider[Long]) =
    apply(RowDataParser.returns.long(0).single, _1, _2)
}

class Procedure3[R, T1, T2, T3] private
  (parser: ResultSetParser[R], typeTraits: TypeTraits, _1: ParamsDef[T1],
    _2: ParamsDef[T2], _3: ParamsDef[T3], ify: InvocationFacility)
      extends Procedure[R](parser, typeTraits, Array(_1, _2, _3), ify) {

  def apply(v1: T1, v2: T2, v3: T3)(implicit c: Connection): Future[R] =
    parse(ify.invoke(this, Array(_1.encodeParam(v1), _2.encodeParam(v2), _3.encodeParam(v3))))

  def mapWith[RR](mapper: R => RR): Procedure3[RR, T1, T2, T3] =
    new Procedure3(parser.mapWith(mapper), typeTraits, _1, _2, _3, ify)

  override def withInvocationFacility(facility: InvocationFacility): Procedure3[R, T1, T2, T3] =
    new Procedure3(parser, typeTraits, _1, _2, _3, facility)
}

object Procedure3 {
  def apply[R, T1, T2, T3](parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3])
    (implicit tp: TypeProvider[R]) =
      new Procedure3(parser, tp.typeTraits, _1, _2, _3, ExistingProcedureInvocationFacility)

  def apply[R, T1, T2, T3]
    (parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], ify: InvocationFacility)
      (implicit tp: TypeProvider[R]) =
        new Procedure3(parser, tp.typeTraits, _1, _2, _3, ify)

  def returningLong[T1, T2, T3](_1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3])(implicit tp: TypeProvider[Long]) =
    apply(RowDataParser.returns.long(0).single, _1, _2, _3)
}

class Procedure4[R, T1, T2, T3, T4] private
  (parser: ResultSetParser[R], typeTraits: TypeTraits, _1: ParamsDef[T1],
    _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4], ify: InvocationFacility)
      extends Procedure[R](parser, typeTraits, Array(_1, _2, _3, _4), ify) {

  def apply(v1: T1, v2: T2, v3: T3, v4: T4)(implicit c: Connection): Future[R] =
    parse(ify.invoke(this, Array(_1.encodeParam(v1), _2.encodeParam(v2), _3.encodeParam(v3), _4.encodeParam(v4))))

  def mapWith[RR](mapper: R => RR): Procedure4[RR, T1, T2, T3, T4] =
    new Procedure4(parser.mapWith(mapper), typeTraits, _1, _2, _3, _4, ify)

  override def withInvocationFacility(facility: InvocationFacility): Procedure4[R, T1, T2, T3, T4] =
    new Procedure4(parser, typeTraits, _1, _2, _3, _4, facility)
}

object Procedure4 {
  def apply[R, T1, T2, T3, T4]
    (parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4])
      (implicit tp: TypeProvider[R]) =
        new Procedure4(parser, tp.typeTraits, _1, _2, _3, _4, ExistingProcedureInvocationFacility)

  def apply[R, T1, T2, T3, T4]
    (parser: ResultSetParser[R], _1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4], ify: InvocationFacility)
      (implicit tp: TypeProvider[R]) =
        new Procedure4(parser, tp.typeTraits, _1, _2, _3, _4, ify)

  def returningLong[T1, T2, T3, T4]
    (_1: ParamsDef[T1], _2: ParamsDef[T2], _3: ParamsDef[T3], _4: ParamsDef[T4])
      (implicit tp: TypeProvider[Long]) =
          apply(RowDataParser.returns.long(0).single, _1, _2, _3, _4)
}

