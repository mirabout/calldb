package com.github.mirabout.calldb

import org.specs2.mutable._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import RowDataParser.returns

trait RoutineTestSupport extends ColumnReaders with ColumnWriters with ColumnTypeProviders {

  implicit class ExplicitlyNamedRoutine[Routine <: UntypedRoutine](routine: Routine) {
    def withName(name: String): Routine = {
      routine._nameInDatabase = name; routine
    }
  }

  def awaitResult[A](future: Future[A]): A = Await.result(future, 3.seconds)
  def awaitReady[A](future: Future[A]): Future[A] = Await.ready(future, 3.seconds)

  def int(name: String): TypedCallable.ParamsDef[Int] = Param(name, 0)
  def long(name: String): TypedCallable.ParamsDef[Long] = Param(name, 0L)
  def double(name: String): TypedCallable.ParamsDef[Double] = Param(name, 0.0)
  def str(name: String): TypedCallable.ParamsDef[String] = Param(name, "")

  def opt[A](paramsDef: TypedCallable.ParamsDef[A])(implicit tp: ColumnTypeProvider[Option[A]], w: ColumnWriter[Option[A]])
    : TypedCallable.ParamsDef[Option[A]] = Param(paramsDef.name, None.asInstanceOf[Option[A]])

  def seq[A](paramsDef: TypedCallable.ParamsDef[A])(implicit tp: ColumnTypeProvider[Seq[A]], w: ColumnWriter[Seq[A]])
    : TypedCallable.ParamsDef[Seq[A]] = Param(paramsDef.name, Seq().asInstanceOf[Seq[A]])

  class WithRoutinesTestDatabaseEnvironment extends WithTestConnectionAndSqlExecuted("RoutinesTest")
}

class UntypedRoutineTest extends Specification with RoutineTestSupport {

  "UntypedRoutine" should {
    "perform call for a result set" in new WithRoutinesTestDatabaseEnvironment {
      val routine = new DummyUntypedRoutine("DummyResultSetRoutine")
      // This routine returns some pairs of double values named "latitude" and "longitude"
      awaitResult(routine.accessPerformCallForResultSet()).columnNames must_=== IndexedSeq("latitude", "longitude")
    }
  }
}

class TypedCallableTest extends Specification with RoutineTestSupport {
  "TypedCallable" should {
    "build procedure call string for case when procedure result type is a scalar" in {
      val callable = new DummyTypedCallable[Int]("Dummy", implicitly[ColumnTypeProvider[Int]].typeTraits)
      callable.buildCallSql() must_== "select Dummy()"
    }

    "build procedure call string for case when procedure result type is a (compound) row" in {
      val intTypeProvider = implicitly[ColumnTypeProvider[Int]]
      val typeProvider = tuple2TypeProvider[Int, Int](intTypeProvider, intTypeProvider)
      val callable = new DummyTypedCallable[(Int, Int)]("Dummy", typeProvider.typeTraits)
      callable.buildCallSql() must_== "select * from Dummy()"
    }
  }
}

class Procedure0Test extends Specification with RoutineTestSupport {
  "Procedure0" should {
    "allow to be called with no args for a Long affected rows count" in new WithRoutinesTestDatabaseEnvironment {
      val procedure: Procedure0 = (new Procedure0).withName("DummyProcedure0")
      // This procedure does not do any actual work and returns a dummy value "0"
      awaitResult(procedure.apply()) must_=== 0L
    }
  }
}

class Procedure1Test extends Specification with RoutineTestSupport {
  "Procedure1" should {
    "allow to be called with 1 arg for a Long affected rows count" in new WithRoutinesTestDatabaseEnvironment {
      val procedure: Procedure1[String] = Procedure1(str("arg0")).withName("DummyProcedure1")
      // This procedure does not do any actual work and returns a dummy value "1"
      awaitResult(procedure("Hello, world!")) must_=== 1L
    }
  }
}

class Procedure2Test extends Specification with RoutineTestSupport {
  "Procedure2" should {
    "allow to be called with 2 args for a Long result" in new WithRoutinesTestDatabaseEnvironment {
      val procedure: Procedure2[Int, Int] = Procedure2(int("arg0"), int("arg1")).withName("DummyProcedure2")
      // This procedure does not do any actual work and returns a dummy value "2"
      awaitResult(procedure(0, 0)) must_=== 2L
    }
  }
}

class Procedure3Test extends Specification with RoutineTestSupport {
  "Procedure3" should {
    "allow to be called with 3 args for a Long result" in new WithRoutinesTestDatabaseEnvironment {
      val procedure: Procedure3[Int, Int, Int] =
        Procedure3(int("arg0"), int("arg1"), int("arg2")).withName("DummyProcedure3")
      // This procedure does not do any actual work and returns a dummy value "3"
      awaitResult(procedure.apply(0, 0, 0)) must_=== 3L
    }
  }
}

class Procedure4Test extends Specification with RoutineTestSupport {
  "Procedure4" should {
    "allow to be called with 4 args for a Long result" in new WithRoutinesTestDatabaseEnvironment {
      val procedure = Procedure4(int("arg0"), int("arg1"), int("arg2"), int("arg3")).withName("DummyProcedure4")
      // This procedure does not do any actual work and returns a dummy value "4"
      awaitResult(procedure.apply(0, 0, 0, 0)) must_=== 4L
    }
  }
}

class Function0Test extends Specification with RoutineTestSupport {
  "Function0" should {
    "allow to be called with no args for a parsed result set" in new WithRoutinesTestDatabaseEnvironment {
      val function: Function0[IndexedSeq[Int]] = Function0(returns.int(index = 0).seq).withName("DummyFunction0")
      val integerOid = PgType.Integer.getOrFetchOid().get.exactOid
      // This function returns OIDs of all registered PostgreSQL types
      awaitResult(function()).toSet must contain(integerOid)
    }
  }
}

class Function1Test extends Specification with RoutineTestSupport {
  "Function1" should {
    "allow to be called with 1 arg for a parsed result set" in new WithTestConnectionAndSqlExecuted("RoutinesTest") {
      // This function returns length of a string as a long value
      val function: Function1[Long, String] =
        Function1(returns.long(index = 0).single, str("arg0")).withName("DummyFunction1")
      awaitResult(function.apply("Hello, world!")) must_=== 13L
    }
  }
}

class Function2Test extends Specification with RoutineTestSupport {
  "Function2" should {
    "allow to be called with 2 args for a parsed result set" in new WithTestConnectionAndSqlExecuted("RoutinesTest") {
      // This function multiplies two given Double's and returns result as a Double
      val function: Function2[Double, Double, Double] =
        Function2(returns.double(index = 0).single, double("arg0"), double("arg1")).withName("DummyFunction2")
      awaitResult(function.apply(6.0, 8.0)) must_=== 48.0
    }
  }
}

class Function3Test extends Specification with RoutineTestSupport {
  "Function3" should {
    "allow to be called with 3 args for a parsed result set" in new WithTestConnectionAndSqlExecuted("RoutinesTest") {
      // This function clamps arg0 using [arg1, arg2] bounds and returns result as a Double
      val function: Function3[Double, Double, Double, Double] =
        Function3(returns.double(index = 0).single, double("arg0"), double("arg1"), double("arg2"))
          .withName("DummyFunction3")

      awaitResult(function.apply(1.0, 2.0, 3.0)) must_=== 2.0
      awaitResult(function.apply(2.5, 2.0, 3.0)) must_=== 2.5
      awaitResult(function.apply(3.5, 2.0, 3.0)) must_=== 3.0
    }
  }
}

