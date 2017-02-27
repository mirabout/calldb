package com.github.mirabout.calldb

import org.specs2.mutable._

class ProceduresReflectorTest extends Specification {

  private object dummy extends ColumnReaders with ColumnWriters with ColumnTypeProviders {
    val Latitude = TableColumn('latitude, 0.0)
    val Longitude = TableColumn('longitude, 0.0)

    var pDummyProcedure0 = Procedure0()
    val pDummyProcedure2 = Procedure2(Latitude, Longitude)
    lazy val pDummyFunction0 = Function0(RowDataParser.string(0).*)
    val pDummyFunction2 = Function2(RowDataParser.double(0).*, Latitude, Longitude)

    val routinesSet: Set[ProceduresReflector.DefinedProcedure[_]] =
      Set(pDummyProcedure0, pDummyProcedure2, pDummyFunction0, pDummyFunction2)
  }

  "ProceduresReflector" should {
    "reflect all procedures" in {
      (new ProceduresReflector).reflectAllProcedures(dummy).toSet must_=== dummy.routinesSet
    }

    "inject reflected procedures member names" in {
      val reflectedRoutines: Set[ProceduresReflector.DefinedProcedure[_]] =
        (new ProceduresReflector).reflectAllProcedures(dummy).toSet
      val routinesNames = reflectedRoutines.map(_.nameAsMember)
      routinesNames must_=== Set("pDummyProcedure0", "pDummyProcedure2", "pDummyFunction0", "pDummyFunction2")
    }
  }
}
