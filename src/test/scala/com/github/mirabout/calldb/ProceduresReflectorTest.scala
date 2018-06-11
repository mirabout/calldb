package com.github.mirabout.calldb

import java.util.UUID

import org.specs2.mutable._

class ProceduresReflectorTest extends Specification {

  import ProceduresReflector.{reflectAllProcedures, injectProcedureMemberNames}

  trait BaseTable1 extends ColumnReaders with ColumnWriters with ColumnTypeProviders {
    lazy val pBaseProcedure0 = Procedure0()
    lazy val pBaseProcedure2 = Procedure2(Param('first, 0), Param('second, 0.0))

    def routines: Set[ProceduresReflector.DefinedProcedure[_]] =
      Set(pBaseProcedure0, pBaseProcedure2)
  }

  trait BaseTable2 extends ColumnReaders with ColumnWriters with ColumnTypeProviders {
    lazy val pBaseFunction0 = Function0(RowDataParser.string(0).*)
    lazy val pBaseFunction2 = Function2(RowDataParser.double(0).*, Param('first, ""), Param('second, ""))

    def routines: Set[ProceduresReflector.DefinedProcedure[_]] =
      Set(pBaseFunction0, pBaseFunction2)
  }

  class DummyTable1 extends BaseTable1 with BaseTable2 {
    lazy val pDummyFunction1 = Function1(RowDataParser.stringKVMap(0).*, Param('id, UUID.randomUUID()))

    override lazy val routines: Set[ProceduresReflector.DefinedProcedure[_]] =
      super[BaseTable1].routines ++ super[BaseTable2].routines ++ Set(pDummyFunction1)
  }

  object DummyTable1 extends DummyTable1

  class DummyTable2 extends BaseTable1 with BaseTable2 {
    lazy val pDummyProcedure3 = Procedure3(
      Param('first, UUID.randomUUID()),
      Param('second, UUID.randomUUID()),
      Param('third, UUID.randomUUID()))

    override lazy val routines: Set[ProceduresReflector.DefinedProcedure[_]] =
      super[BaseTable1].routines ++ super[BaseTable2].routines ++ Set(pDummyProcedure3)
  }

  object DummyTable2 extends DummyTable2

  "ProceduresReflector" should {
    "reflect all procedures" in {
      // Unfortunately there's no way to check procedures equality besides names we do not want to test right now
      reflectAllProcedures(new DummyTable1 {}).toSet.size must_=== DummyTable1.routines.size
      reflectAllProcedures(new DummyTable2 {}).toSet.size must_=== DummyTable2.routines.size
    }

    "inject reflected procedures member names" in {
      val baseNamesSet = Set("pBaseProcedure0", "pBaseProcedure2", "pBaseFunction0", "pBaseFunction2")

      def testInjection(table: BaseTable1 with BaseTable2, addedToBaseNames: String*) = {
        val reflected = reflectAllProcedures(table)
        val expectedNames = baseNamesSet ++ addedToBaseNames
        injectProcedureMemberNames(reflected)
        reflected.values.map(_.nameAsMember).toSet must_=== expectedNames
      }

      testInjection(new DummyTable1, "pDummyFunction1")
      testInjection(new DummyTable2, "pDummyProcedure3")
    }
  }
}
