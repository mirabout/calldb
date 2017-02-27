package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection

import com.github.mirabout.calldb.ProceduresReflector.DefinedProcedure
import org.specs2.mutable.Specification

class TableProceduresCheckerTest extends Specification with ProcedureCheckSupport
  with ColumnReaders with ColumnWriters with ColumnTypeProviders {
  sequential

  private def newChecker()(implicit c: Connection) =
    new TableProceduresChecker(TableName("tDummy"))
  private def newTableProceduresChecker()(implicit c: Connection) =
    new TableProceduresChecker(TableName("tDummy"))
  private def newProcedureChecker(procedure: DefinedProcedure[_], dbDef: DbProcedureDef)(implicit c: Connection) =
    new ProcedureChecker(TableName("tDummy"), procedure, dbDef)
  private def newParamChecker(procedure: DefinedProcedure[_], dbDef: DbProcedureDef, param: Int)(implicit c: Connection) =
    new ProcedureParamTypeChecker(TableName("tDummy"), procedure, dbDef, param)
  private def newReturnTypeChecker(procedure: DefinedProcedure[_], dbDef: DbProcedureDef)(implicit c: Connection) =
    new ProcedureReturnTypeChecker(TableName("tDummy"), procedure, dbDef)

  private def fetchProceduresMap()(implicit c: Connection) = super.fetchDbProcedureDefs()

  private def fetchProcedureDef(name: String)(implicit c: Connection): DbProcedureDef =
    fetchProceduresMap().apply(name.toLowerCase)

  private def fetchCompoundType(oid: Int)(implicit c: Connection): DbCompoundType =
    newTableProceduresChecker().fetchUserDefinedCompoundTypes().apply(oid)

  private def fetchProcedureCompoundReturnType(procedureName: String)(implicit c: Connection): DbCompoundType =
    fetchCompoundType(fetchProcedureDef(procedureName).retType)

  private implicit class NamedProcedure[P <: UntypedRoutine](procedure: P) {
    def withMemberName(name: String): P = {
      procedure._nameAsMember = name; procedure
    }
  }

  private case class GeoPoint(latitude: Double, longitude: Double)
  private case class GeoPoint3D(latitude: Double, longitude: Double, altitude: Option[Double])

  private def traitsOf[A: ColumnTypeProvider]: BasicTypeTraits =
    implicitly[ColumnTypeProvider[A]].typeTraits.asOptBasic.get

  private def rowTypeProviderOf[A](traits: BasicTypeTraits*): TypeProvider[A] = new TypeProvider[A] {
    val typeTraits = RowTypeTraits(traits.toIndexedSeq)
  }

  private def dummyRowParserOf[A](columnsNames: String*): RowDataParser[A] = new RowDataParser[A](row => ???) {
    override def expectedColumnsNames: Option[IndexedSeq[String]] = Some(columnsNames.toIndexedSeq)
  }

  class WithTestEnvironment extends WithTestConnectionAndSqlExecuted("ProceduresCheckerTest")

  "ProceduresChecker" should {
    "run these examples sequentially" >> {

      "provide fetchDbProcedureDefs() object method" in new WithTestEnvironment {
        val defs: Map[String, DbProcedureDef] = fetchDbProcedureDefs()

        // create function pDummyTypeChecked1(arg0 integer) returns bigint
        defs.isDefinedAt("pDummyTypeChecked1".toLowerCase) must beTrue
        val def1 = defs("pDummyTypeChecked1".toLowerCase)
        def1.allArgTypes must_=== IndexedSeq()
        def1.argTypes must_=== IndexedSeq(PgType.Integer.pgNumericOid.get)
        def1.argModes must_=== IndexedSeq()
        def1.argNames must_=== IndexedSeq("arg0")
        def1.retType must_=== PgType.Bigint.pgNumericOid.get

        // create function pDummyTypeChecked2(entity GeoPoint) returns bigint
        defs.isDefinedAt("pDummyTypeChecked2".toLowerCase) must beTrue
        val def2 = defs("pDummyTypeChecked2".toLowerCase)
        def2.argModes must beEmpty
        def2.argNames must_=== IndexedSeq("entity")
        def2.retType must_=== PgType.Bigint.pgNumericOid.get

        // create function pDummyTypeChecked3(entities GeoPoint[]) returns bigint
        defs.isDefinedAt("pDummyTypeChecked3".toLowerCase) must beTrue
        val def3 = defs("pDummyTypeChecked3".toLowerCase)
        def3.allArgTypes must beEmpty
        def3.argTypes must not(beEmpty)
        def3.argModes must beEmpty
        def3.argNames must_=== IndexedSeq("entities")
        def3.retType must_=== PgType.Bigint.pgNumericOid.get

        // create function pDummyTypeChecked4(arg0 integer) returns table(latitude float8, longitude float8, altitude float8)
        defs.isDefinedAt("pDummyTypeChecked4".toLowerCase) must beTrue
        val def4 = defs("pDummyTypeChecked4".toLowerCase)
        def4.allArgTypes must_=== IndexedSeq(
          PgType.Integer, PgType.Double, PgType.Double, PgType.Double).flatMap(_.pgNumericOid)
        def4.argTypes must_=== IndexedSeq(PgType.Integer.pgNumericOid.get)
        def4.argModes must_=== IndexedSeq(DbArgMode.In, DbArgMode.Table, DbArgMode.Table, DbArgMode.Table)
        def4.argNames must_=== IndexedSeq("arg0", "latitude", "longitude", "altitude")

        // create function pDummyTypeChecked5(arg0 integer, arg1 integer) returns setof GeoPoint3D
        defs.isDefinedAt("pDummyTypeChecked5".toLowerCase) must beTrue
        val def5 = defs("pDummyTypeChecked5".toLowerCase)
        def5.allArgTypes must_=== IndexedSeq()
        def5.argTypes must_=== IndexedSeq(PgType.Integer, PgType.Integer).flatMap(_.pgNumericOid)
        def5.argModes must_=== IndexedSeq()
        def5.argNames must_=== IndexedSeq("arg0", "arg1")
      }

      "provide fetchUserDefinedCompoundTypes() object method" in new WithTestEnvironment {
        val fetchedTypes: Map[Int, DbCompoundType] = fetchUserDefinedCompoundTypes()
        fetchedTypes.values.map(_.name) must containAllOf(Seq("geopoint", "geopoint3d"))
      }

      "provide fetchCompoundTypeAttributes() object method" in new WithTestEnvironment {
        fetchCompoundTypeAttributes(Int.MaxValue) must beNone

        val userDefinedTypeByOid: Map[Int, DbCompoundType] =
          (for (t <- fetchUserDefinedCompoundTypes().values) yield (t.typeId, t)).toMap

        val proceduresMap = fetchProceduresMap()
        val dummyProc4Def = proceduresMap("pDummyTypeChecked4".toLowerCase)
        PgType.typeByOid(dummyProc4Def.retType) must beSome(PgType.Record)
        // Records are not user-defined types (however we still may check output columns using DbProcedureDef)
        userDefinedTypeByOid.get(dummyProc4Def.retType) must beNone

        val dummyProc5Def = proceduresMap("pDummyTypeChecked5".toLowerCase)
        // This procedure has user-defined output type
        PgType.typeByOid(dummyProc5Def.retType) must beNone
        userDefinedTypeByOid must beDefinedAt(dummyProc5Def.retType)

        val optProc5TypeAttribs = fetchCompoundTypeAttributes(dummyProc5Def.retType)
        optProc5TypeAttribs must beSome
        val Some(proc5TypeAttribs) = optProc5TypeAttribs

        proc5TypeAttribs must beDefinedAt(0, 1, 2)
        proc5TypeAttribs(0) must_=== DbAttributeDef("latitude", PgType.Double.pgNumericOid.get, 0)
        proc5TypeAttribs(1) must_=== DbAttributeDef("longitude", PgType.Double.pgNumericOid.get, 1)
        proc5TypeAttribs(2) must_=== DbAttributeDef("altitude", PgType.Double.pgNumericOid.get, 2)
      }

      "reject procedures that have nameAsMember that does not start with `p`" in new WithTestEnvironment {
        val dummyProcedure = Procedure0().withMemberName("dummy")
        val checker = newChecker()
        val expectedError = checker.errorProcedureNameAsMemberMustStartWithP("dummy")
        val checkResult = checker.checkProcedure(dummyProcedure, databaseProcedures = Map.empty[String, DbProcedureDef])
        checkResult.map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedures that can't be found by name in database" in new WithTestEnvironment {
        val dummyProcedure = Procedure0().withMemberName("pDummy")
        val checker = newChecker()
        val expectedError = checker.errorProcedureDoesNotHaveItsCounterpart("pDummy_Dummy")
        val checkResult = checker.checkProcedure(dummyProcedure, databaseProcedures = Map.empty[String, DbProcedureDef])
        checkResult.map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code return type is basic and DB return type is compound" in new WithTestEnvironment {
        val procedureName = "pDummyTypeChecked5"
        val procedure = Procedure2(Param("arg0", 0), Param("arg1", 0)).withMemberName(procedureName)
        val checker = newReturnTypeChecker(procedure, fetchProcedureDef(procedureName))
        val expectedError = checker.errorExpectedCompoundReturnType()
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code return type is compound and DB return type is basic" in new WithTestEnvironment {
        implicit val geoPointTypeProvider = rowTypeProviderOf[GeoPoint](traitsOf[Double], traitsOf[Double])
        val rowDataParser = dummyRowParserOf[GeoPoint]("latitude", "longitude")
        val functionName = "pDummyTypeChecked1"
        val function = Function0[GeoPoint](rowDataParser.!).withMemberName(functionName)
        val checker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val expectedError = checker.errorExpectedBasicReturnType()
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code and DB basic return types do not match" in new WithTestEnvironment {
        val functionName = "pDummyTypeChecked1"
        val function = Function0[String](RowDataParser.string(0).!).withMemberName(functionName)
        val returnTypeChecker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val expectedError = returnTypeChecker.errorCodeBasicReturnTypeDoesNotConform(PgType.Text)
        returnTypeChecker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "yield successful procedure return type check result if code and DB basic return types match" in new WithTestEnvironment {
        val procedure1Def = Procedure1(Param("arg0", 0))
        newReturnTypeChecker(procedure1Def, fetchProcedureDef("pDummyTypeChecked1")).result() must beNone
      }

      "reject procedure if DB return type is a record and columns names do not match code ones" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = rowTypeProviderOf[GeoPoint3D](
          traitsOf[Double], traitsOf[Double], traitsOf[Option[Double]])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("_latitude", "longitude", "altitude")
        val functionName = "pDummyTypeChecked4"
        val function = Function1(rowDataParser.*, Param("arg0", 0)).withMemberName(functionName)
        val returnTypeChecker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val expectedError1 = returnTypeChecker.errorColumnsArePresentOnlyInDB(Seq("latitude"))
        val expectedError2 = returnTypeChecker.errorColumnsArePresentOnlyInCode(Seq("_latitude"))
        returnTypeChecker.result().map(_.toSet) must_== Some(Set(expectedError1, expectedError2))
      }

      "reject procedure if DB return type is a record and column types for a named column do not match" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = rowTypeProviderOf[GeoPoint3D](
          traitsOf[Double], traitsOf[Double], traitsOf[Int])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "altitude")
        val functionName = "pDummyTypeChecked4"
        val function = Function1(rowDataParser.*, Param("arg0", 0)).withMemberName(functionName)
        val checker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val integerTypeOid = PgType.Integer.getOrFetchOid().get
        val doubleOid = PgType.Double.getOrFetchOid().get.exactOid
        val expectedError = checker.errorCodeColumnTypeOidDoesNotConform("altitude", integerTypeOid, doubleOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "accept procedure return type if DB record return type signature conforms to code one" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = rowTypeProviderOf[GeoPoint3D](
          traitsOf[Double], traitsOf[Double], traitsOf[Option[Double]])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "altitude")
        val function = Function1(rowDataParser.*, Param("arg0", 0))
        newReturnTypeChecker(function, fetchProcedureDef("pDummyTypeChecked4")).result() must beNone
      }

      "reject procedure if DB return type is a relation and column names do not match code ones" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = rowTypeProviderOf[GeoPoint3D](
          traitsOf[Double], traitsOf[Double], traitsOf[Option[Double]])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "_altitude")
        val functionName = "pDummyTypeChecked5"
        val function = Function2(rowDataParser.*, Param("arg0", 0), Param("arg1", 0)).withMemberName(functionName)
        val checker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val expectedError1 = checker.errorColumnsArePresentOnlyInDB(Seq("altitude"))
        val expectedError2 = checker.errorColumnsArePresentOnlyInCode(Seq("_altitude"))
        checker.result().map(_.toSet) must_== Some(Set(expectedError1, expectedError2))
      }

      "reject procedure if DB return type is a relation and column types for a named column do not match" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = rowTypeProviderOf[GeoPoint3D](traitsOf[Double], traitsOf[Double], traitsOf[String])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "altitude")
        val functionName = "pDummyTypeChecked5"
        val function = Function2(rowDataParser.*, Param("arg0", 0), Param("arg1", 0)).withMemberName(functionName)
        val checker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val textTypeOid = PgType.Text.getOrFetchOid().get
        val doubleOid = PgType.Double.getOrFetchOid().get.exactOid
        val expectedError = checker.errorCodeColumnTypeOidDoesNotConform("altitude", textTypeOid, doubleOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "accept procedure return type if DB relation return type signature conforms to code one" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = rowTypeProviderOf[GeoPoint3D](traitsOf[Double], traitsOf[Double], traitsOf[Option[Double]])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "altitude")
        val functionName = "pDummyTypeChecked5"
        val function = Function2(rowDataParser.*, Param("arg0", 0), Param("arg1", 0)).withMemberName(functionName)
        newReturnTypeChecker(function, fetchProcedureDef(functionName)).result must beNone
      }

      "reject procedure if code and DB parameters count does not match" in new WithTestEnvironment {
        val procedureName = "pDummyTypeChecked3"
        val procedure = Procedure0().withMemberName(procedureName)
        val checker = newProcedureChecker(procedure, fetchProcedureDef(procedureName))
        val expectedError = checker.errorDBArgsCountDoesNotMatchCodeOne()
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code parameter is of basic type and DB parameter is of compound type" in new WithTestEnvironment {
        val procedureName = "pDummyTypeChecked2"
        val procedure = Procedure1(Param("arg0", 0)).withMemberName(procedureName)
        val dbProcedureDef = fetchProcedureDef(procedureName)
        val checker = newParamChecker(procedure, fetchProcedureDef(procedureName), 0)
        val integerTypeOid = PgType.Integer.getOrFetchOid().get
        val expectedError = checker.errorCodeTypeOidDoesNotConform(integerTypeOid, dbProcedureDef.argTypes(0))
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code parameter is of compound type and DB parameter is of basic type" in new WithTestEnvironment {
        val column1 = TableColumn('latitude, 0.0)
        val column2 = TableColumn('longitude, 0.0)
        val entityParamsDef = EntityParamsDef(IndexedSeq(column1, column2), TableName("tDummy"))
        val procedureName = "pDummyTypeChecked1"
        val procedure = Procedure1(entityParamsDef).withMemberName(procedureName)
        val dbProcedureDef = fetchProcedureDef(procedureName)
        val checker = newParamChecker(procedure, dbProcedureDef, 0)
        val integerOid = PgType.Integer.getOrFetchOid().get.exactOid
        val expectedError = checker.errorCantGetCompoundTypeAttributes(integerOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code and DB parameters basic types do not match" in new WithTestEnvironment {
        val procedureName = "pDummyTypeChecked1"
        val procedure = Procedure1(Param("arg0", "")).withMemberName(procedureName)
        val checker = newParamChecker(procedure, fetchProcedureDef(procedureName), 0)
        val textTypeOid = PgType.Text.getOrFetchOid().get
        val integerOid = PgType.Integer.getOrFetchOid().get.exactOid
        val expectedError = checker.errorCodeTypeOidDoesNotConform(textTypeOid, integerOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code and DB parameters compound types have different columns count" in new WithTestEnvironment {
        val column1 = TableColumn('latitude, 0.0)
        val column2 = TableColumn('longitude, 0.0)
        val column3 = TableColumn('altitude, Some(0.0).asInstanceOf[Option[Double]])
        val entityParamsDef = EntityParamsDef(IndexedSeq(column1, column2, column3), TableName("tGeoPoint"))
        val procedureName = "pDummyTypeChecked2"
        val procedure = Procedure1(entityParamsDef).withMemberName(procedureName)
        val checker = newParamChecker(procedure, fetchProcedureDef(procedureName), 0)
        val expectedError = checker.errorAttributeDefsSizeDoesNotMatch(2, entityParamsDef.allColumns.size)
        checker.result().map(_.toSet) must_=== Some(Set(expectedError))
      }

      "reject procedure if code and DB parameters compound types have different columns types" in new WithTestEnvironment {
        val column1 = TableColumn('latitude, 0.toLong)
        // Should be Double
        val column2 = TableColumn('longitude, 0.toLong)
        // Should be Double
        val entityParamsDef = EntityParamsDef(IndexedSeq(column1, column2), TableName("tGeoPoint"))
        val procedureName = "pDummyTypeChecked2"
        val procedure = Procedure1(entityParamsDef).withMemberName(procedureName)
        val checker = newParamChecker(procedure, fetchProcedureDef(procedureName), 0)
        val longTypeOid = PgType.Bigint.getOrFetchOid().get
        val doubleOid = PgType.Double.getOrFetchOid().get.exactOid
        val expectedError1 = checker.errorCodeAttributeOidDoesNotMatch(0, longTypeOid, doubleOid)
        val expectedError2 = checker.errorCodeAttributeOidDoesNotMatch(1, longTypeOid, doubleOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError1, expectedError2))
      }

      "yield successful parameter check result if code and DB compound parameter columns match" in new WithTestEnvironment {
        val column1 = TableColumn('latitude, 0.0)
        val column2 = TableColumn('longitude, 0.0)
        val paramsDef = EntityParamsDef(IndexedSeq(column1, column2), TableName("tGeoPoint"))
        val procedureName = "pDummyTypeChecked3"
        val procedure = Procedure1(paramsDef).withMemberName(procedureName)
        val dbProcedureDef = fetchProcedureDef(procedureName)
        val checker = newParamChecker(procedure, dbProcedureDef, 0)
        checker.result() must beNone
      }
    }
  }
}
