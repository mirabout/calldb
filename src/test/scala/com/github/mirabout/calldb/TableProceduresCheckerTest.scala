package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection

import com.github.mirabout.calldb.TypeProvider.typeProviderOf

import org.specs2.mutable.Specification

class TableProceduresCheckerTest extends Specification with ProcedureCheckSupport
  with ColumnReaders with ColumnWriters with ColumnTypeProviders {
  sequential

  private def newChecker()(implicit c: Connection) =
    new TableProceduresChecker(TableName("tDummy"))
  private def newTableProceduresChecker()(implicit c: Connection) =
    new TableProceduresChecker(TableName("tDummy"))
  private def newProcedureChecker(procedure: Procedure[_], dbDef: DbProcedureDef)(implicit c: Connection) =
    new ProcedureChecker(TableName("tDummy"), procedure, dbDef)
  private def newParamChecker(procedure: Procedure[_], dbDef: DbProcedureDef, param: Int)(implicit c: Connection) =
    new ProcedureParamTypeChecker(TableName("tDummy"), procedure, dbDef, param)
  private def newReturnTypeChecker(procedure: Procedure[_], dbDef: DbProcedureDef)(implicit c: Connection) =
    new ProcedureReturnTypeChecker(TableName("tDummy"), procedure, dbDef)

  private def fetchProceduresMap()(implicit c: Connection) = super.fetchDbProcedureDefs()

  private def fetchProcedureDef(name: String)(implicit c: Connection): DbProcedureDef =
    fetchProceduresMap().apply(name.toLowerCase)

  private def fetchCompoundType(oid: Int)(implicit c: Connection): DbCompoundType =
    newTableProceduresChecker().fetchUserDefinedCompoundTypes().apply(oid)

  private def fetchProcedureCompoundReturnType(procedureName: String)(implicit c: Connection): DbCompoundType =
    fetchCompoundType(fetchProcedureDef(procedureName).retType)

  private implicit class NamedProcedure[P <: Procedure[_]](procedure: P) {
    def callAs(nameAsMember: String)(implicit tableName: TableName): P = {
      ExistingProcedureInvocationFacility.register(procedure, tableName.makeProcedureName(nameAsMember))
      procedure
    }
  }

  private case class GeoPoint(latitude: Double, longitude: Double)
  private case class GeoPoint3D(latitude: Double, longitude: Double, altitude: Option[Double])

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
        def1.argNames must_=== IndexedSeq("arg0_")
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
        def4.argNames must_=== IndexedSeq("arg0_", "latitude", "longitude", "altitude")

        // create function pDummyTypeChecked5(arg0 integer, arg1 integer) returns setof GeoPoint3D
        defs.isDefinedAt("pDummyTypeChecked5".toLowerCase) must beTrue
        val def5 = defs("pDummyTypeChecked5".toLowerCase)
        def5.allArgTypes must_=== IndexedSeq()
        def5.argTypes must_=== IndexedSeq(PgType.Integer, PgType.Integer).flatMap(_.pgNumericOid)
        def5.argModes must_=== IndexedSeq()
        def5.argNames must_=== IndexedSeq("arg0_", "arg1_")
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

      "reject procedures that can't be found by name in database" in new WithTestEnvironment {
        val dummyProcedure = Procedure0.returningLong().callAs("pDummy")(TableName("tDummy"))
        val checker = newChecker()
        val expectedError = ProcedureCheckError.NoDatabaseCounterpart("pdummy_dummy")
        val checkResult = checker.checkProcedure(dummyProcedure, dbOnes = Map.empty[String, DbProcedureDef])
        checkResult.map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code return type is basic and DB return type is compound" in new WithTestEnvironment {
        val procedureName = "pDummyTypeChecked5"
        implicit val tableName = TableName("tDummy")
        val procedure = Procedure2.returningLong(Param("arg0", 0), Param("arg1", 0)).callAs(procedureName)
        val checker = newReturnTypeChecker(procedure, fetchProcedureDef(procedureName))
        val expectedError = ReturnTypeCheckError.ExpectedBasicType(procedure.resultType)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code return type is compound and DB return type is basic" in new WithTestEnvironment {
        implicit val geoPointTypeProvider = TypeProvider.forRow[GeoPoint](typeProviderOf[Double], typeProviderOf[Double])
        val rowDataParser = dummyRowParserOf[GeoPoint]("latitude", "longitude")
        val functionName = "pDummyTypeChecked1"
        val function = Procedure0[GeoPoint](rowDataParser.!).callAs(functionName)(TableName("tDummy"))
        val checker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val expectedError = ReturnTypeCheckError.ExpectedCompoundType(function.resultType)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code and DB basic return types do not match" in new WithTestEnvironment {
        val functionName = "pDummyTypeChecked1"
        val function = Procedure0[String](RowDataParser.string(0).!).callAs(functionName)(TableName("tDummy"))
        val returnTypeChecker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val actualReturnType = PgType.Bigint.getOrFetchOid().get.exactOid
        val expectedError = ReturnTypeCheckError.ReturnTypeMismatch(PgType.Text, actualReturnType)
        returnTypeChecker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "yield successful procedure return type check result if code and DB basic return types match" in new WithTestEnvironment {
        val procedure1Def = Procedure1(RowDataParser.long(0).single, Param("arg0", 0))
        newReturnTypeChecker(procedure1Def, fetchProcedureDef("pDummyTypeChecked1")).result() must beNone
      }

      "reject procedure if DB return type is a record and columns names do not match code ones" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = TypeProvider.forRow[GeoPoint3D](
          typeProviderOf[Double], typeProviderOf[Double], typeProviderOf[Option[Double]])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("_latitude", "longitude", "altitude")
        val functionName = "pDummyTypeChecked4"
        val function = Procedure1(rowDataParser.seq, Param("arg0", 0)).callAs(functionName)(TableName("tDummy"))
        val returnTypeChecker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val expectedError1 = ReturnTypeCheckError.PresentOnlyInCode("_latitude")
        val expectedError2 = ReturnTypeCheckError.PresentOnlyInDB("latitude")
        returnTypeChecker.result().map(_.toSet) must_== Some(Set(expectedError1, expectedError2))
      }

      "reject procedure if DB return type is a record and column types for a named column do not match" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = TypeProvider.forRow[GeoPoint3D](
          typeProviderOf[Double], typeProviderOf[Double], typeProviderOf[Int])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "altitude")
        val functionName = "pDummyTypeChecked4"
        val function = Procedure1(rowDataParser.seq, Param("arg0", 0)).callAs(functionName)(TableName("tDummy"))
        val checker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val doubleOid = PgType.Double.getOrFetchOid().get.exactOid
        val expectedError = ReturnTypeCheckError.ReturnColumnTypeMismatch("altitude", PgType.Integer, doubleOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "accept procedure return type if DB record return type signature conforms to code one" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = TypeProvider.forRow[GeoPoint3D](
          typeProviderOf[Double], typeProviderOf[Double], typeProviderOf[Option[Double]])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("LaTiTuDe", "LoNgItUdE", "AlTiTuDe")
        val function = Procedure1(rowDataParser.seq, Param("arg0", 0))
        newReturnTypeChecker(function, fetchProcedureDef("pDummyTypeChecked4")).result() must beNone
      }

      "reject procedure if DB return type is a relation and column names do not match code ones" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = TypeProvider.forRow[GeoPoint3D](
          typeProviderOf[Double], typeProviderOf[Double], typeProviderOf[Option[Double]])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "_altitude")
        val functionName = "pDummyTypeChecked5"
        implicit val tableName = TableName("tDummy")
        val function = Procedure2(rowDataParser.seq, Param("arg0", 0), Param("arg1", 0)).callAs(functionName)
        val checker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val expectedError1 = ReturnTypeCheckError.PresentOnlyInCode("_altitude")
        val expectedError2 = ReturnTypeCheckError.PresentOnlyInDB("altitude")
        checker.result().map(_.toSet) must_== Some(Set(expectedError1, expectedError2))
      }

      "reject procedure if DB return type is a relation and column types for a named column do not match" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = TypeProvider.forRow[GeoPoint3D](
          typeProviderOf[Double], typeProviderOf[Double], typeProviderOf[String])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "altitude")
        val functionName = "pDummyTypeChecked5"
        implicit val tableName = TableName("tDummy")
        val function = Procedure2(rowDataParser.seq, Param("arg0", 0), Param("arg1", 0)).callAs(functionName)
        val checker = newReturnTypeChecker(function, fetchProcedureDef(functionName))
        val doubleOid = PgType.Double.getOrFetchOid().get.exactOid
        val expectedError = ReturnTypeCheckError.ReturnColumnTypeMismatch("altitude", PgType.Text, doubleOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "accept procedure return type if DB relation return type signature conforms to code one" in new WithTestEnvironment {
        implicit val geoPoint3DTypeProvider = TypeProvider.forRow[GeoPoint3D](
          typeProviderOf[Double], typeProviderOf[Double], typeProviderOf[Option[Double]])
        val rowDataParser = dummyRowParserOf[GeoPoint3D]("latitude", "longitude", "altitude")
        val functionName = "pDummyTypeChecked5"
        implicit val tableName = TableName("tDummy")
        val function = Procedure2(rowDataParser.seq, Param("arg0", 0), Param("arg1", 0)).callAs(functionName)
        newReturnTypeChecker(function, fetchProcedureDef(functionName)).result must beNone
      }

      "reject procedure if code and DB parameters count does not match" in new WithTestEnvironment {
        val procedureName = "pDummyTypeChecked3"
        val procedure = Procedure0.returningLong().callAs(procedureName)(TableName("tDummy"))
        val checker = newProcedureChecker(procedure, fetchProcedureDef(procedureName))
        checker.result().map(_.toSet) must_== Some(Set(ParamCheckError.ArgCountMismatch(0, dbSize = 1)))
      }

      "reject procedure if code parameter is of basic type and DB parameter is of compound type" in new WithTestEnvironment {
        val procedureName = "pDummyTypeChecked2"
        val procedure = Procedure1.returningLong(Param("arg0", 0)).callAs(procedureName)(TableName("tDummy"))
        val dbProcedureDef = fetchProcedureDef(procedureName)
        val checker = newParamChecker(procedure, fetchProcedureDef(procedureName), 0)
        val integerTypeOid = PgType.Integer.getOrFetchOid().get
        // TODO: Should tell the other type is compound explicitly
        val expectedError = ParamCheckError.BasicOidMismatch(integerTypeOid, dbProcedureDef.argTypes(0))
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code parameter is of compound type and DB parameter is of basic type" in new WithTestEnvironment {
        val column1 = TableColumn('latitude, 0.0)
        val column2 = TableColumn('longitude, 0.0)
        implicit val tableName = TableName("tDummy")
        val entityParamsDef = EntityParamsDef(IndexedSeq(column1, column2), tableName)
        val procedureName = "pDummyTypeChecked1"
        val procedure = Procedure1.returningLong(entityParamsDef).callAs(procedureName)
        val dbProcedureDef = fetchProcedureDef(procedureName)
        val checker = newParamChecker(procedure, dbProcedureDef, 0)
        val integerOid = PgType.Integer.getOrFetchOid().get.exactOid
        val expectedError = ParamCheckError.CantGetTypeAttrs(integerOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code and DB parameters basic types do not match" in new WithTestEnvironment {
        val procedureName = "pDummyTypeChecked1"
        val procedure = Procedure1.returningLong(Param("arg0", "")).callAs(procedureName)(TableName("tDummy"))
        val checker = newParamChecker(procedure, fetchProcedureDef(procedureName), 0)
        val textTypeOid = PgType.Text.getOrFetchOid().get
        val integerOid = PgType.Integer.getOrFetchOid().get.exactOid
        val expectedError = ParamCheckError.BasicOidMismatch(textTypeOid, integerOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError))
      }

      "reject procedure if code and DB parameters compound types have different columns count" in new WithTestEnvironment {
        val column1 = TableColumn('latitude, 0.0)
        val column2 = TableColumn('longitude, 0.0)
        val column3 = TableColumn('altitude, Some(0.0).asInstanceOf[Option[Double]])
        implicit val tableName = TableName("tGeoPoint")
        val entityParamsDef = EntityParamsDef(IndexedSeq(column1, column2, column3), tableName)
        val procedureName = "pDummyTypeChecked2"
        val procedure = Procedure1.returningLong(entityParamsDef).callAs(procedureName)
        val checker = newParamChecker(procedure, fetchProcedureDef(procedureName), 0)
        val expectedError = ParamCheckError.AttrSizeMismatch(2, codeTraitsSize = entityParamsDef.allColumns.size)
        checker.result().map(_.toSet) must_=== Some(Set(expectedError))
      }

      "reject procedure if code and DB parameters compound types have different columns types" in new WithTestEnvironment {
        val column1 = TableColumn('latitude, 0.toLong)
        // Should be Double
        val column2 = TableColumn('longitude, 0.toLong)
        // Should be Double
        implicit val tableName = TableName("tGeoPoint")
        val entityParamsDef = EntityParamsDef(IndexedSeq(column1, column2), tableName)
        val procedureName = "pDummyTypeChecked2"
        val procedure = Procedure1.returningLong(entityParamsDef).callAs(procedureName)
        val checker = newParamChecker(procedure, fetchProcedureDef(procedureName), 0)
        val longTypeOid = PgType.Bigint.getOrFetchOid().get
        val doubleOid = PgType.Double.getOrFetchOid().get.exactOid
        val expectedError1 = ParamCheckError.AttrOidMismatch(0, longTypeOid, doubleOid)
        val expectedError2 = ParamCheckError.AttrOidMismatch(1, longTypeOid, doubleOid)
        checker.result().map(_.toSet) must_== Some(Set(expectedError1, expectedError2))
      }

      "yield successful parameter check result if code and DB compound parameter columns match" in new WithTestEnvironment {
        val column1 = TableColumn('latitude, 0.0)
        val column2 = TableColumn('longitude, 0.0)
        implicit val tableName = TableName("tGeoPoint")
        val paramsDef = EntityParamsDef(IndexedSeq(column1, column2), tableName)
        val procedureName = "pDummyTypeChecked3"
        val procedure = Procedure1.returningLong(paramsDef).callAs(procedureName)
        val dbProcedureDef = fetchProcedureDef(procedureName)
        val checker = newParamChecker(procedure, dbProcedureDef, 0)
        checker.result() must beNone
      }
    }
  }
}
