package com.github.mirabout.calldb.generators

import com.github.mirabout.calldb.TableColumn

import org.specs2.mutable._
import org.specs2.matcher.MatchResult

class RetrieveByColumnsProcedureGeneratorTest extends Specification {
  private[this] object TestTable extends GeneratorTestTable {
    case class Generator(override val simpleProcedureName: String, columns: TableColumn[_,_]*)
      extends RetrieveByColumnsGenerator {
      override lazy val tableName = TestTable.tableName
      override lazy val allColumns: Traversable[TableColumn[_,_]] = TestTable.allColumns
      override lazy val conditionColumns: Traversable[TableColumn[_,_]] = columns
    }
  }

  type TestEnvironment = WithRetrieveByColumnsGeneratorTestEnvironment

  "RetrieveByColumnsProcedureGenerator" should {
    "build procedures for retrieval by a single column" in new TestEnvironment {
      private val generator = TestTable.Generator("GetById", TestTable.Id)
      private val tableName = TestTable.tableName.exactName.toLowerCase()

      val expectedArgs = "id_ UUID".toLowerCase()
      generator.procedureArgs.toLowerCase() must_=== expectedArgs
      generator.queryCondition.toLowerCase() must_=== s"(($tableName.id = id_))".toLowerCase()

      testProcedure(generator, simpleProcedureName = "GetById", expectedArgs)
    }

    "build procedures for retrieval by two columns" in new TestEnvironment {
      private val generator = TestTable.Generator("GetByGeoPoint", TestTable.Latitude, TestTable.Longitude)
      private val tableName = TestTable.tableName.exactName.toLowerCase()

      val expectedArgs = "latitude_ float8, longitude_ float8".toLowerCase()
      generator.procedureArgs.toLowerCase() must_=== expectedArgs
      val expectedCondition = s"(($tableName.latitude = latitude_) and ($tableName.longitude = longitude_))"
      generator.queryCondition.toLowerCase() must_=== expectedCondition

      testProcedure(generator, simpleProcedureName = "GetByGeoPoint", expectedArgs)
    }

    "build procedures for retrieval by multiple columns" in new TestEnvironment {
      private val columns = Seq(TestTable.Tag, TestTable.Latitude, TestTable.Longitude)
      private val generator = TestTable.Generator("GetByTagAndGeoPoint", columns :_*)
      private val tableName = TestTable.tableName.exactName.toLowerCase()

      val expectedArgs = "tag_ text, latitude_ float8, longitude_ float8".toLowerCase()
      generator.procedureArgs.toLowerCase() must_=== expectedArgs
      val expectedCondition: String = {
        s"(($tableName.tag = tag_) and " +
        s"($tableName.latitude = latitude_) " +
        s"and ($tableName.longitude = longitude_))"
      }
      generator.queryCondition.toLowerCase() must_=== expectedCondition

      testProcedure(generator, simpleProcedureName = "GetByTagAndGeoPoint", expectedArgs)
    }
  }
}

class WithRetrieveByColumnsGeneratorTestEnvironment extends WithBoilerplateGeneratorSqlExecuted with SpecLike {
  def testProcedure(generator: StatementsGenerator, simpleProcedureName: String, args: String) : MatchResult[String] = {
    // Testing procedures requires creation of vDummy.
    // We cannot add it to Before/After SQL scripts because these scripts are shared
    // for generator tests and vDummy creation is tested itself in another test.
    val viewGenerator = GeneratorTestTable.generatorOfQualifiedViews
    // Create vDummy view
    executeSql(connection, viewGenerator.createSql.get)

    val actualCreateSql = generator.createSql.get
    executeSql(connection, actualCreateSql)
    val actualDropSql = generator.dropSql.get
    executeSql(connection, actualDropSql)

    // Drop vDummy view
    executeSql(connection, viewGenerator.dropSql.get)

    val procedureQualifier = s"p${GeneratorTestTable.tableName.withoutPrefix}"
    val expectedDropSql = s"drop function ${procedureQualifier}_$simpleProcedureName($args)"
    actualDropSql.toLowerCase() must_=== expectedDropSql.toLowerCase()
  }
}
