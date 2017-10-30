package com.github.mirabout.calldb

import java.util.UUID

import org.specs2.matcher.MatchResult
import org.specs2.mutable._

case class GeneratorTestEntity(id: UUID, tag: Option[String], latitude: Double, longitude: Double)

class GeneratorTestTable extends GenericEntityTable[GeneratorTestEntity] with GenericEntityTableBoilerplateSqlGenerator {
  override lazy val tableName = TableName("tDummy")
  override lazy val entityParser: RowDataParser[GeneratorTestEntity] = ???

  lazy val Id = column('id, _.id)
  lazy val Tag = column('tag, _.tag)
  lazy val Latitude = column('latitude, _.latitude)
  lazy val Longitude = column('longitude, _.longitude)

  // Do not rely on injection via reflection since it is not performed in this case
  override lazy val allColumns = IndexedSeq(Id, Tag, Latitude, Longitude)
  override lazy val keyColumns = IndexedSeq(Id)
}

private[this] object GeneratorTestTable extends GeneratorTestTable

class GenericEntityTableBoilerplateSqlGeneratorTest extends Specification {

  "GenericEntityTableBoilerplateSqlGenerator" should {

    "provide procedures and views statement generator groups" in {
      val generatorGroupsMap = GeneratorTestTable.boilerplateGeneratorGroups().toMap
      generatorGroupsMap should haveKeys("procedures", "views")
      generatorGroupsMap("procedures") must not(beEmpty)
      generatorGroupsMap("views") must not(beEmpty)
    }

    "provide fully qualified column names view generator" in new WithBoilerplateGeneratorTestEnvironment {
      val generatorGroupsMap = GeneratorTestTable.boilerplateGeneratorGroups().toMap
      val optViewGenerator = generatorGroupsMap("views").toMap.get("fully qualified fields view")
      optViewGenerator must beSome
      val viewGenerator = optViewGenerator.get

      // CBA to write all Some matchers
      val createSql = viewGenerator.createSql.get.toLowerCase()
      println(createSql)
      val dropSql = viewGenerator.dropSql.get.toLowerCase()

      executeSql(connection, createSql)
      executeSql(connection, dropSql)

      // This is the only test set for CREATE OR REPLACE, IF EXISTS variants.
      // Generating ones is trivial though.
      val createOrReplaceSql = viewGenerator.createOrReplaceSql.get.toLowerCase()
      createSql.replace("create", "create or replace") must_=== createOrReplaceSql

      val dropIfExistsSql = viewGenerator.dropIfExistsSql.get.toLowerCase()
      dropIfExistsSql.replace("if exists ", "") must_=== dropSql

      dropSql must_=== "drop view vDummy".toLowerCase()
    }

    "provide insert procedure generator" in new WithBoilerplateGeneratorTestEnvironment {
      testProcedure("insert procedure", "pDummy_Insert(entity tDummy)")
    }

    "provide update procedure generator" in new WithBoilerplateGeneratorTestEnvironment {
      testProcedure("update procedure", "pDummy_Update(entity tDummy)")
    }

    "provide insert many procedure generator" in new WithBoilerplateGeneratorTestEnvironment {
      testProcedure("insert many procedure", "pDummy_InsertMany(entities tDummy[])")
    }

    "provide update many procedure generator" in new WithBoilerplateGeneratorTestEnvironment {
      testProcedure("update many procedure", "pDummy_UpdateMany(entities tDummy[])")
    }

    "provide insert or ignore procedure generator" in new WithBoilerplateGeneratorTestEnvironment {
      testProcedure("insert or ignore procedure", "pDummy_InsertOrIgnore(entity tDummy)")
    }

    "provide insert or update procedure generator" in new WithBoilerplateGeneratorTestEnvironment {
      testProcedure("insert or update procedure", "pDummy_InsertOrUpdate(entity tDummy)")
    }

    "provide insert or ignore many procedure generator" in new WithBoilerplateGeneratorTestEnvironment {
      testProcedure("insert or ignore many procedure", "pDummy_InsertOrIgnoreMany(entities tDummy[])")
    }

    "provide insert or update many procedure generator" in new WithBoilerplateGeneratorTestEnvironment {
      testProcedure("insert or update many procedure", "pDummy_InsertOrUpdateMany(entities tDummy[])")
    }

    "provide qualify columns procedure" in new WithBoilerplateGeneratorTestEnvironment {
      private val generatorGroupsMap = GeneratorTestTable.boilerplateGeneratorGroups().toMap
      private val viewGenerator = generatorGroupsMap("views").toMap.apply("fully qualified fields view")
      executeSql(connection, viewGenerator.createSql.get)
      testProcedure("qualify columns procedure", "pQualifyColumns(t tDummy)")
      executeSql(connection, viewGenerator.dropSql.get)
    }
  }
}

class WithBoilerplateGeneratorSqlExecuted extends WithTestConnectionAndSqlExecuted("BoilerplateGeneratorTest")

class WithBoilerplateGeneratorTestEnvironment extends WithBoilerplateGeneratorSqlExecuted with SpecLike {

  private def proceduresGroupMap() = GeneratorTestTable.boilerplateGeneratorGroups().toMap.apply("procedures").toMap

  def testProcedure(key: String, signature: String): MatchResult[String] = {
    val generator = proceduresGroupMap().apply(key)
    val actualCreateSql = generator.createSql.get
    executeSql(connection, actualCreateSql)
    val expectedDropSql = s"drop function $signature"
    val actualDropSql = generator.dropSql.get
    executeSql(connection, actualDropSql)
    actualDropSql.toLowerCase() must_=== expectedDropSql.toLowerCase()
  }
}
