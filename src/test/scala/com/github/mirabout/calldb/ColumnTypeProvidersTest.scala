package com.github.mirabout.calldb

import java.util.UUID
import org.specs2.mutable._
import org.specs2.specification.Scope

import TypeProvider.basicTypeTraitsOf

class ColumnTypeProvidersTest extends Specification {
  class WithProviders extends Scope with ColumnTypeProviders

  "ColumnTypeProviders" should {
    "provide an implicit Boolean column type provider" in new WithProviders {
      basicTypeTraitsOf[Boolean].storedInType must_=== PgType.Boolean
    }

    // TODO: Byte provider?

    "provide an implicit Short column type provider" in new WithProviders {
      basicTypeTraitsOf[Short].storedInType must_=== PgType.Smallint
    }

    // TODO: Char provider?

    "provide an implicit Int column type provider" in new WithProviders {
      basicTypeTraitsOf[Int].storedInType must_=== PgType.Integer
    }

    "provide an implicit Long column type provider" in new WithProviders {
      basicTypeTraitsOf[Long].storedInType must_=== PgType.Bigint
    }

    "provide an implicit Float column type provider" in new WithProviders {
      basicTypeTraitsOf[Float].storedInType must_=== PgType.Real
    }

    "provide an implicit Double column type provider" in new WithProviders {
      basicTypeTraitsOf[Double].storedInType must_=== PgType.Double
    }

    "provide an implicit String column type provider" in new WithProviders {
      basicTypeTraitsOf[String].storedInType must_=== PgType.Text
    }

    "provide an implicit UUID column type provider" in new WithProviders {
      basicTypeTraitsOf[UUID].storedInType must_=== PgType.Uuid
    }

    // TODO....

    "provide an implicit Option column type provider" in new WithProviders {
      basicTypeTraitsOf[Option[Long]].storedInType must_=== PgType.Bigint
      basicTypeTraitsOf[Option[Long]].isNullable must beTrue

      basicTypeTraitsOf[Option[Seq[Long]]].storedInType must_=== PgType.BigintArray
      basicTypeTraitsOf[Option[Seq[Long]]].isNullable must beTrue
    }

    "provide an implicit Traversable column type provider" in new WithProviders {
      basicTypeTraitsOf[Traversable[Long]].storedInType must_=== PgType.BigintArray
      basicTypeTraitsOf[Traversable[Long]].isNullable must beFalse

      basicTypeTraitsOf[Option[Traversable[Long]]].storedInType must_=== PgType.BigintArray
      basicTypeTraitsOf[Option[Traversable[Long]]].isNullable must beTrue
    }
  }
}
