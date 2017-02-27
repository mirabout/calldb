package com.github.mirabout.calldb

import java.util.UUID
import org.specs2.mutable._
import org.specs2.specification.Scope

class ColumnTypeProvidersTest extends Specification {
  class WithProviders extends Scope with ColumnTypeProviders

  "ColumnTypeProviders" should {
    "provide an implicit Boolean column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[Boolean]].typeTraits.storedInType must_=== PgType.Boolean
    }

    // TODO: Byte provider?

    "provide an implicit Short column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[Short]].typeTraits.storedInType must_=== PgType.Smallint
    }

    // TODO: Char provider?

    "provide an implicit Int column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[Int]].typeTraits.storedInType must_=== PgType.Integer
    }

    "provide an implicit Long column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[Long]].typeTraits.storedInType must_=== PgType.Bigint
    }

    "provide an implicit Float column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[Float]].typeTraits.storedInType must_=== PgType.Real
    }

    "provide an implicit Double column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[Double]].typeTraits.storedInType must_=== PgType.Double
    }

    "provide an implicit String column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[String]].typeTraits.storedInType must_=== PgType.Text
    }

    "provide an implicit UUID column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[UUID]].typeTraits.storedInType must_=== PgType.Uuid
    }

    // TODO....

    "provide an implicit Option column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[Option[Long]]].typeTraits.storedInType must_=== PgType.Bigint
      implicitly[ColumnTypeProvider[Option[Long]]].typeTraits.isNullable must beTrue

      implicitly[ColumnTypeProvider[Option[Seq[Long]]]].typeTraits.storedInType must_=== PgType.BigintArray
      implicitly[ColumnTypeProvider[Option[Seq[Long]]]].typeTraits.isNullable must beTrue
    }

    "provide an implicit Traversable column type provider" in new WithProviders {
      implicitly[ColumnTypeProvider[Traversable[Long]]].typeTraits.storedInType must_=== PgType.BigintArray
      implicitly[ColumnTypeProvider[Traversable[Long]]].typeTraits.isNullable must beFalse

      implicitly[ColumnTypeProvider[Option[Traversable[Long]]]].typeTraits.storedInType must_=== PgType.BigintArray
      implicitly[ColumnTypeProvider[Option[Traversable[Long]]]].typeTraits.isNullable must beTrue
    }
  }
}
