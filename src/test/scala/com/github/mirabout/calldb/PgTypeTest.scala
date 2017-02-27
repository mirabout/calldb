package com.github.mirabout.calldb

import org.specs2.mutable._

class PgTypeTest extends Specification {

  "PgType" should {
    "provide fetchTypeOid() object method" in new WithTestConnection {
      // Just basic
      PgType.fetchTypeOid("BooL") must_=== Some(16)
      PgType.fetchTypeOid("Text[]") must_=== Some(1009)
      PgType.fetchTypeOid("foobar") must beNone
    }

    "provide fetchTypeName() object method" in new WithTestConnection {
      // Just basic
      PgType.fetchTypeName(16).map(_.toLowerCase) must_=== Some("bool")
      PgType.fetchTypeName(1009).map(_.toLowerCase) must_=== Some("text[]")
      PgType.fetchTypeName(Int.MaxValue) must beNone
    }

    "provide getOrFetchOid() method" in new WithTestConnection {
      PgType.Boolean.getOrFetchOid() must beSome
      PgType.Boolean.getOrFetchOid().get.pgNumericOid must_=== PgType.Boolean.pgNumericOid
      PgType.Hstore.getOrFetchOid() must beSome
    }

    "provide Void type" in new WithTestConnection {
      PgType.Void.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Void.sqlName) must_=== PgType.Void.pgNumericOid
    }

    "provide Boolean scalar and array types" in new WithTestConnection {
      PgType.Boolean.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Boolean.sqlName) must_=== PgType.Boolean.pgNumericOid
      PgType.BooleanArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.BooleanArray.sqlName) must_=== PgType.BooleanArray.pgNumericOid
    }

    "provide Bytea scalar and array types" in new WithTestConnection {
      PgType.Bytea.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Bytea.sqlName) must_=== PgType.Bytea.pgNumericOid
      PgType.ByteaArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.ByteaArray.sqlName) must_=== PgType.ByteaArray.pgNumericOid
    }

    "provide Bigint scalar and array types" in new WithTestConnection {
      PgType.Bigint.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Bigint.sqlName) must_=== PgType.Bigint.pgNumericOid
      PgType.BigintArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.BigintArray.sqlName) must_=== PgType.BigintArray.pgNumericOid
    }

    "provide Smallint scalar and array types" in new WithTestConnection {
      PgType.Smallint.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Smallint.sqlName) must_=== PgType.Smallint.pgNumericOid
      PgType.SmallintArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.SmallintArray.sqlName) must_=== PgType.SmallintArray.pgNumericOid
    }

    "provide Integer scalar and array types" in new WithTestConnection {
      PgType.Integer.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Integer.sqlName) must_=== PgType.Integer.pgNumericOid
      PgType.IntegerArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.IntegerArray.sqlName) must_=== PgType.IntegerArray.pgNumericOid
    }

    "provide Text scalar and array types" in new WithTestConnection {
      PgType.Text.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Text.sqlName) must_=== PgType.Text.pgNumericOid
      PgType.TextArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.TextArray.sqlName) must_=== PgType.TextArray.pgNumericOid
    }

    "provide Point scalar and array types" in new WithTestConnection {
      PgType.Point.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Point.sqlName) must_=== PgType.Point.pgNumericOid
      PgType.PointArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.PointArray.sqlName) must_=== PgType.PointArray.pgNumericOid
    }

    "provide Lseg scalar and array types" in new WithTestConnection {
      PgType.Lseg.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Lseg.sqlName) must_=== PgType.Lseg.pgNumericOid
      PgType.LsegArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.LsegArray.sqlName) must_=== PgType.LsegArray.pgNumericOid
    }

    "provide Path scalar and array types" in new WithTestConnection {
      PgType.Path.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Path.sqlName) must_=== PgType.Path.pgNumericOid
      PgType.PathArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.PathArray.sqlName) must_=== PgType.PathArray.pgNumericOid
    }

    "provide Box scalar and array types" in new WithTestConnection {
      PgType.Box.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Box.sqlName) must_=== PgType.Box.pgNumericOid
      PgType.BoxArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.BoxArray.sqlName) must_=== PgType.BoxArray.pgNumericOid
    }

    "provide Polygon scalar and array types" in new WithTestConnection {
      PgType.Polygon.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Polygon.sqlName) must_=== PgType.Polygon.pgNumericOid
      PgType.PolygonArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.PolygonArray.sqlName) must_=== PgType.PolygonArray.pgNumericOid
    }

    "provide Line scalar and array types" in new WithTestConnection {
      PgType.Line.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Line.sqlName) must_=== PgType.Line.pgNumericOid
      PgType.LineArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.LineArray.sqlName) must_=== PgType.LineArray.pgNumericOid
    }

    "provide Real scalar and array types" in new WithTestConnection {
      PgType.Real.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Real.sqlName) must_=== PgType.Real.pgNumericOid
      PgType.RealArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.RealArray.sqlName) must_=== PgType.RealArray.pgNumericOid
    }

    "provide Double scalar and array types" in new WithTestConnection {
      PgType.Double.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Double.sqlName) must_=== PgType.Double.pgNumericOid
      PgType.DoubleArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.DoubleArray.sqlName) must_=== PgType.DoubleArray.pgNumericOid
    }

    "provide Circle scalar and array types" in new WithTestConnection {
      PgType.Circle.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.Circle.sqlName) must_=== PgType.Circle.pgNumericOid
      PgType.CircleArray.oidDef.isBuiltin must beTrue
      PgType.fetchTypeOid(PgType.CircleArray.sqlName) must_=== PgType.CircleArray.pgNumericOid
    }
  }
}
