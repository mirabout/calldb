package com.github.mirabout.calldb

import com.github.mauricio.async.db.Connection

import java.util.UUID
import org.joda.time.{DateTime, LocalDateTime, LocalTime, LocalDate}

import scala.concurrent.duration._
import scala.concurrent.Await

sealed abstract class TypeOidDef {
  private[calldb] def pgNumericOid: Option[Int]
  def asResolvedOid: Option[TypeOid]
  def isBuiltin: Boolean
  def isCustom: Boolean = !isBuiltin
}

class BuiltinOidDef(staticOid: Int, assignableFrom: => Set[Int], assignableTo: => Set[Int]) extends TypeOidDef {
  private[calldb] def pgNumericOid = Some(staticOid)
  def asResolvedOid: Some[TypeOid] = Some(new TypeOid(staticOid, assignableFrom, assignableTo))
  def isBuiltin = true
}

object BuiltinOidDef {
  def apply(staticOid: Int, assignableFrom: => Set[Int] = Set(), assignableTo: => Set[Int] = Set()) =
    new BuiltinOidDef(staticOid, assignableFrom, assignableTo)
}

class CustomOidDef(assignableFrom: => Set[Int], assignableTo: => Set[Int]) extends TypeOidDef {
  private[calldb] def pgNumericOid = None
  def asResolvedOid: None.type = None
  def isBuiltin = false
  def toResolvedOid(exactResolvedOid: Int) = new TypeOid(exactResolvedOid, assignableFrom, assignableTo)
}

object CustomOidDef {
  def apply(assignableFrom: => Set[Int], assignableTo: => Set[Int]) = new CustomOidDef(assignableFrom, assignableTo)
}

class TypeOid(private[calldb] val exactOid: Int, assignableFrom: Set[Int], assignableTo: Set[Int]) {
  private[calldb] def pgNumericOid: Option[Int] = Some(exactOid)
  def isAssignableFrom(thatOid: Int): Boolean = exactOid == thatOid || assignableFrom.contains(thatOid)
  def isAssignableTo(thatOid: Int): Boolean = exactOid == thatOid || assignableTo.contains(thatOid)

  def conforms(thatOid: Int): Boolean = isAssignableFrom(thatOid) && isAssignableTo(thatOid)

  override def toString = s"TypeOid($exactOid,assignableFrom=$assignableFrom,assignableTo=$assignableTo)"
}

sealed abstract class PgType(val oidDef: TypeOidDef, arrayTypeProvider: => PgArray, val resultSetClass: Class[_], val sqlName: String) {
  private[this] var _arrayType: PgArray = null
  def arrayType: PgArray = {
    if (_arrayType == null) {
      _arrayType = arrayTypeProvider
    }
    _arrayType
  }

  def getOrFetchOid()(implicit c: Connection): Option[TypeOid] = oidDef match {
    case builtin: BuiltinOidDef =>
      builtin.asResolvedOid
    case custom: CustomOidDef =>
      for (oid <- PgType.fetchTypeOid(sqlName))
        yield custom.toResolvedOid(exactResolvedOid = oid)
  }

  private[calldb] def pgNumericOid = oidDef.pgNumericOid
}

sealed abstract class PgArray(oidDef: TypeOidDef, val elemType: PgType) extends PgType(
    oidDef            = oidDef,
    arrayTypeProvider = throw new AssertionError(s"Array type (oid=$oidDef) does not have array type for itself"),
    resultSetClass    = classOf[IndexedSeq[_]],
    sqlName           = elemType.sqlName + "[]")

object PgType {
  // Return PgArray and not Nothing to avoid call by value in initializer since Nothing conforms to :=> PgArray
  private def failOnVoidArrayGetter: PgArray =
    throw new IllegalStateException("Attempt to access an array type provider for Void type")

  case object Void extends PgType(BuiltinOidDef(2278), failOnVoidArrayGetter, classOf[Unit], "VOID")

  case object Boolean extends PgType(BuiltinOidDef(16), BooleanArray, classOf[Boolean], "BOOL")
  case object BooleanArray extends PgArray(BuiltinOidDef(1000), Boolean)

  case object Bytea extends PgType(BuiltinOidDef(17), ByteaArray, classOf[Array[Byte]], "BYTEA")
  case object ByteaArray extends PgArray(BuiltinOidDef(1001), Bytea)

  case object Char extends PgType(BuiltinOidDef(18), CharArray, classOf[Char], "CHARACTER(1)")
  case object CharArray extends PgArray(BuiltinOidDef(1002), Char)

  case object Bigint extends PgType(BuiltinOidDef(20), BigintArray, classOf[Long], "INT8")
  case object BigintArray extends PgArray(BuiltinOidDef(1016), Bigint)

  case object Smallint extends PgType(BuiltinOidDef(21), SmallintArray, classOf[Short], "INT2")
  case object SmallintArray extends PgArray(BuiltinOidDef(1005), Smallint)

  case object Integer extends PgType(BuiltinOidDef(23), IntegerArray, classOf[Integer], "INT4")
  case object IntegerArray extends PgArray(BuiltinOidDef(1007), Integer)

  private[this] val textOidDef = BuiltinOidDef(
    25, assignableFrom = Set(Varchar.pgNumericOid.get), assignableTo = Set(Varchar.pgNumericOid.get))
  private[this] val textArrayOidDef = BuiltinOidDef(
    1009, assignableFrom = Set(VarcharArray.pgNumericOid.get), assignableTo = Set(VarcharArray.pgNumericOid.get))

  case object Text extends PgType(textOidDef, TextArray, classOf[String], "TEXT")
  case object TextArray extends PgArray(textArrayOidDef, Text)

  case object Point extends PgType(BuiltinOidDef(600), PointArray, classOf[String], "POINT")
  case object PointArray extends PgArray(BuiltinOidDef(1017), Point)

  case object Lseg extends PgType(BuiltinOidDef(601), LsegArray, classOf[String], "LSEG")
  case object LsegArray extends PgArray(BuiltinOidDef(1018), Lseg)

  case object Path extends PgType(BuiltinOidDef(602), PathArray, classOf[String], "PATH")
  case object PathArray extends PgArray(BuiltinOidDef(1019), Path)

  case object Box extends PgType(BuiltinOidDef(603), BoxArray, classOf[String], "BOX")
  case object BoxArray extends PgArray(BuiltinOidDef(1020), Box)

  case object Polygon extends PgType(BuiltinOidDef(604), PolygonArray, classOf[String], "POLYGON")
  case object PolygonArray extends PgArray(BuiltinOidDef(1027), Polygon)

  case object Line extends PgType(BuiltinOidDef(628), LineArray, classOf[String], "LINE")
  case object LineArray extends PgArray(BuiltinOidDef(629), Line)

  case object Real extends PgType(BuiltinOidDef(700), RealArray, classOf[Float], "FLOAT4")
  case object RealArray extends PgArray(BuiltinOidDef(1021), Real)

  case object Double extends PgType(BuiltinOidDef(701), DoubleArray, classOf[Double], "FLOAT8")
  case object DoubleArray extends PgArray(BuiltinOidDef(1022), Double)

  case object Circle extends PgType(BuiltinOidDef(718), CircleArray, classOf[String], "CIRCLE")
  case object CircleArray extends PgArray(BuiltinOidDef(719), Circle)

  private[this] val varcharOidDef = BuiltinOidDef(
    1043, assignableFrom = Set(Text.pgNumericOid.get), assignableTo = Set(Text.pgNumericOid.get))
  private[this] val varcharArrayOidDef = BuiltinOidDef(
    1015, assignableFrom = Set(TextArray.pgNumericOid.get), assignableTo = Set(TextArray.pgNumericOid.get))

  case object Varchar extends PgType(varcharOidDef, VarcharArray, classOf[String], "VARCHAR")
  case object VarcharArray extends PgArray(varcharArrayOidDef, Varchar)

  case object Date extends PgType(BuiltinOidDef(1082), DateArray, classOf[LocalDate], "DATE")
  case object DateArray extends PgArray(BuiltinOidDef(1182), Date)

  case object Time extends PgType(BuiltinOidDef(1083), TimeArray, classOf[LocalTime], "TIME")
  case object TimeArray extends PgArray(BuiltinOidDef(1183), Time)

  case object Timestamp extends PgType(BuiltinOidDef(1114), TimestampArray, classOf[LocalDateTime], "TIMESTAMP")
  case object TimestampArray extends PgArray(BuiltinOidDef(1115), Timestamp)

  case object Timestamptz extends PgType(BuiltinOidDef(1184), TimestamptzArray, classOf[DateTime], "TIMESTAMPTZ")
  case object TimestamptzArray extends PgArray(BuiltinOidDef(1185), Timestamptz)

  case object Uuid extends PgType(BuiltinOidDef(2950), UuidArray, classOf[UUID], "UUID")
  case object UuidArray extends PgArray(BuiltinOidDef(2951), Uuid)

  case object Record extends PgType(BuiltinOidDef(2249), RecordArray, classOf[IndexedSeq[Any]], "RECORD")
  case object RecordArray extends PgArray(BuiltinOidDef(2287), Record)

  // Oid of Hstore depends of concrete database since Hstore is an extension
  case object Hstore extends PgType(CustomOidDef.apply(Set(), Set()), HstoreArray, classOf[String], "HSTORE")
  case object HstoreArray extends PgArray(CustomOidDef.apply(Set(), Set()), Hstore)

  lazy val AllTypes: Set[PgType] = {
    val resultBuilder = Set.newBuilder[PgType]
    import scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(this.getClass.getClassLoader)
    val typeMirror = universe.typeOf[this.type]
    for (member <- typeMirror.members if member.isModule) {
      val moduleSymbol = member.asModule
      if (moduleSymbol.typeSignature <:< universe.typeOf[PgType]) {
        resultBuilder += runtimeMirror.reflectModule(moduleSymbol).instance.asInstanceOf[PgType]
      }
    }
    resultBuilder.result()
  }

  private[this] lazy val oidToTypeMap = (for (t <- AllTypes; oid <- t.oidDef.pgNumericOid) yield (oid, t)).toMap
  private[this] lazy val nameToTypeMap = (for (t <- AllTypes) yield (t.sqlName.toLowerCase, t)).toMap

  @deprecated("Consider using fetchTypeName() and then use typeByName() instead. Not all OIDs are known statically", "")
  def typeByOid(oid: Int): Option[PgType] = oidToTypeMap.get(oid)

  def typeByName(name: String): Option[PgType] = nameToTypeMap.get(name.toLowerCase)

  def fetchTypeName(oid: Int)(implicit c: Connection): Option[String] = {
    val futureQueryResult = c.sendQuery(s"SELECT typname FROM pg_type WHERE pg_type.oid = $oid")
    val optDbName = RowDataParser.string(0).?.parse(Await.result(futureQueryResult, 5.seconds).rows.get)
    optDbName map { dbName =>
      if (dbName.startsWith("_")) dbName.substring(1) + "[]" else dbName
    }
  }

  def fetchTypeOid(typeName: String)(implicit c: Connection): Option[Int] = {
    val dbName = (if (typeName.endsWith("[]")) "_" + typeName.dropRight(2) else typeName).toLowerCase
    val futureQueryResult = c.sendQuery(s"SELECT pg_type.oid::integer FROM pg_type WHERE typname = '$dbName'")
    RowDataParser.int(0).?.parse(Await.result(futureQueryResult, 5.seconds).rows.get)
  }
}
