package com.github.mirabout.calldb

import scala.reflect.ClassTag
import scala.collection.mutable

/**
  * Database storage traits of a type.
  */
sealed abstract class TypeTraits {
  def isBasic: Boolean = false
  def isCompound: Boolean = false

  def asOptBasic: Option[BasicTypeTraits] = if (isBasic) Some(asInstanceOf[BasicTypeTraits]) else None
  def asOptCompound: Option[CompoundTypeTraits] = if (isCompound) Some(asInstanceOf[CompoundTypeTraits]) else None

  def asEitherBasicOrCompound: Either[BasicTypeTraits, CompoundTypeTraits] =
    if (isBasic) Left(asInstanceOf[BasicTypeTraits]) else Right(asInstanceOf[CompoundTypeTraits])
}

/**
  * Database storage traits of scalars and arrays of scalars.
  */
sealed abstract class BasicTypeTraits extends TypeTraits {
  override def isBasic = true
  def storedInType: PgType
  def isNullable: Boolean
  def copyWithNullable(nullable: Boolean): BasicTypeTraits

  def toArrayTypeTraits: ArrayTypeTraits
  def toScalarTypeTraits: ScalarTypeTraits
}

/**
  * Database storage traits for scalars.
  * @param classInCode a JVM class of a scalar
  * @param storedInType a database type a scalar gets stored in
  * @param isNullable can a database scalar value be nullable
  */
final case class ScalarTypeTraits(classInCode: Class[_], storedInType: PgType, isNullable: Boolean = false)
  extends BasicTypeTraits with BugReporting {
  def copyWithNullable(nullable: Boolean): ScalarTypeTraits = copy(isNullable = nullable)
  def toArrayTypeTraits = ArrayTypeTraits(classInCode, storedInType.arrayType, isNullable)
  // If it gets called, it is a caller error
  def toScalarTypeTraits: Nothing = BUG(s"$this already is a ScalarTypeTraits")
}

/**
  * Database storage traits for arrays of scalars.
  * @param elemClassInCode a JVM class of an array element. Do not confuse this
  *                        with a class of a JVM array of scalars. These traits
  *                        correspond to an arbitrary [[Traversable]] of scalars.
  * @param storedInType a database type an array gets stored in
  * @param isNullable can a database array value be nullable
  */
final case class ArrayTypeTraits(elemClassInCode: Class[_], storedInType: PgArray, isNullable: Boolean = false)
  extends BasicTypeTraits with BugReporting {
  def copyWithNullable(nullable: Boolean): ArrayTypeTraits = copy(isNullable = nullable)
  def toScalarTypeTraits = ScalarTypeTraits(elemClassInCode, storedInType.elemType)
  // If it gets called, it is an caller error
  def toArrayTypeTraits: Nothing =
    BUG(s"$this already is an ArrayTypeTraits (arrays of arrays are not allowed as db entity fields)")
}

/**
  * Database storage type traits for compound types (records).
  */
sealed abstract class CompoundTypeTraits extends TypeTraits {
  override def isCompound = true
  def columnsTraits: IndexedSeq[BasicTypeTraits]
}

class CompoundTypeTraitsCompanion[T <: CompoundTypeTraits](constructor: IndexedSeq[BasicTypeTraits] => T) {
  def apply(args: TypeTraits*): T = {
    val (basic, compound) = args.partition(_.isBasic)
    if (compound.nonEmpty) {
      throw new AssertionError(s"There are compound traits $compound. Only basic type traits are allowed as arguments")
    }
    constructor(basic.toIndexedSeq.asInstanceOf[IndexedSeq[BasicTypeTraits]])
  }
}

/**
  * Database storage type traits for a single record-like compound value.
  * @param columnsTraits column traits for the record columns
  */
final case class RowTypeTraits(columnsTraits: IndexedSeq[BasicTypeTraits]) extends CompoundTypeTraits
object RowTypeTraits extends CompoundTypeTraitsCompanion[RowTypeTraits](args => new RowTypeTraits(args))

/**
  * Database storage type traits for a record-like compound value that is nullable.
  * @param columnsTraits column traits for the record columns
  */
final case class OptRowTypeTraits(columnsTraits: IndexedSeq[BasicTypeTraits]) extends CompoundTypeTraits
object OptRowTypeTraits extends CompoundTypeTraitsCompanion[OptRowTypeTraits](args => new OptRowTypeTraits(args))

/**
  * Database storage type traits for an arbitrary sequence of record-like values.
  * @param rowTypeTraits column traits for the record columns
  */
final case class RowSeqTypeTraits(rowTypeTraits: RowTypeTraits) extends CompoundTypeTraits {
  def columnsTraits: IndexedSeq[BasicTypeTraits] = rowTypeTraits.columnsTraits
}

object RowSeqTypeTraits extends CompoundTypeTraitsCompanion[RowSeqTypeTraits](args =>
  new RowSeqTypeTraits(new RowTypeTraits(args)))

/**
  * A bearer of [[TypeTraits]] that is specific to the type
  * @tparam A a type that is described by the [[TypeTraits]].
  *           The variance is correct e.g. a type provider of cats
  *           suits as a type provider of animals as well.
  */
trait TypeProvider[+A] {
  val typeTraits: TypeTraits
}

/**
  * A bearer of [[TypeTraits]] for scalars and arrays of scalars.
  */
trait BasicTypeProvider[+A] extends TypeProvider[A] {
  val typeTraits: BasicTypeTraits
}

/**
  * Same as [[BasicTypeProvider]], kept for backward compatibility
  */
@deprecated("Use BasicTypeProvider instead", "0.0.27")
trait ColumnTypeProvider[+A] extends BasicTypeProvider[A] {
  val typeTraits: BasicTypeTraits
}

/**
  * A bearer of [[TypeTraits]] for record-like types and collections of records.
  */
trait CompoundTypeProvider[+A] extends TypeProvider[A] {
  val typeTraits: CompoundTypeTraits
}

object TypeProvider {
  /**
    * Creates a [[TypeProvider]] for a single row that has traits of type [[RowTypeTraits]]
    * constructed based on the supplied list of type providers
    * (that are expected to have basic type traits).
    * @tparam A an expected generic type parameter for the provider ([[Nothing]] if not specified)
    */
  def forRow[A](args: TypeProvider[_]*): TypeProvider[A] =
    forTraits[A](RowTypeTraits(args map (_.typeTraits) :_*))

  /**
    * Creates a [[TypeProvider]] for an optional row that has traits of type [[OptRowTypeTraits]]
    * constructed based on the supplied list of type providers
    * (that are expected to have basic type traits).
    * @tparam A an expected generic type parameter for the provider ([[Nothing]] if not specified)
    */
  def forOptRow[A](args: TypeProvider[_]*): TypeProvider[A] =
    forTraits[A](OptRowTypeTraits(args map (_.typeTraits) :_*))

  /**
    * Creates a [[TypeProvider]] for an sequence of rows that has traits of type [[RowSeqTypeTraits]]
    * constructed based on the supplied list of type providers
    * (that are expected to have basic type traits).
    * @tparam A an expected generic type parameter for the provider ([[Nothing]] if not specified)
    */
  def forRowSeq[A](args: TypeProvider[_]*): TypeProvider[A] =
    forTraits[A](RowSeqTypeTraits(args map (_.typeTraits) :_*))

  /**
    * An utility method that may be imported in a target scope to save typing
    * @tparam A a type for that a type provider is expected
    *           (and an implicit provider is visible in the usage scope)
    */
  def typeProviderOf[A : TypeProvider]: TypeProvider[A] =
    implicitly[TypeProvider[A]]

  /**
    * An utility method that follows [[typeProviderOf]] idea
    * for obtaining [[TypeTraits]] instances for a given type.
    * @tparam A a type for that a type provider is expected
    *           (and an implicit provider is visible in the usage scope)
   */
  def typeTraitsOf[A: TypeProvider]: TypeTraits =
    typeProviderOf[A].typeTraits

  /**
    * An utility method that follows [[typeProviderOf]] idea
    * for obtaining implicit instances of [[BasicTypeProvider]] for a given type
    * @tparam A a type for that a [[BasicTypeProvider]] is expected
    *           (and an implicit provider is visible in the usage scope)
    */
  def basicTypeProviderOf[A: BasicTypeProvider]: BasicTypeProvider[A] =
    implicitly[BasicTypeProvider[A]]

  /**
    * An utility method that follows [[typeProviderOf]] and [[basicTypeProviderOf]] ideas
    * for obtaining instances of [[BasicTypeTraits]] for a given type.
    * @tparam A a type for that a [[BasicTypeProvider]] is expected
    *           (and an implicit provider is visible in the usage scope)
    */
  def basicTypeTraitsOf[A: BasicTypeProvider]: BasicTypeTraits =
    basicTypeProviderOf[A].typeTraits

  /**
    * An utility for creating instances of a [[TypeProvider]] based on the supplied [[TypeTraits]]
    */
  def forTraits[A](traits: TypeTraits): TypeProvider[A] = new TypeProvider[A] {
    val typeTraits: TypeTraits = traits
  }

  /**
    * An utility for creating instances of a [[BasicTypeProvider]] based on the supplied [[BasicTypeTraits]]
    */
  def forTraits[A](traits: BasicTypeTraits): BasicTypeProvider[A] = new BasicTypeProvider[A] {
    override val typeTraits: BasicTypeTraits = traits
  }

  /**
    * An utility for creating instances of a [[CompoundTypeProvider]] based on the supplied [[CompoundTypeTraits]]
    */
  def forTraits[A](traits: CompoundTypeTraits): CompoundTypeProvider[A] = new CompoundTypeProvider[A] {
    override val typeTraits: CompoundTypeTraits = traits
  }
}

trait ColumnTypeProviders extends BugReporting {

  import java.util.UUID
  import org.joda.time.{DateTime, LocalDateTime}
  import org.joda.time.{Duration, Period}

  /**
    * We expose this type as public for optimization purposes.
    * @todo We need to fix variance so we do not have to widen return type for these type providers below!
    */
  final class ScalarTypeProvider[A](clazz: Class[A], pgType: PgType) extends BasicTypeProvider[A] {
    override val typeTraits = ScalarTypeTraits(clazz, pgType)
  }

  implicit final val unitTypeProvider: BasicTypeProvider[Unit] =
    new ScalarTypeProvider(classOf[Unit], PgType.Void)
  implicit final val booleanTypeProvider: BasicTypeProvider[Boolean] =
    new ScalarTypeProvider(classOf[Boolean], PgType.Boolean)
  implicit final val shortTypeProvider: BasicTypeProvider[Short] =
    new ScalarTypeProvider(classOf[Short], PgType.Smallint)
  implicit final val charTypeProvider: BasicTypeProvider[Char] =
    new ScalarTypeProvider(classOf[Char], PgType.Char)
  implicit final val intTypeProvider: BasicTypeProvider[Int] =
    new ScalarTypeProvider(classOf[Int], PgType.Integer)
  implicit final val longTypeProvider: BasicTypeProvider[Long] =
    new ScalarTypeProvider(classOf[Long], PgType.Bigint)
  implicit final val floatTypeProvider: BasicTypeProvider[Float] =
    new ScalarTypeProvider(classOf[Float], PgType.Real)
  implicit final val doubleTypeProvider: BasicTypeProvider[Double] =
    new ScalarTypeProvider(classOf[Double], PgType.Double)
  implicit final val stringTypeProvider: BasicTypeProvider[String] =
    new ScalarTypeProvider(classOf[String], PgType.Text)
  implicit final val bytesTypeProvider: BasicTypeProvider[Array[Byte]] =
    new ScalarTypeProvider(classOf[Array[Byte]], PgType.Bytea)
  implicit final val uuidTypeProvider: BasicTypeProvider[UUID] =
    new ScalarTypeProvider(classOf[UUID], PgType.Uuid)
  implicit final val dateTimeTypeProvider: BasicTypeProvider[DateTime] =
    new ScalarTypeProvider(classOf[DateTime], PgType.Timestamptz)
  implicit final val localDateTimeTypeProvider: BasicTypeProvider[LocalDateTime] =
    new ScalarTypeProvider(classOf[LocalDateTime], PgType.Timestamp)
  implicit final val periodTypeProvider: BasicTypeProvider[Period] =
    new ScalarTypeProvider[Period](classOf[Period], PgType.Bigint)
  implicit final val durationTypeProvider: BasicTypeProvider[Duration] =
    new ScalarTypeProvider[Duration](classOf[Duration], PgType.Bigint)
  implicit final val hStoreTypeProvider: BasicTypeProvider[Map[String, Option[String]]] =
    new ScalarTypeProvider(classOf[Map[String, Option[String]]], PgType.Hstore)

  private def failOnNullElemProvider[A](elemProvider: TypeProvider[A]): Unit = {
    if (elemProvider eq null) {
      BUG(
        s"An element type provider is null. " +
        s"It may be caused by initialization order effects. " +
        s"Prefer `def` or `lazy val` in caller code.")
    }
  }

  implicit final def optionTypeProvider[A](implicit elemProvider: TypeProvider[A])
  : TypeProvider[Option[A]] = {
    failOnNullElemProvider(elemProvider)
    val typeTraits: TypeTraits = {
      elemProvider.typeTraits match {
        case bt: BasicTypeTraits => bt.copyWithNullable(nullable = true)
        case ct: CompoundTypeTraits => ct match {
          case _: RowTypeTraits => OptRowTypeTraits(ct.columnsTraits)
          case _: OptRowTypeTraits => BUG("Can't make a type traits for option of opt row")
          case _: RowSeqTypeTraits => BUG("Can't make a type traits for option of seq of rows")
        }
      }
    }
    TypeProvider.forTraits(typeTraits)
  }

  implicit final def basicOptionTypeProvider[A](implicit elemProvider: BasicTypeProvider[A])
  : BasicTypeProvider[Option[A]] = {
    failOnNullElemProvider(elemProvider)
    if (elemProvider.typeTraits.isNullable) {
      BUG(s"Can't make a type traits for option of option")
    }
    TypeProvider.forTraits(elemProvider.typeTraits.copyWithNullable(nullable = true))
  }

  implicit final def basicTraversableTypeProvider[Coll[_], A](implicit elemProvider: BasicTypeProvider[A])
  : BasicTypeProvider[Coll[A]] = {
    failOnNullElemProvider(elemProvider)
    if (elemProvider.typeTraits.isNullable) {
      BUG(s"Can't make a type traits for option of option")
    }
    if (!elemProvider.typeTraits.isBasic) {
      BUG(s"Only basic type traits are expected")
    }
    new ArrayLikeTypeProvider(elemProvider)
  }

  final class ArrayLikeTypeProvider[Coll[_], A](elemProvider: BasicTypeProvider[A]) extends BasicTypeProvider[Coll[A]] {
    val typeTraits: ArrayTypeTraits = elemProvider.typeTraits.toArrayTypeTraits
  }

  implicit final def traversableTypeProvider[Coll[_], A](implicit elemProvider: TypeProvider[A])
  : TypeProvider[Coll[A]] = {
    failOnNullElemProvider(elemProvider)
    elemProvider match {
      case ctp: BasicTypeProvider[A] => new ArrayLikeTypeProvider(ctp)
      case _ => elemProvider.typeTraits match {
        case ct: CompoundTypeTraits => ct match {
          case rt: RowTypeTraits => TypeProvider.forTraits(RowSeqTypeTraits(rt))
          case _: OptRowTypeTraits => BUG("Can't make IndexedSeq type provider for opt row type provider")
          case _: RowSeqTypeTraits => BUG("Can't make IndexedSeq type provider for row seq type provider")
        }
        case _ => BUG(s"Can't make IndexedSeq type provider for $elemProvider")
      }
    }
  }

  implicit final def tuple2TypeProvider[A, B](implicit p1: BasicTypeProvider[A],
                                              p2: BasicTypeProvider[B])
  : TypeProvider[(A, B)] = {
    TypeProvider.forTraits(RowTypeTraits(IndexedSeq(p1.typeTraits, p2.typeTraits)))
  }

  implicit final def tuple3TypeProvider[A, B, C](implicit p1: BasicTypeProvider[A],
                                                 p2: BasicTypeProvider[B],
                                                 p3: BasicTypeProvider[C])
  : TypeProvider[(A, B, C)] = {
    TypeProvider.forTraits(RowTypeTraits(IndexedSeq(p1.typeTraits, p2.typeTraits, p2.typeTraits)))
  }
}

private[calldb] object ColumnTypeProviders extends ColumnTypeProviders