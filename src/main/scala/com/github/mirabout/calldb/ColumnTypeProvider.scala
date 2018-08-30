package com.github.mirabout.calldb

import scala.reflect.ClassTag
import scala.collection.mutable

sealed abstract class TypeTraits {
  def isBasic: Boolean = false
  def isCompound: Boolean = false

  def asOptBasic: Option[BasicTypeTraits] = if (isBasic) Some(asInstanceOf[BasicTypeTraits]) else None
  def asOptCompound: Option[CompoundTypeTraits] = if (isCompound) Some(asInstanceOf[CompoundTypeTraits]) else None

  def asEitherBasicOrCompound: Either[BasicTypeTraits, CompoundTypeTraits] =
    if (isBasic) Left(asInstanceOf[BasicTypeTraits]) else Right(asInstanceOf[CompoundTypeTraits])
}

sealed abstract class BasicTypeTraits extends TypeTraits {
  override def isBasic = true
  def storedInType: PgType
  def isNullable: Boolean
  def copyWithNullable(nullable: Boolean): BasicTypeTraits

  def toArrayTypeTraits: ArrayTypeTraits
  def toScalarTypeTraits: ScalarTypeTraits
}

final case class ScalarTypeTraits(classInCode: Class[_], storedInType: PgType, isNullable: Boolean = false)
  extends BasicTypeTraits with BugReporting {
  def copyWithNullable(nullable: Boolean): ScalarTypeTraits = copy(isNullable = nullable)
  def toArrayTypeTraits = ArrayTypeTraits(classInCode, storedInType.arrayType, isNullable)
  // If it gets called, it is a caller error
  def toScalarTypeTraits = BUG(s"$this already is a ScalarTypeTraits")
}

final case class ArrayTypeTraits(elemClassInCode: Class[_], storedInType: PgArray, isNullable: Boolean = false)
  extends BasicTypeTraits with BugReporting {
  def copyWithNullable(nullable: Boolean): ArrayTypeTraits = copy(isNullable = nullable)
  def toScalarTypeTraits = ScalarTypeTraits(elemClassInCode, storedInType.elemType)
  // If it gets called, it is an caller error
  def toArrayTypeTraits =
    BUG(s"$this already is an ArrayTypeTraits (arrays of arrays are not allowed as db entity fields)")
}

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

final case class RowTypeTraits(columnsTraits: IndexedSeq[BasicTypeTraits]) extends CompoundTypeTraits
object RowTypeTraits extends CompoundTypeTraitsCompanion[RowTypeTraits](args => new RowTypeTraits(args))

final case class OptRowTypeTraits(columnsTraits: IndexedSeq[BasicTypeTraits]) extends CompoundTypeTraits
object OptRowTypeTraits extends CompoundTypeTraitsCompanion[OptRowTypeTraits](args => new OptRowTypeTraits(args))

final case class RowSeqTypeTraits(rowTypeTraits: RowTypeTraits) extends CompoundTypeTraits {
  def columnsTraits: IndexedSeq[BasicTypeTraits] = rowTypeTraits.columnsTraits
}

object RowSeqTypeTraits extends CompoundTypeTraitsCompanion[RowSeqTypeTraits](args =>
  new RowSeqTypeTraits(new RowTypeTraits(args)))

trait TypeProvider[A] {
  val typeTraits: TypeTraits
}

trait BasicTypeProvider[A] extends TypeProvider[A] {
  val typeTraits: BasicTypeTraits
}

/**
  * Same as [[BasicTypeProvider]], kept for backward compatibility
  */
@deprecated("Use BasicTypeProvider instead", "0.0.27")
trait ColumnTypeProvider[A] extends BasicTypeProvider[A] {
  val typeTraits: BasicTypeTraits
}

trait CompoundTypeProvider[A] extends TypeProvider[A] {
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
  def basicTypeTraitsOf[A: ColumnTypeProvider]: BasicTypeTraits =
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

  final class ScalarProvider[A](clazz: Class[A], pgType: PgType) extends ColumnTypeProvider[A] {
    override val typeTraits = ScalarTypeTraits(clazz, pgType)
  }

  implicit final val unitColumnTypeProvider = new ScalarProvider(classOf[Unit], PgType.Void)
  implicit final val booleanColumnTypeProvider = new ScalarProvider(classOf[Boolean], PgType.Boolean)
  implicit final val shortColumnTypeProvider = new ScalarProvider(classOf[Short], PgType.Smallint)
  implicit final val charColumnTypeProvider = new ScalarProvider(classOf[Char], PgType.Char)
  implicit final val intColumnTypeProvider = new ScalarProvider(classOf[Int], PgType.Integer)
  implicit final val longColumnTypeProvider = new ScalarProvider(classOf[Long], PgType.Bigint)
  implicit final val floatColumnTypeProvider = new ScalarProvider(classOf[Float], PgType.Real)
  implicit final val doubleColumnTypeProvider = new ScalarProvider(classOf[Double], PgType.Double)
  implicit final val stringColumnTypeProvider = new ScalarProvider(classOf[String], PgType.Text)
  implicit final val bytesTypeProvider = new ScalarProvider(classOf[Array[Byte]], PgType.Bytea)
  implicit final val uuidColumnTypeProvider = new ScalarProvider(classOf[UUID], PgType.Uuid)
  implicit final val dateTimeTypeProvider = new ScalarProvider(classOf[DateTime], PgType.Timestamptz)
  implicit final val localDateTimeTypeProvider = new ScalarProvider(classOf[LocalDateTime], PgType.Timestamp)

  private def failOnNullElemProvider[A](elemProvider: TypeProvider[A], classTag: ClassTag[A]): Unit = {
    if (elemProvider eq null) {
      BUG(
        s"Elem provider for $classTag is null. " +
        s"It may be caused by initialization order effects. " +
        s"Prefer `def` or `lazy val` in caller code.")
    }
  }

  // More generic version
  implicit final def optionTypeProvider[A](implicit elemProvider: TypeProvider[A], classTag: ClassTag[A] = null): TypeProvider[Option[A]] = {
    new TypeProvider[Option[A]] {
      failOnNullElemProvider(elemProvider, classTag)
      val typeTraits: TypeTraits = {
        elemProvider.typeTraits match {
          case bt: BasicTypeTraits => bt.copyWithNullable(nullable = true)
          case ct: CompoundTypeTraits => ct match {
            case rt: RowTypeTraits => OptRowTypeTraits(ct.columnsTraits)
            case ot: OptRowTypeTraits => BUG("Can't make a type traits for option of opt row")
            case st: RowSeqTypeTraits => BUG("Can't make a type traits for option of seq of rows")
          }
        }
      }
    }
  }

  implicit final def columnOptionTypeProvider[A](implicit elemProvider: ColumnTypeProvider[A], classTag: ClassTag[A])
      : ColumnTypeProvider[Option[A]] = {
    new ColumnTypeProvider[Option[A]] {
      failOnNullElemProvider(elemProvider, classTag)
      if (elemProvider.typeTraits.isNullable) {
        BUG(s"Can't make a type traits for option of option")
      }
      val typeTraits: BasicTypeTraits = elemProvider.typeTraits.copyWithNullable(nullable = true)
    }
  }

  final class ArrayLikeTypeProvider[Coll[_], A](elemProvider: ColumnTypeProvider[A]) extends ColumnTypeProvider[Coll[A]] {
    val typeTraits: ArrayTypeTraits = elemProvider.typeTraits.toArrayTypeTraits
  }

  // More generic version. We use classTag for debugging only, type gets erased completely
  implicit final def indexedSeqTypeProvider[A](implicit elemProvider: TypeProvider[A], classTag: ClassTag[A] = null)
      : TypeProvider[IndexedSeq[A]] = {
    failOnNullElemProvider(elemProvider, null)
    elemProvider match {
      case ctp: ColumnTypeProvider[A] => new ArrayLikeTypeProvider(ctp)
      case _ => elemProvider.typeTraits match {
        case ct: CompoundTypeTraits => ct match {
          case rt: RowTypeTraits => new TypeProvider[IndexedSeq[A]] {
            val typeTraits: RowSeqTypeTraits = RowSeqTypeTraits(rt)
          }
          case ot: OptRowTypeTraits => BUG("Can't make IndexedSeq type provider for opt row type provider")
          case st: RowSeqTypeTraits => BUG("Can't make IndexedSeq type provider for row seq type provider")
        }
        case _ => BUG(s"Can't make IndexedSeq type provider for $elemProvider")
      }
    }
  }

  implicit final def columnIndexedSeqTypeProvider[A](implicit elemProvider: ColumnTypeProvider[A]): ColumnTypeProvider[IndexedSeq[A]] =
    new ArrayLikeTypeProvider(elemProvider)

  implicit final def seqTypeProvider[A](implicit elemProvider: TypeProvider[A], d: DummyImplicit, classTag: ClassTag[A] = null): TypeProvider[Seq[A]] =
    indexedSeqTypeProvider(elemProvider, classTag).asInstanceOf[TypeProvider[Seq[A]]]  // TODO: Check variance to avoid cast?

  implicit final def columnSeqTypeProvider[A](implicit elemProvider: ColumnTypeProvider[A]): ColumnTypeProvider[Seq[A]] =
    new ArrayLikeTypeProvider(elemProvider)

  implicit final def setTypeProvider[A](implicit elemProvider: TypeProvider[A], d: DummyImplicit, classTag: ClassTag[A] = null): TypeProvider[Set[A]] =
    indexedSeqTypeProvider(elemProvider, classTag).asInstanceOf[TypeProvider[Set[A]]] // A param type is erased, so the cast is legal

  implicit final def columnSetTypeProvider[A](implicit elemProvider: ColumnTypeProvider[A]): ColumnTypeProvider[Set[A]] =
    new ArrayLikeTypeProvider(elemProvider)

  implicit final def traversableTypeProvider[A](implicit elemProvider: TypeProvider[A], d: DummyImplicit, classTag: ClassTag[A] = null): TypeProvider[Traversable[A]] =
    indexedSeqTypeProvider(elemProvider, classTag).asInstanceOf[TypeProvider[Traversable[A]]] // Param type is erased, so cast is ok

  implicit final def columnTraversableTypeProvider[A](implicit elemProvider: ColumnTypeProvider[A]): ColumnTypeProvider[Traversable[A]] =
    new ArrayLikeTypeProvider(elemProvider)

  implicit final def tuple2TypeProvider[A, B](implicit p1: ColumnTypeProvider[A], p2: ColumnTypeProvider[B]): TypeProvider[(A, B)] = {
    new TypeProvider[(A, B)] {
      override val typeTraits: RowTypeTraits = RowTypeTraits(IndexedSeq(p1.typeTraits, p2.typeTraits))
    }
  }

  implicit final def tuple3TypeProvider[A, B, C](implicit p1: ColumnTypeProvider[A], p2: ColumnTypeProvider[B], p3: ColumnTypeProvider[C]): TypeProvider[(A, B, C)] = {
    new TypeProvider[(A, B, C)] {
      override val typeTraits: RowTypeTraits = RowTypeTraits(IndexedSeq(p1.typeTraits, p2.typeTraits, p2.typeTraits))
    }
  }

  implicit final val periodTypeProvider = new ScalarProvider[Period](classOf[Period], PgType.Bigint)
  implicit final val durationTypeProvider = new ScalarProvider[Duration](classOf[Duration], PgType.Bigint)

  implicit final val hStoreTypeProvider = new ColumnTypeProvider[Map[String, Option[String]]] {
    val typeTraits = ScalarTypeTraits(classOf[Map[_,_]], PgType.Hstore)
  }
}

private[calldb] object ColumnTypeProviders extends ColumnTypeProviders