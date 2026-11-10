package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.evolution.AdtEvolution.NoEvolution
import org.apache.flinkx.api.evolution.{Evolutions, Evolvable}
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ScalaCaseObjectSerializer, nullable}
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.{ClassTag, classTag}

private[api] trait TypeInformationDerivation {

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  def cache: DerivationCache = TypeInformationDerivation.cache

  /** Derivation of a case class at version 0, kept for the code compiled before release 2.4.0. */
  def join[T <: Product: ClassTag: TypeTag](ctx: CaseClass[TypeInformation, T]): TypeInformation[T] = {
    Evolutions.rejectUnversionedDerivation(classTag[T].runtimeClass)
    deriveCaseClass(ctx, Evolvable.unversioned.version)
  }

  /** Derivation of a sealed trait at version 0, kept for the code compiled before release 2.4.0. */
  def split[T: ClassTag: TypeTag](ctx: SealedTrait[TypeInformation, T]): TypeInformation[T] = {
    Evolutions.rejectUnversionedDerivation(classTag[T].runtimeClass)
    deriveSealedTrait(ctx, Evolvable.unversioned.version)
  }

  private[api] def deriveCaseClass[T <: Product: ClassTag: TypeTag](
      ctx: CaseClass[TypeInformation, T],
      version: Int
  ): TypeInformation[T] = {
    val cacheKey = DerivationCacheKey(classTag[T].runtimeClass, typeName[T], ctx.parameters.map(_.typeclass))
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        val clazz      = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        val fieldNames = ctx.parameters.map(_.label).toArray

        val evolution = new NoEvolution(clazz, version, Some(fieldNames))

        val serializer = if (typeOf[T].typeSymbol.isModuleClass) {
          new ScalaCaseObjectSerializer[T](evolution)
        } else {
          new CaseClassSerializer[T](
            evolution = evolution,
            isCaseClassImmutable = isCaseClassImmutable(clazz, fieldNames),
            fieldNames = Some(fieldNames),
            scalaFieldSerializers = ctx.parameters.map { p =>
              val ser = p.typeclass.createSerializer(config)
              if (p.annotations.exists(_.isInstanceOf[nullable])) {
                NullableSerializer.wrapIfNullIsNotSupported(ser, true)
              } else ser
            }.toArray
          )
        }

        val ti = new CaseClassTypeInfo[T](
          clazz = clazz,
          fieldTypes = ctx.parameters.map(_.typeclass),
          fieldNames = fieldNames,
          ser = serializer
        )
        cache.putIfAbsent(cacheKey, ti).getOrElse(ti).asInstanceOf[TypeInformation[T]]
    }
  }

  private[api] def deriveSealedTrait[T: ClassTag: TypeTag](
      ctx: SealedTrait[TypeInformation, T],
      version: Int
  ): TypeInformation[T] = {
    val cacheKey = DerivationCacheKey(classTag[T].runtimeClass, typeName[T], ctx.subtypes.map(_.typeclass))
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        val clazz          = classTag.runtimeClass.asInstanceOf[Class[T]]
        val subtypeClasses = ctx.subtypes.map(_.typeclass.getTypeClass).toArray[Class[_]]
        val subtypeNames   = subtypeClasses.map(_.getName)

        val serializer = new CoproductSerializer[T](
          evolution = Some(new NoEvolution(clazz, version)),
          subtypeClasses = subtypeClasses,
          subtypeNames = subtypeNames,
          subtypeSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
        )

        val ti = new CoproductTypeInformation[T](clazz, serializer)
        cache.putIfAbsent(cacheKey, ti).getOrElse(ti).asInstanceOf[TypeInformation[T]]
    }
  }

  private def typeName[T: TypeTag]: String = typeOf[T].toString

}

/** Derivation reading the schema version of the ADT. Magnolia requires a single `join` and `split` in this trait. */
private[api] trait EvolvingDerivation extends TypeInformationDerivation {

  def join[T <: Product: ClassTag: TypeTag: Evolvable](ctx: CaseClass[TypeInformation, T]): TypeInformation[T] =
    deriveCaseClass(ctx, implicitly[Evolvable[T]].version)

  def split[T: ClassTag: TypeTag: Evolvable](ctx: SealedTrait[TypeInformation, T]): TypeInformation[T] =
    deriveSealedTrait(ctx, implicitly[Evolvable[T]].version)

}

private[api] object TypeInformationDerivation {

  /** Storage of the cache exposed by [[TypeInformationDerivation.cache]]. */
  private val cache: DerivationCache = new DerivationCache()

}
