package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.evolution.{EvolutionBuilder, EvolutionNotAllowedException, Evolutions}
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ScalaCaseObjectSerializer, nullable}
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.collection.concurrent.TrieMap
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.{ClassTag, classTag}

private[api] trait TypeInformationDerivation {

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  def cache: TrieMap[DerivationCacheKey, TypeInformation[_]] = TypeInformationDerivation.cache

  def join[T <: Product: ClassTag: TypeTag](
      ctx: CaseClass[TypeInformation, T]
  ): TypeInformation[T] = {
    val cacheKey = DerivationCacheKey(typeName[T], ctx.parameters.map(_.typeclass))
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        val clazz      = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        val version    = Evolutions.findVersionInAnnotations(clazz, ctx.annotations)
        val fieldNames = ctx.parameters.map(_.label).toArray

        val builder = new EvolutionBuilder(clazz, version, fieldNames) // Field names required even with version 0
        if (version == 0) {
          // Do not allow Evolution annotations on version 0
          ctx.annotations.foreach {
            case _: renamed if ctx.inheritedAnnotations.exists(_.isInstanceOf[version]) => // Allow @renamed if parent has version
            case e: Evolved => throw EvolutionNotAllowedException(e, s"$clazz with version 0")
            case _          => // Ignore other annotations
          }
          ctx.parameters.foreach { p =>
            p.annotations.foreach {
              case e: Evolved => throw EvolutionNotAllowedException(e, s"$p of $clazz with version 0")
              case _          => // Ignore other annotations
            }
          }
        } else { // version > 0
          // Iterate over case class annotations to register evolutions from current source code
          ctx.annotations.foreach {
            case r: renamed        => builder.registerFormerClass(r.formerName, clazz, r.since)
            case d: deletedFields  => d.formerNames.foreach(builder.fieldEvolutions += Delete(d.since, clazz, _))
            case d: deletedClasses =>
              d.formerClassNames.foreach(builder.registerDeletedFormerClass(_, clazz, d.since, d.throwOnInstance))
            case p: postDeserialize[T] => builder.addPostDeserialize(p)
            case e: Evolved            => throw EvolutionNotAllowedException(e, clazz.toString)
            case _                     => // Ignore other annotations
          }
          // Iterate over case class fields annotations to register evolutions from current source code
          ctx.parameters.foreach { p =>
            p.annotations.foreach {
              case a: added             => builder.fieldEvolutions += Add(a.since, clazz, p.label, p.default)
              case r: renamed           => builder.fieldEvolutions += Rename(r.since, clazz, r.formerName, p.label)
              case t: transformed[_, _] => builder.fieldEvolutions += Transform(t.since, clazz, p.label, t.mapper)
              case e: version           => throw EvolutionNotAllowedException(e, s"$clazz.${p.label}")
              case e: Evolved           => throw EvolutionNotAllowedException(e, s"$clazz.${p.label}")
              case _                    => // Ignore other annotations
            }
          }
        }
        Evolutions.register(builder)
        val evolution = Evolutions.get(clazz, version)

        val serializer = if (typeOf[T].typeSymbol.isModuleClass) {
          new ScalaCaseObjectSerializer[T](evolution, version)
        } else {
          new CaseClassSerializer[T](
            evolution = evolution,
            version = version,
            isCaseClassImmutable = isCaseClassImmutable(clazz, fieldNames),
            fieldNames = fieldNames,
            paramSerializers = ctx.parameters.map { p =>
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

  def split[T: ClassTag: TypeTag](ctx: SealedTrait[TypeInformation, T]): TypeInformation[T] = {
    val cacheKey = DerivationCacheKey(typeName[T], ctx.subtypes.map(_.typeclass))
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        val clazz          = classTag.runtimeClass.asInstanceOf[Class[T]]
        val version        = Evolutions.findVersionInAnnotations(clazz, ctx.annotations)
        val subtypeClasses = ctx.subtypes.map(_.typeclass.getTypeClass).toArray[Class[_]]
        val subtypeFqns    = subtypeClasses.map(_.getName)

        if (version == 0) {
          // Do not allow Evolution annotations on version 0
          ctx.annotations.foreach {
            case e: Evolved => throw EvolutionNotAllowedException(e, s"$clazz with version 0")
            case _          => // Ignore other annotations
          }
          ctx.subtypes.foreach { p =>
            p.annotations.foreach {
              case e: Evolved => throw EvolutionNotAllowedException(e, s"$p of $clazz with version 0")
              case _          => // Ignore other annotations
            }
          }
        } else { // version > 0
          val builder = new EvolutionBuilder(clazz, version, subtypeFqns)
          // Iterate over coproduct annotations to register evolutions from current source code
          ctx.annotations.foreach {
            case r: renamed        => builder.registerFormerClass(r.formerName, clazz, r.since)
            case d: deletedClasses =>
              d.formerClassNames.foreach(builder.registerDeletedFormerClass(_, clazz, d.since, d.throwOnInstance))
            case p: postDeserialize[T] => builder.addPostDeserialize(p)
            case e: Evolved            => throw EvolutionNotAllowedException(e, clazz.toString)
            case _                     => // Ignore other annotations
          }
          // Iterate over subtypes annotations to register evolutions from current source code
          ctx.subtypes.foreach { p =>
            p.annotations.collect {
              case _: renamed if p.annotations.exists(_.isInstanceOf[version])        => // registered by join()
              case _: deletedFields if p.annotations.exists(_.isInstanceOf[version])  => // allowed on versioned subtype
              case _: deletedClasses if p.annotations.exists(_.isInstanceOf[version]) => // allowed on versioned subtype
              case _: postDeserialize[T] if p.annotations.exists(_.isInstanceOf[version]) => // allowed on versioned subtype
              case e: Evolved => throw EvolutionNotAllowedException(e, p.typeclass.getTypeClass.toString)
              case _          => // Ignore other annotations
            }
          }
          Evolutions.register(builder)
        }
        val evolution = Evolutions.get(clazz, version)

        val serializer = new CoproductSerializer[T](
          evolution = evolution,
          version = version,
          subtypeClasses = subtypeClasses,
          subtypeFqns = subtypeFqns,
          subtypeSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
        )

        val ti = new CoproductTypeInformation[T](clazz, serializer)
        cache.putIfAbsent(cacheKey, ti).getOrElse(ti).asInstanceOf[TypeInformation[T]]
    }
  }

  private def typeName[T: TypeTag]: String = typeOf[T].toString

}

private[api] object TypeInformationDerivation {

  /** Storage of the cache exposed by [[TypeInformationDerivation.cache]]. */
  private val cache: TrieMap[DerivationCacheKey, TypeInformation[_]] = TrieMap.empty

}
