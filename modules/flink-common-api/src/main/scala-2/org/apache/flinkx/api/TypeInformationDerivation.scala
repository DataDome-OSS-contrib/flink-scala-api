package org.apache.flinkx.api

import magnolia1.{CaseClass, SealedTrait}
import org.apache.flink.api.common.serialization.{SerializerConfig, SerializerConfigImpl}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolutions.throwEvolutionNotAllowed
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.evolution.{EvolutionBuilder, Evolutions}
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ScalaCaseObjectSerializer, nullable}
import org.apache.flinkx.api.typeinfo.{CaseClassTypeInfo, CoproductTypeInformation, MarkerTypeInfo}
import org.apache.flinkx.api.util.ClassUtil.isCaseClassImmutable

import scala.collection.mutable
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.{ClassTag, classTag}

private[api] trait TypeInformationDerivation {

  private[api] type Typeclass[T] = TypeInformation[T]

  private val config: SerializerConfig = new SerializerConfigImpl()

  protected val cache: mutable.Map[String, TypeInformation[_]] = mutable.Map.empty

  def join[T <: Product: ClassTag: TypeTag](
      ctx: CaseClass[TypeInformation, T]
  ): TypeInformation[T] = {
    val cacheKey = typeName[T]
    cache.get(cacheKey) match {
      case Some(MarkerTypeInfo) =>
        throw new FlinkRuntimeException(s"Unsupported: recursivity detected in '$cacheKey'.")
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        cache.put(cacheKey, MarkerTypeInfo)
        val clazz      = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        val version    = Evolutions.findVersionInAnnotations(clazz, ctx.annotations)
        val fieldNames = ctx.parameters.map(_.label).toArray
        val serializer = if (typeOf[T].typeSymbol.isModuleClass) {
          new ScalaCaseObjectSerializer[T](clazz)
        } else {
          new CaseClassSerializer[T](
            clazz = clazz,
            isCaseClassImmutable = isCaseClassImmutable(clazz, fieldNames),
            version = version,
            fieldNames = fieldNames,
            paramSerializers = ctx.parameters.map { p =>
              val ser = p.typeclass.createSerializer(config)
              if (p.annotations.exists(_.isInstanceOf[nullable])) {
                NullableSerializer.wrapIfNullIsNotSupported(ser, true)
              } else ser
            }.toArray
          )
        }

        val builder = new EvolutionBuilder(clazz, fieldNames) // Field names required even with version 0
        if (version == 0) {
          // Do not allow Evolution annotations without a version
          ctx.annotations.foreach {
            case _: renamed if ctx.inheritedAnnotations.exists(_.isInstanceOf[version]) => // Allow @renamed if parent has version
            case e: Evolved => throwEvolutionNotAllowed(e, s"$clazz without version")
            case _          => // Ignore other annotations
          }
          ctx.parameters.foreach { p =>
            p.annotations.foreach {
              case e: Evolved => throwEvolutionNotAllowed(e, s"$p of $clazz without version")
              case _          => // Ignore other annotations
            }
          }
        } else { // version > 0
          // Iterate over case class annotations to register evolutions from current source code
          ctx.annotations.foreach {
            case r: renamed            => Evolutions.registerFormerClass(r.from, clazz)
            case d: deletedFields      => d.names.foreach(builder.fieldEvolutions += Delete(d.since, clazz, _))
            case d: deletedClasses     => d.names.foreach(Evolutions.registerDeletedFormerClass(_, clazz))
            case p: postDeserialize[T] => builder.addPostDeserialize(p)
            case e: Evolved            => throwEvolutionNotAllowed(e, clazz.toString)
            case _                     => // Ignore other annotations
          }
          // Iterate over case class fields annotations to register evolutions from current source code
          ctx.parameters.foreach { p =>
            p.annotations.foreach {
              case a: added             => builder.fieldEvolutions += Add(a.since, clazz, p.label, p.default)
              case r: renamed           => builder.fieldEvolutions += Rename(r.since, clazz, p.label, r.from)
              case t: transformed[_, _] => builder.fieldEvolutions += Transform(t.since, clazz, p.label, t.mapper)
              case e: version           => throwEvolutionNotAllowed(e, s"$clazz.${p.label}")
              case e: Evolved           => throwEvolutionNotAllowed(e, s"$clazz.${p.label}")
              case _                    => // Ignore other annotations
            }
          }
        }
        Evolutions.register(builder)

        val ti = new CaseClassTypeInfo[T](
          clazz = clazz,
          fieldTypes = ctx.parameters.map(_.typeclass),
          fieldNames = fieldNames,
          ser = serializer
        )
        cache.put(cacheKey, ti)
        ti
    }
  }

  def split[T: ClassTag: TypeTag](ctx: SealedTrait[TypeInformation, T]): TypeInformation[T] = {
    val cacheKey = typeName[T]
    cache.get(cacheKey) match {
      case Some(cached) => cached.asInstanceOf[TypeInformation[T]]
      case None         =>
        val clazz      = classTag.runtimeClass.asInstanceOf[Class[T]]
        val version    = Evolutions.findVersionInAnnotations(clazz, ctx.annotations)
        val serializer = new CoproductSerializer[T](
          clazz = clazz,
          version = version,
          subtypeClasses = ctx.subtypes.map(_.typeclass.getTypeClass).toArray,
          subtypeSerializers = ctx.subtypes.map(_.typeclass.createSerializer(config)).toArray
        )

        if (version == 0) {
          // Do not allow Evolution annotations without a version
          ctx.annotations.foreach {
            case e: Evolved => throwEvolutionNotAllowed(e, s"$clazz without version")
            case _          => // Ignore other annotations
          }
          ctx.subtypes.foreach { p =>
            p.annotations.foreach {
              case e: Evolved => throwEvolutionNotAllowed(e, s"$p of $clazz without version")
              case _          => // Ignore other annotations
            }
          }
        } else { // version > 0
          val builder = new EvolutionBuilder(clazz)
          // Iterate over coproduct annotations to register evolutions from current source code
          ctx.annotations.foreach {
            case r: renamed            => Evolutions.registerFormerClass(r.from, clazz)
            case d: deletedClasses     => d.names.foreach(Evolutions.registerDeletedFormerClass(_, clazz))
            case p: postDeserialize[T] => builder.addPostDeserialize(p)
            case e: Evolved            => throwEvolutionNotAllowed(e, clazz.toString)
            case _                     => // Ignore other annotations
          }
          // Iterate over subtypes annotations to register evolutions from current source code
          ctx.subtypes.foreach { p =>
            p.annotations.collect {
              case r: renamed => Evolutions.registerFormerClass(r.from, p.typeclass.getTypeClass)
              case _: deletedFields if p.annotations.exists(_.isInstanceOf[version])  => // allowed on versioned subtype
              case _: deletedClasses if p.annotations.exists(_.isInstanceOf[version]) => // allowed on versioned subtype
              case _: postDeserialize[T] if p.annotations.exists(_.isInstanceOf[version]) => // allowed on versioned subtype
              case e: Evolved => throwEvolutionNotAllowed(e, p.typeclass.getTypeClass.toString)
              case _          => // Ignore other annotations
            }
          }
          Evolutions.register(builder)
        }

        val ti = new CoproductTypeInformation[T](clazz, serializer)
        cache.put(cacheKey, ti)
        ti
    }
  }

  private def typeName[T: TypeTag]: String = typeOf[T].toString

}
