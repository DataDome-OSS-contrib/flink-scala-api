package org.apache.flinkx.api.serializer

import scala.annotation.nowarn
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe

private[serializer] trait ConstructorCompat {

  @nowarn("msg=(eliminated by erasure)|(explicit array)")
  final def lookupConstructor[T <: Product](cls: Class[T]): Array[AnyRef] => T = {
    val rootMirror  = universe.runtimeMirror(cls.getClassLoader)
    val classSymbol = rootMirror.classSymbol(cls)

    require(
      classSymbol.isStatic,
      s"""
         |The class ${cls.getSimpleName} is an instance class, meaning it is not a member of a
         |top level object, or of an object contained in a top level object,
         |therefore it requires an outer instance to be instantiated, but we don't have a
         |reference to the outer instance. Please consider changing the outer class to an object.
         |""".stripMargin
    )

    val primaryConstructorSymbol = classSymbol.toType
      .decl(universe.termNames.CONSTRUCTOR)
      .alternatives
      .collectFirst {
        case constructorSymbol: universe.MethodSymbol if constructorSymbol.isPrimaryConstructor =>
          constructorSymbol
      }
      .head
      .asMethod

    val classMirror = rootMirror.reflectClass(classSymbol)
    val constructor = classMirror.reflectConstructor(primaryConstructorSymbol)

    (args: Array[AnyRef]) => constructor.apply(args: _*).asInstanceOf[T]
  }

}
