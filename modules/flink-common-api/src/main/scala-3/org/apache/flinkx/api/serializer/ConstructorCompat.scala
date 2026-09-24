package org.apache.flinkx.api.serializer

import java.lang.reflect.Constructor
import scala.reflect.NameTransformer
import scala.util.control.NonFatal

private[serializer] trait ConstructorCompat:
  // As of Scala version 3.1.2, there is no direct support for runtime reflection.
  // This is in contrast to Scala 2, which has its own APIs for reflecting on classes.
  // Thus, fallback to Java reflection and look up the constructor matching the required signature.
  final def lookupConstructor[T](cls: Class[T]): Array[AnyRef] => T =
    val constructor = constructorOf(cls)

    (args: Array[AnyRef]) => {
      if isEnum(constructor) then
        // Apply method is used for enum case classes because it cannot be instantiated by its constructor
        val applyMethod = cls.getMethod("apply", constructor.getParameterTypes*)
        applyMethod.invoke(null, args*).asInstanceOf[T]
      else constructor.newInstance(args*).asInstanceOf[T]
    }

  /** Names of the parameters [[lookupConstructor]] instantiates the given class with, in their order, empty when the
    * class file doesn't record them.
    */
  final def lookupFieldNames(cls: Class[?]): Array[String] =
    try
      val parameters = constructorOf(cls).getParameters
      if parameters.forall(_.isNamePresent) then parameters.map(parameter => NameTransformer.decode(parameter.getName))
      else Array.empty
    catch case NonFatal(_) => Array.empty

  // Types of parameters can fail to match when (un)boxing is used.
  // Say you have a class `final case class Foo(a: String, b: Int)`.
  // The first parameter is an alias for `java.lang.String`, which the constructor uses.
  // The second parameter is an alias for `java.lang.Integer`, but the constructor actually takes an unboxed `int`.
  private def constructorOf(cls: Class[?]): Constructor[?] =
    try
      cls.getConstructors
        .foldLeft[Option[Constructor[?]]](None) {
          case (Some(longest), c) if longest.getParameterCount < c.getParameterCount => Some(c)
          case (_, c)                                                                => Some(c)
        }
        .get
    catch
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"""
             |The class ${cls.getSimpleName} does not have a matching constructor.
             |It could be an instance class, meaning it is not a member of a
             |toplevel object, or of an object contained in a toplevel object,
             |therefore it requires an outer instance to be instantiated, but we don't have a
             |reference to the outer instance. Please consider changing the outer class to an object.
             |""".stripMargin,
          e
        )

  // Enum modifier constant defined in java.lang.reflect.Modifier.ENUM but inaccessible
  private val EnumModifier: Int = 0x00004000

  // Same check in java.lang.reflect.Constructor.acquireConstructorAccessor line 546 that prevents enum instantiation
  private def isEnum(constructor: Constructor[_]): Boolean =
    (constructor.getDeclaringClass.getModifiers & EnumModifier) != 0

/** Reads the constructor of a case class outside of its serializer, from its snapshot in particular. */
private[api] object ConstructorCompat extends ConstructorCompat
