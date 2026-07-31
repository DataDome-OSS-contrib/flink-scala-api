package org.apache.flinkx.api.serializer

import java.lang.reflect.Constructor
import scala.util.control.NonFatal

private[serializer] trait ConstructorCompat:
  // As of Scala version 3.1.2, there is no direct support for runtime reflection.
  // This is in contrast to Scala 2, which has its own APIs for reflecting on classes.
  // Thus, fallback to Java reflection and look up the constructor matching the required signature.
  final def lookupConstructor[T](cls: Class[T]): Array[AnyRef] => T =
    // Types of parameters can fail to match when (un)boxing is used.
    // Say you have a class `final case class Foo(a: String, b: Int)`.
    // The first parameter is an alias for `java.lang.String`, which the constructor uses.
    // The second parameter is an alias for `java.lang.Integer`, but the constructor actually takes an unboxed `int`.
    val constructor =
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

    lazy val defaultArgs = lookupDefaultValues(cls).map(_._2)

    (args: Array[AnyRef]) => {
      // Append default values for missing arguments
      val allArgs = args ++ defaultArgs.takeRight(constructor.getParameterCount - args.length)
      if isEnum(constructor) then
        // Apply method is used for enum case classes because it cannot be instantiated by its constructor
        val applyMethod = cls.getMethod("apply", constructor.getParameterTypes*)
        applyMethod.invoke(null, allArgs*).asInstanceOf[T]
      else constructor.newInstance(allArgs*).asInstanceOf[T]
    }

  /** 1-based indices of the class primary constructor parameters having a default value. */
  final def defaultValueIndices(cls: Class[?]): Set[Int] = lookupDefaultValues(cls).map(_._1).toSet

  /** Default values of the class primary constructor parameters, by 1-based parameter index, in declaration order.
    * Parameters without a default value are skipped.
    */
  private def lookupDefaultValues(cls: Class[?]): Array[(Int, AnyRef)] =
    cls.getMethods
      .filter(_.getName.startsWith(DefaultValueMethodPrefix))
      .map(method => method.getName.drop(DefaultValueMethodPrefix.length).toInt -> method)
      .sortBy(_._1) // Sort by parameter index, as the lexicographic order of the method names isn't the same
      .map((index, method) => index -> method.invoke(null))

  // Prefix of the methods giving the default value of the constructor parameter of the suffixed 1-based index
  private val DefaultValueMethodPrefix: String = "$lessinit$greater$default$"

  // Enum modifier constant defined in java.lang.reflect.Modifier.ENUM but inaccessible
  private val EnumModifier: Int = 0x00004000

  // Same check in java.lang.reflect.Constructor.acquireConstructorAccessor line 546 that prevents enum instantiation
  private def isEnum(constructor: Constructor[_]): Boolean =
    (constructor.getDeclaringClass.getModifiers & EnumModifier) != 0
