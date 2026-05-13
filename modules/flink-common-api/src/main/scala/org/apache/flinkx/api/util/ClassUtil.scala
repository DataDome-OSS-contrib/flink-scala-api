package org.apache.flinkx.api.util

import org.apache.flink.util.FlinkRuntimeException

import java.lang.reflect.{Field, Modifier}

object ClassUtil {

  /** Checks if given case class is immutable by checking if all its parameters are val (final fields).
    *
    * @param clazz
    *   The case class to check.
    * @param paramNames
    *   The names of the case class parameters.
    * @return
    *   true if all the case class parameters are immutable, false otherwise.
    */
  def isCaseClassImmutable(clazz: Class[_], paramNames: Array[String]): Boolean = {
    val declaredFields: Array[Field] = clazz.getDeclaredFields
    paramNames.forall(paramName =>
      declaredFields
        .find(field => field.getName == paramName)
        .orElse(declaredFields.find(field => field.getName == s"${clazz.getName.replace('.', '$')}$$$$$paramName"))
        // return true if the field isn't found in the class: case classes can only override val from parent classes
        .forall(field => Modifier.isFinal(field.getModifiers))
    )
  }

  /** Resolve a former class name to its fully qualified binary name (see [[java.lang.ClassLoader]]) based on the
    * current class.
    *
    * The given `formerClassName` can be a simple name, a relative path or an absolute path.
    *
    * With a relative path, the first component (package or class) of `formerClassName` has to be in common with one
    * component of the current class's package or nesting hierarchy.
    *
    * The resolved fully qualified former class name is then converted it to the JVM internal format where nested
    * classes are separated by `$` instead of dots.
    *
    * Warn: due to the limitation of String manipulation without the possibility to load a class that doesn't exist
    * anymore in the classpath, we have to rely on naming convention in one relative path edge case: if a former class
    * has been moved outside its top level class, this top level class must have a capital letter. If it's not the case,
    * a `$` has to be used as separator between inner classes.
    *
    * ==Examples==
    *
    * {{{
    * package org.example
    *
    * object Parent {
    *
    *   @version(1)
    *   @renamed(since = 1, formerName = "OldName")
    *   case object NewName
    * }
    * }}}
    *
    * With previous snippet, `formerName` can be:
    *   - a simple name: `"OldName"` is resolved to `org.example.Parent$OldName` former class name
    *   - a relative path, its first component (package or class) has to be in common with one component of the
    *     annotated class:
    *   - `"Parent.OldName"` → `org.example.Parent$OldName`
    *   - `"example.oldPackage.OldName"` → `org.example.oldPackage.OldName`
    *   - `"example.lowerClass$OldName"` → `org.example.lowerClass$OldName`
    *   - an absolute path: `"old.example.OldName"` → `old.example.OldName`
    *
    * @param formerClassName
    *   The former class name
    * @param currentClass
    *   A class currently existing, used as context for resolving relative former class name
    * @return
    *   The fully qualified internal former class name (e.g., `org.example.Outer$Inner`)
    * @throws FlinkRuntimeException
    *   If `formerClassName` is empty, contains leading/consecutive/trailing dots
    */
  def resolveFormerClassName(formerClassName: String, currentClass: Class[_]): String = {
    if (formerClassName.isEmpty || formerClassName.matches("^[.].*|.*[.][.].*|.*[.]$")) {
      throw new FlinkRuntimeException(s"Class name '$formerClassName' in from parameter is malformed.")
    }

    /** Convert dot-separated path to JVM internal class name format: `package.Outer.Inner` → `package.Outer$Inner` */
    def toInternalClassName(formerFqn: String, firstClass: String): String = {
      // Find the first uppercase component (first class), then replace remaining dots with `$`.
      val components      = formerFqn.split('.')
      val firstClassIndex = components.indexOf(firstClass)
      val firstUpperIndex = if (firstClassIndex >= 0) firstClassIndex else components.indexWhere(_.head.isUpper)
      if (firstUpperIndex >= 0 && firstUpperIndex < components.length - 1) {
        val packageAndFirstClass = components.take(firstUpperIndex + 1).mkString(".")
        val nestedClasses        = components.drop(firstUpperIndex + 1).mkString("$")
        s"$packageAndFirstClass$$$nestedClasses"
      } else {
        formerFqn
      }
    }

    /** Walks the declaring class nesting hierarchy and returns a list of class names */
    def toClassNames(clazz: Class[_]): List[String] =
      if (clazz == null) List.empty else toClassNames(clazz.getDeclaringClass) :+ clazz.getSimpleName.stripSuffix("$")

    val formerComponents       = formerClassName.split('.')
    val currentClassNames      = toClassNames(currentClass)
    val currentClassComponents = currentClass.getPackageName.split('.') ++ currentClassNames

    // Match the first component of former class name against the current class hierarchy and resolve accordingly
    val formerFqn = currentClassComponents.lastIndexOf(formerComponents.head) match {
      case -1 if formerComponents.length == 1 => // No match & one component: relative to parent class ("org.example.Current" → "org.example.Old")
        s"${currentClassComponents.dropRight(1).mkString(".")}.$formerClassName"
      case -1 => // No match: treat as absolute path
        formerClassName
      case 0 => // Match at root (like "org"): treat as absolute path
        formerClassName
      case i => // Relative path (like "example.Current"): prepend the matched prefix
        s"${currentClassComponents.take(i).mkString(".")}.$formerClassName"
    }

    toInternalClassName(formerFqn, currentClassNames.head)
  }

}
