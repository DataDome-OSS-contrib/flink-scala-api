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

  /** Resolve a former class name to its fully qualified internal name based on the current class.
    *
    * Interpret `from` parameter relative to the current class's package and nesting hierarchy, converting it to the JVM
    * internal format where nested classes are separated by `$` instead of dots.
    *
    * Note, 'from' relies on the conventions to name:
    *  - first package with a lowercase first letter (e.g., `org` not `Org`)
    *  - classes with a capital letter. If it's not the case, a `$` has to be used as separator between inner classes
    *
    * @param from
    *   The former class name, which can be:
    *   - A simple name: `"OldName"`
    *   - A relative path: `"Parent.OldName"` or `"api.oldPackage.OldName"` or `"api.lowerClass$OldName"`
    *   - An absolute path: `"org.example.OldName"`
    * @param currentClass
    *   A class currently existing, used as context for resolving relative names
    * @return
    *   The fully qualified internal former class name (e.g., `org.example.Outer$Inner`)
    * @throws FlinkRuntimeException
    *   If `from` is empty, contains leading/consecutive/trailing dots
    */
  def resolveFormerClassName(from: String, currentClass: Class[_]): String = {
    if (from.isEmpty || from.matches("^\\..*|.*\\.\\..*|.*\\.$")) {
      throw new FlinkRuntimeException(s"Class name '$from' in from parameter is malformed.")
    }

    /** Convert dot-separated path to JVM internal class name format: `package.Outer.Inner` → `package.Outer$Inner` */
    def toInternalClassName(path: String, firstClass: String): String = {
      // Find the first uppercase component (first class), then replace remaining dots with `$`.
      val components      = path.split('.')
      val firstClassIndex = components.indexOf(firstClass)
      val firstUpperIndex = if (firstClassIndex >= 0) firstClassIndex else components.indexWhere(_.head.isUpper)
      if (firstUpperIndex >= 0 && firstUpperIndex < components.length - 1) {
        val packageAndFirstClass = components.take(firstUpperIndex + 1).mkString(".")
        val nestedClasses        = components.drop(firstUpperIndex + 1).mkString("$")
        s"$packageAndFirstClass$$$nestedClasses"
      } else {
        path
      }
    }

    /** Walks the declaring class hierarchy and returns a list of class names */
    def toClassNames(clazz: Class[_]): List[String] =
      if (clazz == null) List.empty else toClassNames(clazz.getDeclaringClass) :+ clazz.getSimpleName.stripSuffix("$")

    val firstFormerComponent   = from.split('.').head
    val currentClassNames      = toClassNames(currentClass)
    val currentClassComponents = currentClass.getPackageName.split('.') ++ currentClassNames

    // Match the first component of former class name against the current class hierarchy and resolve accordingly
    val formerClassName = currentClassComponents.lastIndexOf(firstFormerComponent) match {
      case 0 => // Match at root (like "org"): treat as absolute path
        from
      case -1 if firstFormerComponent.head.isLower => // No match & lowercase first component: treat as absolute path
        from
      case -1 => // No match & uppercase: treat as relative to current class ("org.example.Current" → "org.example.Old")
        s"${currentClassComponents.dropRight(1).mkString(".")}.$from"
      case i => // Relative path (like "example.Current"): prepend the matched prefix
        s"${currentClassComponents.take(i).mkString(".")}.$from"
    }

    toInternalClassName(formerClassName, currentClassNames.head)
  }

}
