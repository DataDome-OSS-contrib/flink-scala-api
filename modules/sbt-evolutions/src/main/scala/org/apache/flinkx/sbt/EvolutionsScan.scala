package org.apache.flinkx.sbt

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Finds the ADTs a module declares evolutions for, and writes the provider declaring them.
  *
  * Kept free of sbt so that it can be tested on its own: the plugin is only the glue running it after compilation.
  */
object EvolutionsScan {

  val VersionAnnotation = "org.apache.flinkx.api.version"
  val ProviderService   = "org.apache.flinkx.api.evolution.EvolutionsProvider"

  /** Binary names of the classes of `classDirectory` annotated with `@version`, sorted for a stable output.
    *
    * Classes are loaded without initializing them, with a loader seeing the module and its dependencies: a `@version`
    * is a Java annotation retained at runtime, so it is read from the class itself rather than from any source.
    */
  def scan(classDirectory: Path, classpath: Seq[Path]): Seq[String] =
    if (!Files.isDirectory(classDirectory)) Seq.empty
    else {
      val urls   = (classDirectory +: classpath).map(_.toUri.toURL).toArray[URL]
      val parent = getClass.getClassLoader
      Using.resource(new URLClassLoader(urls, parent)) { loader =>
        val annotation =
          try loader.loadClass(VersionAnnotation).asInstanceOf[Class[_ <: java.lang.annotation.Annotation]]
          catch { case _: ClassNotFoundException => null }
        if (annotation == null) Seq.empty
        else
          Using.resource(Files.walk(classDirectory)) { paths =>
            paths
              .iterator()
              .asScala
              .filter(path => path.toString.endsWith(".class"))
              .map(path => binaryName(classDirectory, path))
              .filter(isDeclarable)
              .filter(name => isAnnotated(loader, annotation, name))
              .toSeq
              .sorted
          }
      }
    }

  private def binaryName(classDirectory: Path, classFile: Path): String =
    classDirectory.relativize(classFile).toString.stripSuffix(".class").replace(File.separatorChar, '.')

  /** Anonymous, local and synthetic classes have no name to declare them by. */
  private def isDeclarable(binaryName: String): Boolean =
    !binaryName.contains("$$") && !binaryName.split('$').exists(part => part.isEmpty || part.forall(_.isDigit))

  private def isAnnotated(
      loader: ClassLoader,
      annotation: Class[_ <: java.lang.annotation.Annotation],
      binaryName: String
  ): Boolean =
    try Class.forName(binaryName, false, loader).getAnnotation(annotation) != null
    catch { case _: Throwable => false }

  /** Source name of an ADT, as the generated provider has to name it: a binary name separates the enclosing types and
    * the module classes with `$`.
    */
  def sourceName(binaryName: String): String =
    if (binaryName.endsWith("$")) s"${binaryName.dropRight(1).replace('$', '.')}.type"
    else binaryName.replace('$', '.')

  /** The provider declaring every given ADT, as Scala source. */
  def providerSource(packageName: String, className: String, adtNames: Seq[String]): String = {
    val declarations = adtNames.map(name => s"    org.apache.flinkx.api.evolution.Declare.declare[${sourceName(name)}]")
    s"""package $packageName
       |
       |/** Generated: declares the evolutions of the ADTs of this module, so that they reach every JVM restoring them. */
       |final class $className extends org.apache.flinkx.api.evolution.EvolutionsProvider {
       |  override def declare(): Unit = {
       |${declarations.mkString("\n")}
       |  }
       |}
       |""".stripMargin
  }

}
