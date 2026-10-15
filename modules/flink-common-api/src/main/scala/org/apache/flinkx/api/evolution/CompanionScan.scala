package org.apache.flinkx.api.evolution

import org.apache.flink.shaded.asm9.org.objectweb.asm.ClassReader

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path, Paths}
import java.util.jar.JarFile
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Finds the companions extending [[Evolving]] in the jars and class directories a class loader loads from.
  *
  * Only the companions of the job need to be found, and they are all in the jars of its class loader: a companion
  * extends [[Evolving]], which the parent loaders can't see. Flink hands a wrapper of that loader, whose URLs are the
  * ones of the loader it wraps. The class path of the JVM stands in when there is none: a job running in a MiniCluster,
  * or a test, has its classes on the class path rather than in a jar of its own.
  *
  * A companion may extend [[Evolving]] through a trait or a class of its own: only the header of a class file is
  * parsed, and its supertypes are walked up, each read once per scan, skipping the JDK and the Scala library.
  */
private[evolution] object CompanionScan {

  private val EvolvingName = classOf[Evolving[_]].getName.replace('.', '/')

  // Packages that never extend Evolving, whose types are not read
  private val LibraryPrefixes = Seq("java/", "javax/", "jdk/", "scala/")

  /** Binary names of the classes extending [[Evolving]] found in what the given class loader loads from. */
  def companionsOf(classLoader: ClassLoader): Seq[String] = {
    val supertypes = new Supertypes(classLoader)
    rootsOf(classLoader).flatMap(companionsIn(_, supertypes)).distinct
  }

  private def rootsOf(classLoader: ClassLoader): Seq[Path] = {
    val urls = classLoader match {
      case loader: URLClassLoader => loader.getURLs.toSeq.flatMap(pathOf)
      case _                      => Seq.empty
    }
    if (urls.nonEmpty) urls
    else System.getProperty("java.class.path", "").split(File.pathSeparator).toSeq.filter(_.nonEmpty).map(Paths.get(_))
  }

  private def pathOf(url: URL): Option[Path] =
    if (url.getProtocol == "file") Some(Paths.get(url.toURI)) else None

  private def companionsIn(root: Path, supertypes: Supertypes): Seq[String] =
    if (Files.isDirectory(root)) companionsInDirectory(root, supertypes)
    else if (Files.isRegularFile(root)) companionsInJar(root, supertypes)
    else Seq.empty

  private def companionsInDirectory(root: Path, supertypes: Supertypes): Seq[String] =
    Using.resource(Files.walk(root)) { paths =>
      paths
        .iterator()
        .asScala
        .filter(path => isCompanionFile(path.toString) && Files.isRegularFile(path))
        .flatMap(path => companionNamed(Files.readAllBytes(path), supertypes))
        .toList
    }

  private def companionsInJar(jar: Path, supertypes: Supertypes): Seq[String] =
    Using.resource(new JarFile(jar.toFile)) { file =>
      file
        .entries()
        .asScala
        .filter(entry => !entry.isDirectory && isCompanionFile(entry.getName))
        .flatMap(entry => companionNamed(Using.resource(file.getInputStream(entry))(_.readAllBytes()), supertypes))
        .toList
    }

  /** A companion object is compiled to a class named after its type with a trailing `$`. */
  private def isCompanionFile(name: String): Boolean = name.endsWith("$.class")

  /** The binary name of the given class file if it extends [[Evolving]]. */
  private def companionNamed(classFile: Array[Byte], supertypes: Supertypes): Option[String] =
    headerOf(classFile).filter(supertypes.extendsEvolving).map(_.getClassName.replace('/', '.'))

  /** The header of the given class file, none if its version is more recent than the ones ASM reads. */
  private def headerOf(classFile: Array[Byte]): Option[ClassReader] =
    try Some(new ClassReader(classFile))
    catch { case _: IllegalArgumentException => None }

  /** Tells whether a class extends [[Evolving]] through its supertypes, read from the class loader once per scan. */
  private final class Supertypes(classLoader: ClassLoader) {

    private val known = mutable.Map.empty[String, Boolean]

    def extendsEvolving(header: ClassReader): Boolean =
      (Option(header.getSuperName) ++ header.getInterfaces).exists(isEvolving)

    private def isEvolving(name: String): Boolean =
      name == EvolvingName || !LibraryPrefixes.exists(name.startsWith) && known.getOrElse(name, resolve(name))

    private def resolve(name: String): Boolean = {
      known(name) = false // Stops the walk on a cycle, which an inconsistent class path may hold
      val extending = classFile(name).flatMap(headerOf).exists(extendsEvolving)
      known(name) = extending
      extending
    }

    private def classFile(name: String): Option[Array[Byte]] =
      Option(classLoader.getResourceAsStream(s"$name.class")).map(in => Using.resource(in)(_.readAllBytes()))

  }

}
