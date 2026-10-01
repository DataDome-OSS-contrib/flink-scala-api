package org.apache.flinkx.api.evolution

import org.apache.flink.shaded.asm9.org.objectweb.asm.ClassReader

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.jar.JarFile
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Finds the companions extending [[Evolving]] in the jars and class directories a class loader loads from.
  *
  * Only the companions of the job need to be found, and they are all in the jars of its class loader: a companion
  * extends [[Evolving]], which the parent loaders can't see. Flink hands a wrapper of that loader, whose URLs are the
  * ones of the loader it wraps. The class path of the JVM stands in when there is none: a job running in a MiniCluster,
  * or a test, has its classes on the class path rather than in a jar of its own.
  *
  * A class file is read only when it names [[Evolving]] in its constant pool, which a companion extending it does, and
  * only its header is parsed then.
  */
private[evolution] object CompanionScan {

  private val EvolvingName  = classOf[Evolving[_]].getName.replace('.', '/')
  private val EvolvingBytes = EvolvingName.getBytes(StandardCharsets.UTF_8)

  /** Binary names of the classes extending [[Evolving]] found in what the given class loader loads from. */
  def companionsOf(classLoader: ClassLoader): Seq[String] =
    rootsOf(classLoader).flatMap(companionsIn).distinct

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

  private def companionsIn(root: Path): Seq[String] =
    if (Files.isDirectory(root)) companionsInDirectory(root)
    else if (Files.isRegularFile(root)) companionsInJar(root)
    else Seq.empty

  private def companionsInDirectory(root: Path): Seq[String] =
    Using.resource(Files.walk(root)) { paths =>
      paths
        .iterator()
        .asScala
        .filter(path => isCompanionFile(path.toString) && Files.isRegularFile(path))
        .flatMap(path => companionNamed(Files.readAllBytes(path)))
        .toList
    }

  private def companionsInJar(jar: Path): Seq[String] =
    Using.resource(new JarFile(jar.toFile)) { file =>
      file
        .entries()
        .asScala
        .filter(entry => !entry.isDirectory && isCompanionFile(entry.getName))
        .flatMap(entry => companionNamed(Using.resource(file.getInputStream(entry))(_.readAllBytes())))
        .toList
    }

  /** A companion object is compiled to a class named after its type with a trailing `$`. */
  private def isCompanionFile(name: String): Boolean = name.endsWith("$.class")

  /** The binary name of the given class file if it extends [[Evolving]]. */
  private def companionNamed(classFile: Array[Byte]): Option[String] =
    if (!contains(classFile, EvolvingBytes)) None
    else {
      val reader = new ClassReader(classFile)
      // A trait compiles to an interface, that the header of the class lists
      if (reader.getInterfaces.contains(EvolvingName)) Some(reader.getClassName.replace('/', '.')) else None
    }

  private def contains(haystack: Array[Byte], needle: Array[Byte]): Boolean = {
    val last = haystack.length - needle.length
    var i    = 0
    while (i <= last) {
      var j = 0
      while (j < needle.length && haystack(i + j) == needle(j)) j += 1
      if (j == needle.length) return true
      i += 1
    }
    false
  }

}
