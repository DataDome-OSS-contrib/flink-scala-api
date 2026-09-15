package org.apache.flinkx.sbt

import sbt.Keys._
import sbt._
import sbt.internal.inc.{RawCompiler, ScalaInstance}
import xsbti.compile.ClasspathOptionsUtil

import java.nio.file.{Files, Path}

/** Generates, after each compilation, the provider declaring the evolutions of the ADTs of the module.
  *
  * The evolutions of an ADT are read from its annotations when its type information is derived, which happens where
  * the job graph is built. A TaskManager never derives anything: it needs the declarations to travel with the jar,
  * which this plugin arranges by listing a generated provider in `META-INF/services`.
  *
  * Scope is the output of the module itself: every module declaring versioned ADTs enables the plugin, and the service
  * files of the jars merge naturally at assembly time.
  */
object FlinkxEvolutionsPlugin extends AutoPlugin {

  override def trigger = noTrigger

  object autoImport {
    val evolutionsProviderPackage = settingKey[String]("Package of the generated evolutions provider")
    val evolutionsProviderClass   = settingKey[String]("Class name of the generated evolutions provider")
    val evolutionsGenerate        = taskKey[Seq[Path]]("Generates and compiles the evolutions provider of the module")
  }
  import autoImport._

  override def projectSettings: Seq[Setting[_]] = Seq(
    evolutionsProviderPackage := "flinkx.evolutions.generated",
    evolutionsProviderClass := s"${name.value.split("[^A-Za-z0-9]").filter(_.nonEmpty).map(_.capitalize).mkString}EvolutionsProvider",
    evolutionsGenerate := generate.value,
    // Runs after the compilation whose classes it reads, which no source generator can do
    Compile / products := {
      val compiled = (Compile / products).value
      evolutionsGenerate.value
      compiled
    }
  )

  private def generate: Def.Initialize[Task[Seq[Path]]] = Def.task {
    val log            = streams.value.log
    val classes   = (Compile / classDirectory).value.toPath
    val classpath = (Compile / dependencyClasspath).value.map(_.data.toPath)
    val adtNames  = EvolutionsScan.scan(classes, classpath)

    if (adtNames.isEmpty) {
      log.debug(s"[flinkx] no @version annotated ADT in $classes")
      Seq.empty
    } else {
      val packageName   = evolutionsProviderPackage.value
      val className     = evolutionsProviderClass.value
      val generatedFile = (Compile / target).value.toPath.resolve(s"flinkx-evolutions/$className.scala")
      Files.createDirectories(generatedFile.getParent)
      Files.write(generatedFile, EvolutionsScan.providerSource(packageName, className, adtNames).getBytes("UTF-8"))
      log.info(s"[flinkx] declaring the evolutions of ${adtNames.size} ADT(s) in $packageName.$className")

      val compiler = new RawCompiler(scalaInstance.value, ClasspathOptionsUtil.auto, log)
      compiler(
        Seq(generatedFile),
        classes +: classpath,
        classes,
        (Compile / scalacOptions).value
      )

      val service = classes.resolve(s"META-INF/services/${EvolutionsScan.ProviderService}")
      Files.createDirectories(service.getParent)
      Files.write(service, s"$packageName.$className\n".getBytes("UTF-8"))
      Seq(generatedFile, service)
    }
  }

}
