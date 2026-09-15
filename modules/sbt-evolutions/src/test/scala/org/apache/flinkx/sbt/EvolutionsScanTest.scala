package org.apache.flinkx.sbt

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionsScanTest extends AnyFlatSpec with Matchers {

  it should "name a top level ADT" in {
    EvolutionsScan.sourceName("com.acme.Order") shouldBe "com.acme.Order"
  }

  it should "name an ADT nested in an object" in {
    EvolutionsScan.sourceName("com.acme.Domain$Order") shouldBe "com.acme.Domain.Order"
  }

  it should "name a case object" in {
    EvolutionsScan.sourceName("com.acme.Domain$Empty$") shouldBe "com.acme.Domain.Empty.type"
  }

  it should "declare every scanned ADT" in {
    val source = EvolutionsScan.providerSource("flinkx.generated", "Provider", Seq("com.acme.Order", "com.acme.D$Item"))

    source should include("package flinkx.generated")
    source should include("final class Provider extends org.apache.flinkx.api.evolution.EvolutionsProvider")
    source should include("Declare.declare[com.acme.Order]")
    source should include("Declare.declare[com.acme.D.Item]")
  }

  it should "find nothing in a directory that does not exist" in {
    EvolutionsScan.scan(java.nio.file.Paths.get("target/does-not-exist"), Seq.empty) shouldBe empty
  }

  // Scans the real output of the library, whose test fixtures carry the annotation this looks for
  it should "find the annotated ADTs of a compiled module" in {
    // The working directory is the root of the build when run from sbt, the module when run from an IDE
    val target = Seq("modules/flink-common-api/target", "../flink-common-api/target")
      .map(java.nio.file.Paths.get(_).resolve("flink1-jvm-2.13"))
      .find(path => java.nio.file.Files.isDirectory(path.resolve("test-classes")))
    assume(target.isDefined, "the library has to be compiled for this check")
    val classes = target.get.resolve("test-classes")
    val library = target.get.resolve("classes")

    val found = EvolutionsScan.scan(classes, Seq(library))

    found should contain("org.apache.flinkx.api.EvolutionTest$Dog")
    found should contain("org.apache.flinkx.api.evolution.EvolutionRenamedTest$Pet")
    withClue("an unversioned fixture must not be declared:")(
      found shouldNot contain("org.apache.flinkx.api.EvolutionTest$AddedFieldWithoutAnnotation")
    )
  }

}
