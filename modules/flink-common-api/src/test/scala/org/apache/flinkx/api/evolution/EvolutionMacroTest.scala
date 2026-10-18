package org.apache.flinkx.api.evolution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Checks the evolutions a companion extending `Evolving` builds register exactly what the annotations describe. */
class EvolutionMacroTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  import org.apache.flinkx.api.evolution.EvolutionMacroTest._
  import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pet

  override protected def beforeEach(): Unit = Evolutions.reset()

  /** The class names an ADT declares. */
  private def namesOf(evolutions: Evolutions[_]): Set[String] = evolutions.declarations.map(_.formerClassName).toSet

  /** What the evolutions do to a former schema, which compares the rules themselves and the order they run in. */
  private def dryRunOf[T](
      formerName: String,
      formerVersion: Int,
      formerFields: Array[String]
  ): Option[Either[Seq[String], Seq[Option[Int]]]] =
    Option(Evolutions.get[T](formerName, formerVersion, getClass.getClassLoader))
      .map(_.dryRun(formerFields).left.map(_.map(_.getMessage).toSeq).map(_.toSeq))

  it should "declare the former and the current name of a case class" in {
    namesOf(Probe.evolutions) shouldBe Set(
      classOf[Probe].getName,
      classOf[Probe].getName.replace("Probe", "FormerProbe"),
      // Declared deleted, so it resolves to the marker, whether it was a class or an object
      classOf[Probe].getName.replace("Probe", "GoneType"),
      classOf[Probe].getName.replace("Probe", "GoneType$")
    )
  }

  it should "declare a sealed trait and the former name it was renamed from" in {
    namesOf(Pet.evolutions) should contain allOf (
      classOf[Pet].getName,
      classOf[Pet].getName.replace("Pet", "Animal")
    )
  }

  // Each version holds only what migrates the data written at it: the addition of version 2 and the mapper for version 1
  it should "compute the evolution of the data written at each version of a class name" in {
    val declaration = Probe.evolutions.declarations.find(_.formerClassName == classOf[Probe].getName).get

    declaration.versions.map(_.formerVersion) shouldBe Seq(0, 1, 2)
    declaration.versions(1).dryRun(Array("id", "count")).map(_.toSeq) shouldBe Right(Seq(Some(0), Some(1), None))
    val version1 = declaration.versions(1).asInstanceOf[Evolution[Probe]]
    version1.postEvolve(Probe("id", "3", "default")) shouldBe Probe("id", "3", "default1")
    declaration.versions(2).isAvoidable(Array("id", "count", "label")) shouldBe true
  }

  // The rules themselves: a rename, a transform with its mapper, a deletion and an addition with its default value
  it should "apply the same field evolutions as the derivation" in {
    val formerName   = classOf[Probe].getName.replace("Probe", "FormerProbe")
    val formerFields = Array("formerId", "count", "gone")

    val failures = dryRunOf[Probe](formerName, 0, formerFields)
    withClue("the former name must resolve:")(failures shouldBe defined)
    // The transformed field loses its lineage by design, as does the added one
    withClue("every field of the current schema must be filled:")(
      failures shouldBe Some(Right(Seq(Some(0), None, None)))
    )
  }

  // The mappers live in the companion, hidden from the outside: the declaration is read from the companion itself
  it should "apply the mappers of the companion" in {
    val evolution = Evolutions.get(classOf[Probe], 1)

    evolution.postEvolve(Probe("id", "3", "default")) shouldBe Probe("id", "3", "default1")
  }

}

object EvolutionMacroTest {

  /* Probe v0
  case class FormerProbe(formerId: String, count: Int, gone: String)
   */

  @version(2)
  @renamed(since = 1, "FormerProbe")
  @deletedFields(since = 1, "gone")
  @deletedClasses(since = 1, throwOnInstance = false, "GoneType")
  @postEvolution(Probe.bump)
  case class Probe(
      @renamed(since = 1, "formerId") id: String,
      @transformed(since = 1, Probe.intToString) count: String,
      @added(since = 2) label: String = "default"
  )

  object Probe extends Evolving[Probe] {
    val evolutions = Evolutions[Probe]
    // Visible from the annotations of the class, and from nowhere else
    private[EvolutionMacroTest] def intToString(i: Int): String             = i.toString
    private[EvolutionMacroTest] def bump(version: Int, probe: Probe): Probe = probe.copy(label = probe.label + version)
  }

}
