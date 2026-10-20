package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.evolution.EvolutionTest.AddedFieldWithoutAnnotation
import org.apache.flinkx.api.evolution.FieldEvolution.Former
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** What `Evolutions[T]` builds from the annotations of an ADT: the class names its data was written under, and the
  * evolution of the data written at each version, which plans how the former fields evolve, or why they can't.
  */
class EvolutionMacroTest extends AnyFlatSpec with Matchers {

  import org.apache.flinkx.api.evolution.EvolutionMacroTest._
  import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pet

  /** The class names an ADT declares. */
  private def namesOf(evolutions: Evolutions[_]): Set[String] = evolutions.declarations.map(_.formerClassName).toSet

  /** The former field each current field reads, none when its lineage is broken: what the evolutions do to a former
    * schema, which compares the rules themselves and the order they run in.
    */
  private def lineageOf(evolution: Evolution[_], formerFields: Array[String]): Either[Seq[String], Seq[Option[Int]]] =
    evolution
      .plan(formerFields)
      .map(_.toSeq.map {
        case Former(index) => Some(index)
        case _             => None
      })

  // Schema of Click v0, as recorded in its snapshot
  private val ClickV0FieldNames = Array("a", "inFileClicks", "fieldNotInFile", "identifier", "b")

  /** Every failure the plan of the evolutions `clazz` declares from version 0 reports on Click v0. */
  private def planFailures(clazz: Class[_]): Seq[String] =
    evolutionOf(clazz).plan(ClickV0FieldNames).swap.getOrElse(Seq.empty)

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
    lineageOf(declaration.versions(1), Array("id", "count")) shouldBe Right(Seq(Some(0), Some(1), None))
    val version1 = declaration.versions(1).asInstanceOf[Evolution[Probe]]
    version1.restore(Probe("id", "3", "default")) shouldBe Probe("id", "3", "default1")
    declaration.versions(2).isAvoidable(Array("id", "count", "label")) shouldBe true
  }

  // The rules themselves: a rename, a transform with its mapper, a deletion and an addition with its default value
  it should "plan the field evolutions migrating the data written under a former name" in {
    val formerName = classOf[Probe].getName.replace("Probe", "FormerProbe")
    val evolution  = Evolutions.get[Probe](formerName, 0, getClass.getClassLoader)

    // The transformed field loses its lineage by design, as does the added one
    lineageOf(evolution, Array("formerId", "count", "gone")) shouldBe Right(Seq(Some(0), None, None))
  }

  // The mappers live in the companion, hidden from the outside: the declaration is read from the companion itself
  it should "apply the mappers of the companion" in {
    evolutionOf(classOf[Probe], 1).restore(Probe("id", "3", "default")) shouldBe Probe("id", "3", "default1")
  }

  // The field names are checked by the plan, when the schema compatibility is resolved, and no longer on every
  // record: these assert the guidance the user gets, which resolveSchemaCompatibility only logs
  it should "report an unused former field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongFieldNotUsed]) shouldBe Seq(
      "'b' former field not used. Use @deletedFields(since=<version>,\"b\") annotation to indicate its deletion"
    )
  }

  it should "report every offending field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongSeveralFields]) shouldBe Seq(
      "'missing' field missing. Use @added(since=<version>) annotation to indicate it has been added",
      "'identifier' former field not used. Use @deletedFields(since=<version>,\"identifier\") annotation to indicate its deletion",
      "'b' former field not used. Use @deletedFields(since=<version>,\"b\") annotation to indicate its deletion"
    )
  }

  it should "report a missing current field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongMissingField]) shouldBe Seq(
      "'missingField' field missing. Use @added(since=<version>) annotation to indicate it has been added"
    )
  }

  // An unversioned ADT declares nothing: its no-op evolution still reports what its plan would hit
  it should "report a field added without annotation to an unversioned case class" in {
    evolutionOf(classOf[AddedFieldWithoutAnnotation]).plan(Array("a")).swap shouldBe Right(
      Seq("'b' field missing. Use @added(since=<version>) annotation to indicate it has been added")
    )
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
