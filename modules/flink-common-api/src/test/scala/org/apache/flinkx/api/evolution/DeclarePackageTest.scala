package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.pkg.{PackageDeclaration, Nested, Order}
import org.apache.flinkx.api.evolution.pkg.sub.Item
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Checks `Declare.declarePackage` declares every versioned ADT of a package, wherever it is nested. */
class DeclarePackageTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TestSources = new java.io.File(sys.props("flinkx.test.sources"))

  override protected def beforeEach(): Unit = Evolutions.reset()

  private def formerNameOf[T](clazz: Class[T], formerName: String): Option[Class[_]] =
    Evolutions.find[T](clazz.getPackageName + "." + formerName, 0, clazz.getClassLoader).map(_.currentClass)

  it should "declare a top-level ADT, a nested one and one of a sub-package" in {
    Declare.declarePackage("org.apache.flinkx.api.evolution.pkg")

    withClue("top-level:")(formerNameOf(classOf[Order], "Purchase") shouldBe Some(classOf[Order]))
    withClue("nested in an object:")(
      Evolutions.get(classOf[Nested.Customer], 1).currentClass shouldBe classOf[Nested.Customer]
    )
    withClue("in a sub-package:")(formerNameOf(classOf[Item], "Article") shouldBe Some(classOf[Item]))
  }

  it should "declare the evolutions themselves, not only the classes" in {
    Declare.declarePackage("org.apache.flinkx.api.evolution.pkg")

    // The v0 data still carries the field deleted in v1, which the declared evolution drops
    val evolution = Evolutions.get(classOf[Nested.Customer], 0)
    withClue("the deleted field must be declared:")(
      evolution.dryRun(Array("name", "gone")).map(_.toSeq) shouldBe Right(Seq(Some(0)))
    )
  }

  // The net under declarePackage: an ADT no provider declares must be reported while building, not at restore time
  it should "report the versioned ADTs no provider declares" in {
    val undeclared = EvolutionsCheck.undeclaredIn(TestSources, "org.apache.flinkx.api.evolution.pkg")

    // Read from the sources, so an ADT is named by its package and its own name, whatever object encloses it
    undeclared should contain theSameElementsAs Seq(
      classOf[Order].getName,
      s"${classOf[Order].getPackageName}.Customer",
      classOf[Item].getName
    )
  }

  it should "report nothing once the package is declared" in {
    Declare.declarePackage("org.apache.flinkx.api.evolution.pkg")

    EvolutionsCheck.undeclaredIn(TestSources, "org.apache.flinkx.api.evolution.pkg") shouldBe empty
  }

  // Two modules may legitimately declare the same shared ADT, so a declaration already registered must be a no-op
  it should "declare a package twice without piling up windows" in {
    Declare.declarePackage("org.apache.flinkx.api.evolution.pkg")
    val declaredOnce = Evolutions.declaredEvolutions(classOf[Order].getClassLoader).view.mapValues(_.length).toMap

    Declare.declarePackage("org.apache.flinkx.api.evolution.pkg")

    Evolutions.declaredEvolutions(classOf[Order].getClassLoader).view.mapValues(_.length).toMap shouldBe declaredOnce
  }

  // The check reads the sources and declarePackage the symbols: their agreement is what keeps the check honest
  it should "report exactly what declarePackage declares" in {
    val before = EvolutionsCheck.undeclaredIn(TestSources, "org.apache.flinkx.api.evolution.pkg")

    Declare.declarePackage("org.apache.flinkx.api.evolution.pkg")

    before should not be empty
    EvolutionsCheck.undeclaredIn(TestSources, "org.apache.flinkx.api.evolution.pkg") shouldBe empty
  }

  // A provider sitting at the root of a model declares it whole, without naming its own package
  it should "declare the package the call is written in" in {
    PackageDeclaration.declare()

    EvolutionsCheck.undeclaredIn(TestSources, "org.apache.flinkx.api.evolution.pkg") shouldBe empty
  }

}
