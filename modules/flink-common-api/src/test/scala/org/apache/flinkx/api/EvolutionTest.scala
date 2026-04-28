package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.EvolutionTest._
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.Evolutions
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  /*
  it should "serialize Click v0" in {
    val event: Click = Click("a", List(ClickEvent("2021-01-01", 1), ClickEvent("2022-02-02", 2)), 5, "id1", "b")
    serializeToFile("Click-v0", event)
  }
   */

  it should "deserialize Click v0 to Click v3" in {
    val expected: Click = Click("id1", List(ClickAction("1", "2021-01-01"), ClickAction("2", "2022-02-02")), 25, "5")
    testDeserializeFromFile("Click-v0", expected)
  }

  /*
  it should "serialize Event v0" in {
    val event: Event = View(123456789)
    serializeToFile("Event-v0", event)
  }
   */

  it should "deserialize Event v0 to Action v1" in {
    val expected: Action = Web(123456790)
    testDeserializeFromFile("Event-v0", expected)
  }

  // Error handling

  it should "throw when @version has a wrong current version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongCurrentVersion]]
    }
    exception.getMessage shouldBe "Current version of class org.apache.flinkx.api.EvolutionTest$WrongCurrentVersion must be positive, got @version(0)"
  }

  it should "throw when @added is on a case class" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongAddedOnCaseClass]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongAddedOnCaseClass"
  }

  it should "throw when @renamed is on a case class without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongRenamedOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnCaseClassWithoutVersion without version"
  }

  it should "allow when @renamed is on a sealed trait subtype without version" in {
    implicitly[TypeInformation[CorrectRenamedOnSealedTraitSubtypeWithoutVersion]]
    implicitly[TypeInformation[CorrectRenamedOnCaseClassWithoutVersion]]
  }

  it should "throw when @transformed is on a case class" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongTransformedOnCaseClass]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnCaseClass"
  }

  it should "throw when @deletedElements is on a case class without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongDeletedElementsOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedElements(1,\"a\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedElementsOnCaseClassWithoutVersion without version"
  }

  it should "throw when @version is on a case class field" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongVersionOnField]]
    }
    exception.getMessage shouldBe "@version(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongVersionOnField.a"
  }

  it should "throw when @added is on a case class field without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongAddedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongAddedOnFieldWithoutVersion without version"
  }

  it should "throw when @added is on a case class field without default value" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongAddedOnFieldWithoutDefaultValue]]
    }
    exception.getMessage shouldBe "'a' added field in class org.apache.flinkx.api.EvolutionTest$WrongAddedOnFieldWithoutDefaultValue must have a default value"
  }

  it should "throw when @renamed is on a case class field without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongRenamedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"a\") annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnFieldWithoutVersion without version"
  }

  it should "throw when @transformed is on a case class field without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongTransformedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnFieldWithoutVersion without version"
  }

  it should "throw when @deletedElements is on a case class field" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongDeletedElementsOnField]]
    }
    exception.getMessage shouldBe "@deletedElements(1,\"a\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedElementsOnField.a"
  }

  it should "throw when @added is on a sealed trait" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongAddedOnSealedTrait]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongAddedOnSealedTrait"
  }

  it should "throw when @renamed is on a sealed trait without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongRenamedOnSealedTraitWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongRenamedOnSealedTraitWithoutVersion without version"
  }

  it should "throw when @transformed is on a sealed trait" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongTransformedOnSealedTrait]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongTransformedOnSealedTrait"
  }

  it should "throw when @deletedElements is on a sealed trait without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongDeletedElementsOnSealedTraitWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedElements(1,\"A\") annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongDeletedElementsOnSealedTraitWithoutVersion without version"
  }

  it should "throw when @added is on a sealed trait subtype" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongAddedOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongAddedOnSubtype$ without version"
  }

  it should "throw when @renamed is on a sealed trait subtype without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongRenamedOnSealedTraitSubtypeWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnSubtypeWithoutVersion$ without version"
  }

  it should "throw when @transformed is on a sealed trait subtype" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongTransformedOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnSubtype$ without version"
  }

  it should "throw when @deletedElements is on a sealed trait subtype" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongDeletedElementsOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@deletedElements(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedElementsOnSubtype$ without version"
  }

  it should "allow when @deletedElements is on a sealed trait subtype being itself a sealed trait" in {
    implicitly[TypeInformation[CorrectDeletedElementsOnSealedTraitSubtype]]
  }

  it should "throw field not found when deserializing Click v0 with wrong added field" in {
    val expected  = WrongAddedField("123456789")
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot add 'a'. Field already exists in class org.apache.flinkx.api.EvolutionTest$WrongAddedField. Existing fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong renamed field" in {
    val expected  = WrongRenamedField("123456789")
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot rename 'wrongFieldName'. Field not found in class org.apache.flinkx.api.EvolutionTest$WrongRenamedField. Available fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong transformed field" in {
    val expected  = WrongTransformedField("123456789")
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot transform 'wrongFieldName'. Field not found in class org.apache.flinkx.api.EvolutionTest$WrongTransformedField. Available fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong deleted field" in {
    val expected  = WrongDeletedField("123456789")
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot delete 'wrongFieldName'. Field not found in class org.apache.flinkx.api.EvolutionTest$WrongDeletedField. Available fields: [\"a\"]"
  }

  it should "throw field not used when deserializing Click v0 with extra field" in {
    val expected  = WrongFieldNotUsed("123456789")
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "'b' field not used to instantiate class org.apache.flinkx.api.EvolutionTest$WrongFieldNotUsed. Use @deletedElements(since=<version>,\"b\") annotation to indicate it has been deleted"
  }

  it should "throw missing field when deserializing Click v0 with missing field to instantiate" in {
    val expected  = WrongMissingField("123456789", "is missing")
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "'missingField' field missing to instantiate class org.apache.flinkx.api.EvolutionTest$WrongMissingField. Use @added(since=<version>) annotation to indicate it has been added"
  }

}

object EvolutionTest {

  // API demonstration

  /* click-v0
  case class Click(
      a: String,
      inFileClicks: List[ClickEvent],
      fieldNotInFile: Int,
      identifier: String,
      b: String
  )

  case class ClickEvent(
      date: String,
      sessionId: Int
  )
   */

  @version(3)
  @deletedElements(since = 1, "a")
  @deletedElements(since = 2, "b")
  @postDeserialize(updateClick)
  case class Click(
      @renamed(since = 2, "identifier") id: String,
      inFileClicks: List[ClickAction],
      @added(since = 3) fieldInFile: Int = 1,
      @transformed(since = 1, mapIntToString) fieldNotInFile: String
  )

  @version(1)
  @renamed(since = 1, "ClickEvent")
  case class ClickAction(
      @renamed(since = 1, "sessionId") @transformed(since = 1, mapIntToString) id: String,
      date: String
  )

  def mapIntToString(a: Int): String   = a.toString
  def updateClick(click: Click): Click = click.copy(fieldInFile = click.fieldNotInFile.toInt * 5)

  /* event-v0
  sealed trait Event
  case class View(ts: Long) extends Event
  case class Purchase(price: Double) extends Event
   */

  @version(1)
  @renamed(since = 1, "Event")
  @deletedElements(since = 1, "Purchase")
  @postDeserialize(updateAction)
  sealed trait Action

  case object Login extends Action

  @renamed(since = 1, "View")
  case class Web(ts: Long) extends Action

  case class Cart(items: Int) extends Action

  def updateAction(action: Action): Action = action match {
    case Web(ts) => Web(ts + 1)
    case Cart(_) => Login
    case e @ _   => e
  }

  // Error handling

  @version(0)
  case class WrongCurrentVersion()

  @version(1)
  @added(since = 1)
  case class WrongAddedOnCaseClass()

  @renamed(since = 1, "A")
  case class WrongRenamedOnCaseClassWithoutVersion()

  @version(1)
  sealed trait CorrectRenamedOnSealedTraitSubtypeWithoutVersion
  @renamed(since = 1, "A")
  case class CorrectRenamedOnCaseClassWithoutVersion() extends CorrectRenamedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  @transformed(since = 1, identity[Int])
  case class WrongTransformedOnCaseClass()

  @deletedElements(1, "a")
  case class WrongDeletedElementsOnCaseClassWithoutVersion()

  @version(1)
  case class WrongVersionOnField(@version(1) a: String)

  case class WrongAddedOnFieldWithoutVersion(@added(1) a: String)

  @version(1)
  case class WrongAddedOnFieldWithoutDefaultValue(@added(1) a: String)

  case class WrongRenamedOnFieldWithoutVersion(@renamed(1, "a") a: String)

  case class WrongTransformedOnFieldWithoutVersion(@transformed(1, identity[String]) a: String)

  @version(1)
  case class WrongDeletedElementsOnField(@deletedElements(1, "a") a: String)

  @version(1)
  @added(since = 1)
  sealed trait WrongAddedOnSealedTrait
  case object Subtype1 extends WrongAddedOnSealedTrait

  @renamed(since = 1, "A")
  sealed trait WrongRenamedOnSealedTraitWithoutVersion
  case object Subtype2 extends WrongRenamedOnSealedTraitWithoutVersion

  @version(1)
  @transformed(since = 1, identity[Int])
  sealed trait WrongTransformedOnSealedTrait
  case object Subtype3 extends WrongTransformedOnSealedTrait

  @deletedElements(since = 1, "A")
  sealed trait WrongDeletedElementsOnSealedTraitWithoutVersion
  case object Subtype4 extends WrongDeletedElementsOnSealedTraitWithoutVersion

  @version(1)
  sealed trait WrongAddedOnSealedTraitSubtype
  @added(since = 1)
  case object WrongAddedOnSubtype extends WrongAddedOnSealedTraitSubtype

  sealed trait WrongRenamedOnSealedTraitSubtypeWithoutVersion
  @renamed(since = 1, "A")
  case object WrongRenamedOnSubtypeWithoutVersion extends WrongRenamedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  sealed trait WrongTransformedOnSealedTraitSubtype
  @transformed(since = 1, identity[Int])
  case object WrongTransformedOnSubtype extends WrongTransformedOnSealedTraitSubtype

  @version(1)
  sealed trait WrongDeletedElementsOnSealedTraitSubtype
  @deletedElements(since = 1, "A")
  case object WrongDeletedElementsOnSubtype extends WrongDeletedElementsOnSealedTraitSubtype

  @version(1)
  sealed trait CorrectDeletedElementsOnSealedTraitSubtype
  @version(1)
  @deletedElements(since = 1, "A")
  sealed trait CorrectDeletedElementsOnSubtype extends CorrectDeletedElementsOnSealedTraitSubtype
  case object CorrectDeletedElementsCaseObject extends CorrectDeletedElementsOnSubtype

  @version(2)
  @renamed(since = 1, "Click")
  @deletedElements(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  case class WrongAddedField(@added(since = 2) a: String = "")

  @version(2)
  @renamed(since = 1, "Click")
  @deletedElements(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  case class WrongRenamedField(@renamed(since = 2, "wrongFieldName") a: String)

  @version(2)
  @renamed(since = 1, "Click")
  @deletedElements(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  case class WrongTransformedField(@transformed(since = 2, identity[String]) wrongFieldName: String)

  @version(2)
  @renamed(since = 1, "Click")
  @deletedElements(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedElements(since = 2, "wrongFieldName")
  case class WrongDeletedField(a: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedElements(since = 1, "inFileClicks", "fieldNotInFile", "identifier")
  case class WrongFieldNotUsed(a: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedElements(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  case class WrongMissingField(a: String, missingField: String)

}
