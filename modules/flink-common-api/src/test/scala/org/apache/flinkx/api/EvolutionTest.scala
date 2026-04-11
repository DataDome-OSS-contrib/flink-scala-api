package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.EvolutionTest._
import org.apache.flinkx.api.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionTest extends AnyFlatSpec with Matchers with TestUtils {

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
    exception.getMessage shouldBe "Current version must be positive, got @version(0)"
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

  it should "throw when @deletedMembers is on a case class without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongDeletedMembersOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedMembers(1,Array(\"a\")) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedMembersOnCaseClassWithoutVersion without version"
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

  it should "throw when @deletedMembers is on a case class field" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongDeletedMembersOnField]]
    }
    exception.getMessage shouldBe "@deletedMembers(1,Array(\"a\")) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedMembersOnField.a"
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

  it should "throw when @deletedMembers is on a sealed trait without version" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongDeletedMembersOnSealedTraitWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedMembers(1,Array(\"A\")) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongDeletedMembersOnSealedTraitWithoutVersion without version"
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

  it should "throw when @deletedMembers is on a sealed trait subtype" in {
    val exception = intercept[FlinkRuntimeException] {
      implicitly[TypeInformation[WrongDeletedMembersOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@deletedMembers(1,Array(\"A\")) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedMembersOnSubtype$ without version"
  }

  it should "allow when @deletedMembers is on a sealed trait subtype being itself a sealed trait" in {
    implicitly[TypeInformation[CorrectDeletedMembersOnSealedTraitSubtype]]
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
    exception.getMessage shouldBe "'b' field not used to instantiate class org.apache.flinkx.api.EvolutionTest$WrongFieldNotUsed. Use @deletedMembers(since=<version>,Array(\"b\")) annotation to indicate it has been deleted"
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
  @deletedMembers(since = 1, Array("a"))
  @deletedMembers(since = 2, Array("b"))
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
  @deletedMembers(since = 1, Array("Purchase"))
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

  @deletedMembers(1, Array("a"))
  case class WrongDeletedMembersOnCaseClassWithoutVersion()

  @version(1)
  case class WrongVersionOnField(@version(1) a: String)

  case class WrongAddedOnFieldWithoutVersion(@added(1) a: String)

  @version(1)
  case class WrongAddedOnFieldWithoutDefaultValue(@added(1) a: String)

  case class WrongRenamedOnFieldWithoutVersion(@renamed(1, "a") a: String)

  case class WrongTransformedOnFieldWithoutVersion(@transformed(1, identity[String]) a: String)

  @version(1)
  case class WrongDeletedMembersOnField(@deletedMembers(1, Array("a")) a: String)

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

  @deletedMembers(since = 1, Array("A"))
  sealed trait WrongDeletedMembersOnSealedTraitWithoutVersion
  case object Subtype4 extends WrongDeletedMembersOnSealedTraitWithoutVersion

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
  sealed trait WrongDeletedMembersOnSealedTraitSubtype
  @deletedMembers(since = 1, Array("A"))
  case object WrongDeletedMembersOnSubtype extends WrongDeletedMembersOnSealedTraitSubtype

  @version(1)
  sealed trait CorrectDeletedMembersOnSealedTraitSubtype
  @version(1)
  @deletedMembers(since = 1, Array("A"))
  sealed trait CorrectDeletedMembersOnSubtype extends CorrectDeletedMembersOnSealedTraitSubtype
  case object CorrectDeletedMembersCaseObject extends CorrectDeletedMembersOnSubtype

  @version(2)
  @renamed(since = 1, "Click")
  @deletedMembers(since = 1, Array("inFileClicks", "fieldNotInFile", "identifier", "b"))
  case class WrongAddedField(@added(since = 2) a: String = "")

  @version(2)
  @renamed(since = 1, "Click")
  @deletedMembers(since = 1, Array("inFileClicks", "fieldNotInFile", "identifier", "b"))
  case class WrongRenamedField(@renamed(since = 2, "wrongFieldName") a: String)

  @version(2)
  @renamed(since = 1, "Click")
  @deletedMembers(since = 1, Array("inFileClicks", "fieldNotInFile", "identifier", "b"))
  case class WrongTransformedField(@transformed(since = 2, identity[String]) wrongFieldName: String)

  @version(2)
  @renamed(since = 1, "Click")
  @deletedMembers(since = 1, Array("inFileClicks", "fieldNotInFile", "identifier", "b"))
  @deletedMembers(since = 2, Array("wrongFieldName"))
  case class WrongDeletedField(a: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedMembers(since = 1, Array("inFileClicks", "fieldNotInFile", "identifier"))
  case class WrongFieldNotUsed(a: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedMembers(since = 1, Array("inFileClicks", "fieldNotInFile", "identifier", "b"))
  case class WrongMissingField(a: String, missingField: String)

}
