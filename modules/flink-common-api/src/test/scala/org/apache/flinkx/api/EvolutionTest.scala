package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  import org.apache.flinkx.api.EvolutionTest._

  override protected def beforeEach(): Unit = Evolutions.reset()

  /* Test to serialize Click v0 code into Click-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Click v0" in {
    val event: Click = Click("a", List(ClickEvent("2021-01-01", 1), ClickEvent("2022-02-02", 2)), 5, "id1", "b")
    serializeToFile("Click-v0", event)
  }
   */

  it should "deserialize Click v0 to Click v3" in {
    val expected: Click = Click("id1", List(ClickAction("1", "2021-01-01"), ClickAction("2", "2022-02-02")), 25, "5")
    testDeserializeFromFile("Click-v0", expected)
  }

  /* Test to serialize Event v0 code into Event-v0.snapshot file, uncomment both test and code to regenerate
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
    val exception = intercept[VersionNotAllowedException] {
      implicitly[TypeInformation[WrongCurrentVersion]]
    }
    exception.getMessage shouldBe "Current version of class org.apache.flinkx.api.EvolutionTest$WrongCurrentVersion must be >= 0, got @version(-1)"
  }

  it should "throw when @added is on a case class" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnCaseClass]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongAddedOnCaseClass"
  }

  it should "throw when @renamed is on a case class with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongRenamedOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnCaseClassWithoutVersion with version 0"
  }

  it should "allow when @renamed is on a sealed trait subtype with version 0" in {
    implicitly[TypeInformation[CorrectRenamedOnSealedTraitSubtypeWithoutVersion]]
    implicitly[TypeInformation[CorrectRenamedOnCaseClassWithoutVersion]]
  }

  it should "throw when @transformed is on a case class" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnCaseClass]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnCaseClass"
  }

  it should "throw when @deletedFields is on a case class with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedFieldsOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedFields(1,\"a\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedFieldsOnCaseClassWithoutVersion with version 0"
  }

  it should "throw when @deletedClasses is on a case class with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedClassesOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedClasses(\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedClassesOnCaseClassWithoutVersion with version 0"
  }

  it should "throw when @postDeserialize is on a case class twice" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongPostDeserializeTwiceOnCaseClass]]
    }
    exception.getMessage shouldBe "@postDeserialize(<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongPostDeserializeTwiceOnCaseClass twice"
  }

  it should "throw when @version is on a case class field" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongVersionOnField]]
    }
    exception.getMessage shouldBe "@version(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongVersionOnField.a"
  }

  it should "throw when @added is on a case class field with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongAddedOnFieldWithoutVersion with version 0"
  }

  it should "throw when @added is on a case class field without default value" in {
    val exception = intercept[AddedFieldWithoutDefaultException] {
      implicitly[TypeInformation[WrongAddedOnFieldWithoutDefaultValue]]
    }
    exception.getMessage shouldBe "'a' added field in class org.apache.flinkx.api.EvolutionTest$WrongAddedOnFieldWithoutDefaultValue must have a default value"
  }

  it should "throw when @renamed is on a case class field with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongRenamedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"a\") annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnFieldWithoutVersion with version 0"
  }

  it should "throw when @transformed is on a case class field with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnFieldWithoutVersion with version 0"
  }

  it should "throw when @deletedFields is on a case class field" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedFieldsOnField]]
    }
    exception.getMessage shouldBe "@deletedFields(1,\"a\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedFieldsOnField.a"
  }

  it should "throw when @deletedClasses is on a case class field" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedClassesOnField]]
    }
    exception.getMessage shouldBe "@deletedClasses(\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedClassesOnField.a"
  }

  it should "throw when @postDeserialize is on a case class field" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongPostDeserializeOnField]]
    }
    exception.getMessage shouldBe "@postDeserialize(<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongPostDeserializeOnField.a"
  }

  it should "throw when @added is on a sealed trait" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnSealedTrait]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongAddedOnSealedTrait"
  }

  it should "throw when @renamed is on a sealed trait with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongRenamedOnSealedTraitWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongRenamedOnSealedTraitWithoutVersion with version 0"
  }

  it should "throw when @transformed is on a sealed trait" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnSealedTrait]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongTransformedOnSealedTrait"
  }

  it should "throw when @postDeserialize is on a sealed trait twice" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongPostDeserializeTwiceOnSealedTrait]]
    }
    exception.getMessage shouldBe "@postDeserialize(<mapper>) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongPostDeserializeTwiceOnSealedTrait twice"
  }

  it should "throw when @deletedClasses is on a sealed trait with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedClassesOnSealedTraitWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedClasses(\"A\") annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongDeletedClassesOnSealedTraitWithoutVersion with version 0"
  }

  it should "throw when @added is on a sealed trait subtype with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnSealedTraitSubtypeWithoutVersion]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongAddedOnSubtypeWithoutVersion$ with version 0"
  }

  it should "throw when @added is on a sealed trait subtype" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongAddedOnSubtype$"
  }

  it should "throw when @renamed is on a sealed trait subtype with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongRenamedOnSealedTraitSubtypeWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnSubtypeWithoutVersion$ with version 0"
  }

  it should "throw when @transformed is on a sealed trait subtype with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnSealedTraitSubtypeWithoutVersion]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnSubtypeWithoutVersion$ with version 0"
  }

  it should "throw when @transformed is on a sealed trait subtype" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnSubtype$"
  }

  it should "throw when @deletedClasses is on a sealed trait subtype" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedClassesOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@deletedClasses(\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedClassesOnSubtype$ with version 0"
  }

  it should "allow when @deletedClasses is on a sealed trait subtype being itself a sealed trait" in {
    implicitly[TypeInformation[CorrectDeletedClassesOnSealedTraitSubtype]]
  }

  it should "throw when @postDeserialize is on a sealed trait subtype with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongPostDeserializeOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@postDeserialize(<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongPostDeserializeOnSubtype$ with version 0"
  }

  it should "allow when @postDeserialize is on a versioned sealed trait subtype" in {
    implicitly[TypeInformation[CorrectPostDeserializeOnSealedTraitSubtype]]
  }

  it should "throw field not found when deserializing Click v0 with wrong added field" in {
    val expected  = WrongAddedField("123456789")
    val exception = intercept[FieldAlreadyExistException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot add 'a'. Field already exists in class org.apache.flinkx.api.EvolutionTest$WrongAddedField. Existing fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong renamed field" in {
    val expected  = WrongRenamedField("123456789")
    val exception = intercept[FieldNotFoundException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot rename 'wrongFieldName'. Field not found in class org.apache.flinkx.api.EvolutionTest$WrongRenamedField. Available fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong transformed field" in {
    val expected  = WrongTransformedField("123456789")
    val exception = intercept[FieldNotFoundException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot transform 'wrongFieldName'. Field not found in class org.apache.flinkx.api.EvolutionTest$WrongTransformedField. Available fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong deleted field" in {
    val expected  = WrongDeletedField("123456789")
    val exception = intercept[FieldNotFoundException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot delete 'wrongFieldName'. Field not found in class org.apache.flinkx.api.EvolutionTest$WrongDeletedField. Available fields: [\"a\"]"
  }

  it should "throw field not used when deserializing Click v0 with extra field" in {
    val expected  = WrongFieldNotUsed("123456789")
    val exception = intercept[FieldNotUsedException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "'b' field not used to instantiate class org.apache.flinkx.api.EvolutionTest$WrongFieldNotUsed. Use @deletedFields(since=<version>,\"b\") annotation to indicate it has been deleted"
  }

  it should "throw missing field when deserializing Click v0 with missing field to instantiate" in {
    val expected  = WrongMissingField("123456789", "is missing")
    val exception = intercept[MissingFieldException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "'missingField' field missing to instantiate class org.apache.flinkx.api.EvolutionTest$WrongMissingField. Use @added(since=<version>) annotation to indicate it has been added"
  }

  it should "throw when deserializing Event v0 with delete subtype instance" in {
    val exception = intercept[DeletedInstanceException] {
      testDeserializeFromFile[WrongDeletedClassesWithSubtypeInstanceThrow]("Event-v0", null)
    }
    exception.getMessage shouldBe "Encountered an instance of deleted 'org.apache.flinkx.api.EvolutionTest$View' class during deserialization. Don't delete a class in usage or use @deletedClasses(since = <version>, throwOnInstance = false, ...) to deserialize it as null instead"
  }

  it should "return null when deserializing Event v0 with delete subtype instance" in {
    testDeserializeFromFile[WrongDeletedClassesWithSubtypeInstanceToNull]("Event-v0", null)
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
  @deletedFields(since = 1, "a")
  @deletedFields(since = 2, "b")
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

  def mapIntToString(a: Int): String                 = a.toString
  def updateClick(version: Int, click: Click): Click =
    click.copy(fieldInFile = click.fieldNotInFile.toInt * 5 + version)

  /* event-v0
  sealed trait Event
  case class View(ts: Long) extends Event
  case class Purchase(price: Double) extends Event
   */

  @version(1)
  @renamed(since = 1, "Event")
  @deletedClasses(since = 1, "Purchase")
  @postDeserialize(updateAction)
  sealed trait Action

  @version(0)
  case object Login extends Action

  @renamed(since = 1, "View")
  case class Web(ts: Long) extends Action

  case class Cart(items: Int) extends Action

  def updateAction(version: Int, action: Action): Action = action match {
    case Web(ts) => Web(if (version == 0) ts + 1 else ts)
    case Cart(_) => Login
    case e @ _   => e
  }

  // Error handling

  @version(-1)
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

  @deletedFields(1, "a")
  case class WrongDeletedFieldsOnCaseClassWithoutVersion()

  @deletedClasses(since = 1, "A")
  case class WrongDeletedClassesOnCaseClassWithoutVersion()

  @version(1)
  @postDeserialize(updateClick)
  @postDeserialize(updateAction)
  case class WrongPostDeserializeTwiceOnCaseClass()

  @version(1)
  case class WrongVersionOnField(@version(1) a: String)

  case class WrongAddedOnFieldWithoutVersion(@added(1) a: String)

  @version(1)
  case class WrongAddedOnFieldWithoutDefaultValue(@added(1) a: String)

  case class WrongRenamedOnFieldWithoutVersion(@renamed(1, "a") a: String)

  case class WrongTransformedOnFieldWithoutVersion(@transformed(1, identity[String]) a: String)

  @version(1)
  case class WrongDeletedFieldsOnField(@deletedFields(1, "a") a: String)

  @version(1)
  case class WrongDeletedClassesOnField(@deletedClasses(since = 1, "A") a: String)

  @version(1)
  case class WrongPostDeserializeOnField(@postDeserialize(updateClick) a: String)

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

  @version(1)
  @postDeserialize(updateClick)
  @postDeserialize(updateAction)
  sealed trait WrongPostDeserializeTwiceOnSealedTrait
  case object Subtype4 extends WrongPostDeserializeTwiceOnSealedTrait

  @deletedClasses(since = 1, "A")
  sealed trait WrongDeletedClassesOnSealedTraitWithoutVersion
  case object Subtype5 extends WrongDeletedClassesOnSealedTraitWithoutVersion

  @version(1)
  sealed trait WrongAddedOnSealedTraitSubtypeWithoutVersion
  @added(since = 1)
  case object WrongAddedOnSubtypeWithoutVersion extends WrongAddedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  sealed trait WrongAddedOnSealedTraitSubtype
  @version(1)
  @added(since = 1)
  case object WrongAddedOnSubtype extends WrongAddedOnSealedTraitSubtype

  sealed trait WrongRenamedOnSealedTraitSubtypeWithoutVersion
  @renamed(since = 1, "A")
  case object WrongRenamedOnSubtypeWithoutVersion extends WrongRenamedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  sealed trait WrongTransformedOnSealedTraitSubtypeWithoutVersion
  @transformed(since = 1, identity[Int])
  case object WrongTransformedOnSubtypeWithoutVersion extends WrongTransformedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  sealed trait WrongTransformedOnSealedTraitSubtype
  @version(1)
  @transformed(since = 1, identity[Int])
  case object WrongTransformedOnSubtype extends WrongTransformedOnSealedTraitSubtype

  @version(1)
  sealed trait WrongDeletedClassesOnSealedTraitSubtype
  @deletedClasses(since = 1, "A")
  case object WrongDeletedClassesOnSubtype extends WrongDeletedClassesOnSealedTraitSubtype

  @version(1)
  sealed trait CorrectDeletedClassesOnSealedTraitSubtype
  @version(1)
  @deletedClasses(since = 1, "A")
  sealed trait CorrectDeletedClassesOnSubtype extends CorrectDeletedClassesOnSealedTraitSubtype
  case object CorrectDeletedClassesCaseObject extends CorrectDeletedClassesOnSubtype

  @version(1)
  sealed trait WrongPostDeserializeOnSealedTraitSubtype
  @postDeserialize(updateAction)
  case object WrongPostDeserializeOnSubtype extends WrongPostDeserializeOnSealedTraitSubtype

  @version(1)
  sealed trait CorrectPostDeserializeOnSealedTraitSubtype
  @version(1)
  @postDeserialize(updateAction)
  case class CorrectPostDeserializeOnSubtype() extends CorrectPostDeserializeOnSealedTraitSubtype

  @version(2)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, "ClickEvent")
  case class WrongAddedField(@added(since = 2) a: String = "")

  @version(2)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, "ClickEvent")
  case class WrongRenamedField(@renamed(since = 2, "wrongFieldName") a: String)

  @version(2)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, "ClickEvent")
  case class WrongTransformedField(@transformed(since = 2, identity[String]) wrongFieldName: String)

  @version(2)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedFields(since = 2, "wrongFieldName")
  @deletedClasses(since = 1, "ClickEvent")
  case class WrongDeletedField(a: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier")
  @deletedClasses(since = 1, "ClickEvent")
  case class WrongFieldNotUsed(a: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, "ClickEvent")
  case class WrongMissingField(a: String, missingField: String)

  @version(1)
  @renamed(since = 1, "Event")
  @deletedClasses(since = 1, throwOnInstance = true, "View")
  sealed trait WrongDeletedClassesWithSubtypeInstanceThrow

  @version(1)
  @renamed(since = 1, "Purchase")
  case object WrongDeletedClassesWithSubtypeInstanceThrowSubtype extends WrongDeletedClassesWithSubtypeInstanceThrow

  @version(1)
  @renamed(since = 1, "Event")
  @deletedClasses(since = 1, throwOnInstance = false, "View")
  sealed trait WrongDeletedClassesWithSubtypeInstanceToNull

  @version(1)
  @renamed(since = 1, "Purchase")
  case object WrongDeletedClassesWithSubtypeInstanceToNullSubtype extends WrongDeletedClassesWithSubtypeInstanceToNull

}
