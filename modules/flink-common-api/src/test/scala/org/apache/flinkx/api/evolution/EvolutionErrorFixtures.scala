package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.EvolutionTest._

/** ADTs whose evolutions are declared wrongly on purpose, to check what the feature reports. */
object EvolutionErrorFixtures {

  @version(-1)
  case class WrongCurrentVersion()
  object WrongCurrentVersion extends IsolatedEvolved[WrongCurrentVersion]

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
  @postEvolution(updateClick)
  @postEvolution(updateAction)
  case class WrongPostEvolutionTwiceOnCaseClass()

  /** Versioned, and no provider declares it. */
  @version(1)
  case class NeverDeclared(a: String)

  @version(1)
  case class WrongVersionOnField(@version(1) a: String)

  case class WrongAddedOnFieldWithoutVersion(@added(1) a: String)

  @version(1)
  case class WrongAddedOnFieldWithoutDefaultValue(@added(1) a: String)

  @version(1)
  case class WrongSinceAboveVersion(@added(since = 2) a: String = "")
  object WrongSinceAboveVersion extends IsolatedEvolved[WrongSinceAboveVersion]

  @version(1)
  @deletedFields(since = 0, "removed")
  case class WrongSinceBelowOne(a: String)
  object WrongSinceBelowOne extends IsolatedEvolved[WrongSinceBelowOne]

  case class WrongRenamedOnFieldWithoutVersion(@renamed(1, "a") a: String)

  case class WrongTransformedOnFieldWithoutVersion(@transformed(1, identity[String]) a: String)

  @version(1)
  case class WrongDeletedFieldsOnField(@deletedFields(1, "a") a: String)

  @version(1)
  case class WrongDeletedClassesOnField(@deletedClasses(since = 1, "A") a: String)

  @version(1)
  case class WrongPostEvolutionOnField(@postEvolution(updateClick) a: String)

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
  @postEvolution(updateClick)
  @postEvolution(updateAction)
  sealed trait WrongPostEvolutionTwiceOnSealedTrait
  case object Subtype4 extends WrongPostEvolutionTwiceOnSealedTrait

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
  object CorrectDeletedClassesOnSealedTraitSubtype extends IsolatedEvolved[CorrectDeletedClassesOnSealedTraitSubtype]
  @version(1)
  @deletedClasses(since = 1, "A")
  sealed trait CorrectDeletedClassesOnSubtype extends CorrectDeletedClassesOnSealedTraitSubtype
  object CorrectDeletedClassesOnSubtype       extends IsolatedEvolved[CorrectDeletedClassesOnSubtype]
  case object CorrectDeletedClassesCaseObject extends CorrectDeletedClassesOnSubtype

  @version(1)
  sealed trait WrongPostEvolutionOnSealedTraitSubtype
  @postEvolution(updateAction)
  case object WrongPostEvolutionOnSubtype extends WrongPostEvolutionOnSealedTraitSubtype

  @version(1)
  sealed trait CorrectPostEvolutionOnSealedTraitSubtype
  object CorrectPostEvolutionOnSealedTraitSubtype extends IsolatedEvolved[CorrectPostEvolutionOnSealedTraitSubtype]
  @version(1)
  @postEvolution(updateAction)
  case class CorrectPostEvolutionOnSubtype() extends CorrectPostEvolutionOnSealedTraitSubtype
  object CorrectPostEvolutionOnSubtype       extends IsolatedEvolved[CorrectPostEvolutionOnSubtype]

  @version(2)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "org.apache.flinkx.api.EvolutionTest$ClickEvent")
  case class WrongAddedField(@added(since = 2) a: String = "")
  object WrongAddedField extends IsolatedEvolved[WrongAddedField]

  @version(2)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "org.apache.flinkx.api.EvolutionTest$ClickEvent")
  case class WrongRenamedField(@renamed(since = 2, "wrongFieldName") a: String)
  object WrongRenamedField extends IsolatedEvolved[WrongRenamedField]

  @version(2)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "org.apache.flinkx.api.EvolutionTest$ClickEvent")
  case class WrongTransformedField(@transformed(since = 2, identity[String]) wrongFieldName: String)
  object WrongTransformedField extends IsolatedEvolved[WrongTransformedField]

  @version(2)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedFields(since = 2, "wrongFieldName")
  @deletedClasses(since = 1, throwOnInstance = false, "org.apache.flinkx.api.EvolutionTest$ClickEvent")
  case class WrongDeletedField(a: String)
  object WrongDeletedField extends IsolatedEvolved[WrongDeletedField]

  @version(1)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile")
  @deletedClasses(since = 1, throwOnInstance = false, "org.apache.flinkx.api.EvolutionTest$ClickEvent")
  case class WrongSeveralFields(a: String, missing: String)
  object WrongSeveralFields extends IsolatedEvolved[WrongSeveralFields]

  @version(1)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier")
  @deletedClasses(since = 1, throwOnInstance = false, "org.apache.flinkx.api.EvolutionTest$ClickEvent")
  case class WrongFieldNotUsed(a: String)
  object WrongFieldNotUsed extends IsolatedEvolved[WrongFieldNotUsed]

  @version(1)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "org.apache.flinkx.api.EvolutionTest$ClickEvent")
  case class WrongMissingField(a: String, missingField: String)
  object WrongMissingField extends IsolatedEvolved[WrongMissingField]

  @version(1)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Event")
  @deletedClasses(since = 1, throwOnInstance = true, "org.apache.flinkx.api.EvolutionTest$View")
  sealed trait WrongDeletedClassesWithSubtypeInstanceThrow
  object WrongDeletedClassesWithSubtypeInstanceThrow
      extends IsolatedEvolved[WrongDeletedClassesWithSubtypeInstanceThrow]

  @version(1)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Purchase")
  case object WrongDeletedClassesWithSubtypeInstanceThrowSubtype extends WrongDeletedClassesWithSubtypeInstanceThrow

  @version(1)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Event")
  @deletedClasses(since = 1, throwOnInstance = false, "org.apache.flinkx.api.EvolutionTest$View")
  sealed trait WrongDeletedClassesWithSubtypeInstanceToNull
  object WrongDeletedClassesWithSubtypeInstanceToNull
      extends IsolatedEvolved[WrongDeletedClassesWithSubtypeInstanceToNull]

  @version(1)
  @renamed(since = 1, "org.apache.flinkx.api.EvolutionTest$Purchase")
  case object WrongDeletedClassesWithSubtypeInstanceToNullSubtype extends WrongDeletedClassesWithSubtypeInstanceToNull
}
