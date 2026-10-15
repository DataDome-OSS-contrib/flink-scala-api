package org.apache.flinkx.api.evolution

import EvolutionTest._

/** ADTs whose evolutions are declared wrongly on purpose, to check what the feature reports. */
object EvolutionErrorFixtures {

  @version(-1)
  case class WrongCurrentVersion()

  // Not a literal, so the macros declaring the evolutions can't read it
  val NotLiteral: Int = 1

  @version(NotLiteral)
  case class WrongVersionNotLiteral()

  @version(1)
  case class WrongSinceNotLiteral(@added(since = NotLiteral) a: String = "")

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

  /** Derives its type information while initializing, before declaring the evolutions the derivation reads. */
  @version(1)
  case class DerivedBeforeDeclared(a: String)
  object DerivedBeforeDeclared extends Evolving[DerivedBeforeDeclared] {
    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flinkx.api.auto._

    val typeInfo: TypeInformation[DerivedBeforeDeclared] = implicitly[TypeInformation[DerivedBeforeDeclared]]
    lazy val evolutions                                  = Evolutions[DerivedBeforeDeclared]
  }

  /** Declares its evolutions in a strict val, which a derivation running while it initializes would read as null. */
  @version(1)
  case class EagerlyDeclared(a: String)
  object EagerlyDeclared extends Evolving[EagerlyDeclared] {
    val evolutions = Evolutions[EagerlyDeclared]
  }

  @version(1)
  case class WrongVersionOnField(@version(1) a: String)

  case class WrongAddedOnFieldWithoutVersion(@added(1) a: String)

  @version(1)
  case class WrongAddedOnFieldWithoutDefaultValue(@added(1) a: String)

  @version(1)
  case class WrongSinceAboveVersion(@added(since = 2) a: String = "")

  @version(1)
  @deletedFields(since = 0, "removed")
  case class WrongSinceBelowOne(a: String)

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
  object CorrectDeletedClassesOnSealedTraitSubtype extends Evolving[CorrectDeletedClassesOnSealedTraitSubtype] {
    lazy val evolutions = Evolutions[CorrectDeletedClassesOnSealedTraitSubtype]
  }
  @version(1)
  @deletedClasses(since = 1, "A")
  sealed trait CorrectDeletedClassesOnSubtype extends CorrectDeletedClassesOnSealedTraitSubtype
  object CorrectDeletedClassesOnSubtype       extends Evolving[CorrectDeletedClassesOnSubtype] {
    lazy val evolutions = Evolutions[CorrectDeletedClassesOnSubtype]
  }
  case object CorrectDeletedClassesCaseObject extends CorrectDeletedClassesOnSubtype

  @version(1)
  sealed trait WrongPostEvolutionOnSealedTraitSubtype
  @postEvolution(updateAction)
  case object WrongPostEvolutionOnSubtype extends WrongPostEvolutionOnSealedTraitSubtype

  @version(1)
  sealed trait CorrectPostEvolutionOnSealedTraitSubtype
  object CorrectPostEvolutionOnSealedTraitSubtype extends Evolving[CorrectPostEvolutionOnSealedTraitSubtype] {
    lazy val evolutions = Evolutions[CorrectPostEvolutionOnSealedTraitSubtype]
  }
  @version(1)
  @postEvolution(updateAction)
  case class CorrectPostEvolutionOnSubtype() extends CorrectPostEvolutionOnSealedTraitSubtype
  object CorrectPostEvolutionOnSubtype       extends Evolving[CorrectPostEvolutionOnSubtype] {
    lazy val evolutions = Evolutions[CorrectPostEvolutionOnSubtype]
  }

  @version(2)
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  case class WrongAddedField(@added(since = 2) a: String = "")
  object WrongAddedField extends Evolving[WrongAddedField] { lazy val evolutions = Evolutions[WrongAddedField] }

  @version(2)
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  case class WrongRenamedField(@renamed(since = 2, "wrongFieldName") a: String)
  object WrongRenamedField extends Evolving[WrongRenamedField] {
    lazy val evolutions = Evolutions[WrongRenamedField]
  }

  @version(2)
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  case class WrongTransformedField(@transformed(since = 2, identity[String]) wrongFieldName: String)
  object WrongTransformedField extends Evolving[WrongTransformedField] {
    lazy val evolutions = Evolutions[WrongTransformedField]
  }

  @version(2)
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedFields(since = 2, "wrongFieldName")
  case class WrongDeletedField(a: String)
  object WrongDeletedField extends Evolving[WrongDeletedField] {
    lazy val evolutions = Evolutions[WrongDeletedField]
  }

  @version(1)
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile")
  case class WrongSeveralFields(a: String, missing: String)
  object WrongSeveralFields extends Evolving[WrongSeveralFields] {
    lazy val evolutions = Evolutions[WrongSeveralFields]
  }

  @version(1)
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier")
  case class WrongFieldNotUsed(a: String)
  object WrongFieldNotUsed extends Evolving[WrongFieldNotUsed] {
    lazy val evolutions = Evolutions[WrongFieldNotUsed]
  }

  @version(1)
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  case class WrongMissingField(a: String, missingField: String)
  object WrongMissingField extends Evolving[WrongMissingField] {
    lazy val evolutions = Evolutions[WrongMissingField]
  }
}
