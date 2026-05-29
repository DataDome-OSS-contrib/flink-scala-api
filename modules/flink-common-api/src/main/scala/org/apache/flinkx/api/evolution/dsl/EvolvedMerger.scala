package org.apache.flinkx.api.evolution.dsl

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.evolution.{EvolutionBuilder, Evolutions}

/** Merges a DSL-produced [[Evolved]] descriptor into an [[EvolutionBuilder]] populated from annotations, and into the
  * global [[Evolutions]] registry for class-level renames and deletions. Called once per ADT from
  * `TypeInformationDerivation`.
  *
  * Field-level deltas in [[Evolved.fieldDeltas]] are translated into `FieldEvolution` entries. For [[FieldDelta.Add]],
  * the default value comes from one of two sources:
  *   1. the macro-captured default carried by [[FieldDelta.Add.default]] when the user used the typed-accessor
  *      `.added(_.field)` overload (already validated at compile time);
  *   2. `defaultFor(name)` as a runtime fallback for the string-based `.added("field")` overload, which can't be
  *      statically checked.
  */
@Internal
object EvolvedMerger {

  /** Merge the DSL data carried by `evolved` into `builder` and the global [[Evolutions]] registry.
    *
    * @param evolved
    *   The DSL descriptor to merge
    * @param builder
    *   The annotation-driven `EvolutionBuilder` to mutate
    * @param currentClass
    *   The current ADT class (used as context for class-name resolution)
    * @param defaultFor
    *   Runtime fallback lookup from field name to Magnolia-supplied default value. Used only when a
    *   [[FieldDelta.Add]] has no macro-captured default (string-based API path). Should return `None` if the field
    *   has no default.
    */
  def merge[T](
      evolved: Evolved[T],
      builder: EvolutionBuilder[T],
      currentClass: Class[T],
      defaultFor: String => Option[Any]
  ): Unit = {
    evolved.fieldDeltas.foreach {
      case FieldDelta.Add(fieldName, since, dslDefault) =>
        builder.fieldEvolutions += Add(since, currentClass, fieldName, dslDefault.orElse(defaultFor(fieldName)))
      case FieldDelta.Rename(formerName, currentName, since) =>
        builder.fieldEvolutions += Rename(since, currentClass, formerName, currentName)
      case t: FieldDelta.Transform[a, b] =>
        builder.fieldEvolutions += Transform[a, b](t.since, currentClass, t.fieldName, t.mapper)
      case FieldDelta.Delete(formerName, since) =>
        builder.fieldEvolutions += Delete(since, currentClass, formerName)
    }

    evolved.renamedFormerClasses.foreach { formerName =>
      Evolutions.registerFormerClass(formerName, currentClass)
    }

    evolved.deletedFormerClasses.foreach { case (formerName, throwOnInstance) =>
      Evolutions.registerDeletedFormerClass(formerName, currentClass, throwOnInstance)
    }

    evolved.renamedFormerSubtypes.foreach { case (formerName, subtypeClass) =>
      Evolutions.registerFormerClass(formerName, subtypeClass)
    }

    evolved.formerToCurrentEnumValueName.foreach { case (formerName, currentName) =>
      builder.formerToCurrentEnumValueName(formerName) = currentName
    }

    evolved.postDeserialize.foreach { mapper =>
      // Mirror the "at most once" semantics of `@postDeserialize`: if annotation already set it, reject.
      builder.addPostDeserialize(new org.apache.flinkx.api.postDeserialize[T](mapper))
    }
  }

}
