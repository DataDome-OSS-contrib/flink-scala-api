package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal

/** The evolutions an ADT declares through its annotations, read at compile time where its companion extends
  * [[Evolved]], and applied by [[Evolutions]] once that companion is initialized.
  *
  * Nothing is built before then: the companion may still be initializing, and the mappers and default values the
  * annotations name may live in it.
  *
  * @param currentClass
  *   The ADT class declaring the evolutions
  * @param builders
  *   Builds the evolutions from the annotations of the ADT, and of the case objects among its subtypes, which have no
  *   companion of their own to declare them
  * @param memberClasses
  *   The subtypes of a sealed trait that are classes, whose own companions declare their own evolutions
  */
@Internal
final class Declaration[T](
    val currentClass: Class[T],
    builders: () => Seq[EvolutionBuilder[?]],
    val memberClasses: () => Seq[Class[?]]
):
  private[evolution] def build(): Seq[EvolutionBuilder[?]] = builders()

object Declaration:

  /** Reads the annotations of `T`, from the companion of `T`: a mapper private to that companion is visible there. */
  inline given derived[T]: Declaration[T] = ${ Declare.declarationImpl[T] }
