package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal

/** Declares the evolutions of the ADTs of a module, so that they reach the JVMs that never derive them.
  *
  * Implementations are listed in `META-INF/services/org.apache.flinkx.api.evolution.EvolutionsProvider` and loaded by
  * [[Evolutions]] on first need, with the class loader asking for a declaration: on a TaskManager that happens while
  * reading a checkpoint, before any state is restored, and on the client while deriving the type information.
  *
  * An implementation needs a public no-argument constructor, and declares a whole package at once, or one ADT at a
  * time:
  * {{{
  * class MyEvolutions extends EvolutionsProvider {
  *   override def declare(): Unit = {
  *     Declare.declarePackage("com.example.model")
  *     Declare.declarePackage // Declare current package
  *     Declare.declare[Order]
  *   }
  * }
  * }}}
  */
@Internal
trait EvolutionsProvider {

  /** Register the evolutions of every ADT of the module. Called once per class loader, and must be idempotent. */
  def declare(): Unit
}
