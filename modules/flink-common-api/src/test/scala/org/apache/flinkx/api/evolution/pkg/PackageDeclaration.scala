package org.apache.flinkx.api.evolution.pkg

import org.apache.flinkx.api.evolution.Declare

/** Declares the ADTs of the package it sits in, as a provider placed at the root of a model does. */
object PackageDeclaration {
  def declare(): Unit = Declare.declarePackage
}
