package org.apache.flinkx.api.evolution.dsl

import org.apache.flink.annotation.Internal

/** Cross-version, always-resolvable typeclass that wraps an `Option[Evolved[T]]`. It exists because the type
  * `Option[Evolved[T]]` cannot itself be summoned at the derivation site (no implicit instances for it directly), and
  * because Scala 2 and Scala 3 need a uniform mechanism to summon "an `Evolved[T]` if the user provided one, otherwise
  * `None`".
  *
  * Resolution rules:
  *   - If the user provides an `implicit/given Evolved[T]` (typically in `T`'s companion), [[OptionalEvolved.some]]
  *     fires and the wrapper carries `Some(ev)`.
  *   - Otherwise the low-priority fallback [[LowPriorityOptionalEvolved.none]] fires and the wrapper carries `None`.
  *
  * Used by `TypeInformationDerivation` via a context bound on `join` / `split`.
  */
@Internal
final case class OptionalEvolved[T](value: Option[Evolved[T]])

@Internal
object OptionalEvolved extends LowPriorityOptionalEvolved {

  /** Higher-priority instance: fires when the user has an `implicit/given Evolved[T]` in scope. */
  implicit def some[T](implicit ev: Evolved[T]): OptionalEvolved[T] = OptionalEvolved(Some(ev))

}

@Internal
trait LowPriorityOptionalEvolved {

  /** Fallback when no `Evolved[T]` is in scope. Always resolves; never asks the compiler to summon anything else. */
  implicit def none[T]: OptionalEvolved[T] = OptionalEvolved(None)

}
