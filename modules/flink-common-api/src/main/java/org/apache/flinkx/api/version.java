package org.apache.flinkx.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the current schema version of an ADT (case class, sealed trait or Scala 3 enum) and opts it in to the
 * annotation-based schema evolution feature allowing to restore former data read from a checkpoint to the current
 * source code.
 *
 * <p>This feature commonly employs the following vocabulary to qualify version, class, field, etc.:
 * <ul>
 *   <li>{@code Former} describes the serialization time when the checkpoint was done.
 *   <li>{@code Current} describes the deserialization time with the current source code.
 * </ul>
 *
 * <p>An ADT without this annotation is considered to have version 0 which makes it safe to add {@code @version(1)} to
 * an existing ADT and restore it from a checkpoint produced by the unversioned code.
 *
 * <p>Unlike the evolution annotations, this one is a Java annotation retained at runtime: it declares which ADTs the
 * feature applies to, which has to be readable from the class alone, wherever the derivation ran. Its value is
 * therefore read reflectively rather than through the derivation.
 *
 * <p>Annotation of ADT (case class, sealed trait or Scala 3 enum). A version below 0 throws
 * {@code org.apache.flinkx.api.evolution.VersionNotAllowedException}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface version {

    /** Current schema version, must be >= 0. */
    int value();
}
