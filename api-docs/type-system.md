# Interaction with Flink's type system

This Scala API is enforcing usage of Flink's `TypeInformation` objects by requiring them to be implicitly available in the scope. It plays well with the derivation macro generating TypeInformations for Scala ADTs.

However, this project cannot enforce TypeInformation usage in the Flink Java API where there is other ways to provide information on types to Flink, notably using `Class`, for exemple:
- `TypeInformation.of(Class<T>)`
- `StateDescriptor` and subclasses: constructors with a `Class<T>` param
- `TypeHint`

Usage of this code may lead to silently fallback to Kryo.

From Flink 1.19, a check is done to detect this misusage. To disable it, see [Disable fail-fast on Scala type resolution with Class feature flag](feature-flags.md#disable-fail-fast-on-scala-type-resolution-with-class).

> [!WARNING]  
> Official `flink-scala` deprecated dependency contains Scala-specialized Kryo serializers. If this dependency is removed from the classpath (see [Supported Flink versions](getting-started.md#supported-flink-versions)), usage of Kryo with Scala classes leads to erroneous re-instantiations of `object` and `case object` singletons.
> 
> We recommend to test your application with Kryo explicitly disabled (Flink property `pipeline.generic-types: false`).

## Flink ADT

To derive a TypeInformation for a case class or sealed trait, you can do:

```scala mdoc:reset-object
import org.apache.flinkx.api.semiauto._
import org.apache.flink.api.common.typeinfo.TypeInformation

sealed trait Event extends Product with Serializable

object Event {
  final case class Click(id: String) extends Event
  final case class Purchase(price: Double) extends Event

  implicit val eventTypeInfo: TypeInformation[Event] = deriveTypeInformation
}
```

Be careful with a wildcard import of import `org.apache.flink.api.scala._`: it has a `createTypeInformation` implicit function, which may happily generate you a kryo-based serializer in a place you never expected. So in a case if you want to do this type of wildcard import, make sure that you explicitly called `deriveTypeInformation` for all the sealed traits in the current scope.

### Auto and semi-auto derivation

This library provides two approaches for deriving TypeInformation: **automatic** (`auto`) and **semi-automatic** (`semiauto`).

#### Automatic derivation

Import `org.apache.flinkx.api.auto._` when you want TypeInformation to be derived automatically:

```scala mdoc:reset-object
import org.apache.flinkx.api._
import org.apache.flinkx.api.auto._ // Automatic derivation

case class User(id: String, age: Int)

val env = StreamExecutionEnvironment.getExecutionEnvironment

// TypeInformation is derived automatically
env.fromElements(User("alice", 30), User("bob", 25))
```

Auto TypeInformation derivation is called implicitly whenever needed. This automatic behavior is easier and more convenient when you don't need a fine control over derivation.

#### Semi-automatic derivation

Import `org.apache.flinkx.api.semiauto._` when you want explicit control over TypeInformation derivation:

```scala mdoc:reset-object
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api._
import org.apache.flinkx.api.semiauto._ // Manual derivation

case class User(id: String, age: Int)

object User {
  // Explicitly derive and cache TypeInformation
  implicit val userInfo: TypeInformation[User] = deriveTypeInformation[User]
}

val env = StreamExecutionEnvironment.getExecutionEnvironment

// Uses pre-derived TypeInformation found in User companion object
env.fromElements(User("alice", 30), User("bob", 25))
```

A good practice is to declare the type-information as implicit val in the companion object of the case class, it's derived once, cached, and will be available in the implicit context wherever the case class is used.

Benefits of `semiauto`:
- Control: Choose exactly which types have TypeInformation
- Better compile times: TypeInformation is derived once and cached

### Null value handling

A case class can be null, the case class serializer natively handles the null case.

A case class field can also be null, either:
- the serializer of this field natively handles its nullability.
- the field must be annotated with `@nullable` in order to be wrapped in Flink's `NullableSerializer`.

In any cases, it can be a good hint to use `@nullable` annotation to indicate when fields are meant to be nullable.

```scala mdoc:reset-object
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.serializer.nullable
import org.apache.flink.api.common.typeinfo.TypeInformation

case class Click(id: String, clickEvent: ClickEvent)

case class ClickEvent(
    @nullable history: Array[String], // @nullable allows to handle null array
    @nullable id: String) // Effectless here as null strings are natively handled

Click("id1", null) // A case class can be null
Click("id2", ClickEvent(null, null)) // Valid thanks to @nullable
```

## Java types

Built-in serializers are for Scala language abstractions and won't derive `TypeInformation` for Java classes (as they don't extend the `scala.Product` type). But you can always fall back to Flink's own POJO serializer in this way, so just make it implicit so this API can pick it up:

```scala mdoc:reset-object
import java.time.LocalDate
import org.apache.flink.api.common.typeinfo.TypeInformation

implicit val localDateTypeInfo: TypeInformation[LocalDate] = TypeInformation.of(classOf[LocalDate])
```

## Type mapping

Sometimes built-in serializers may spot a type (usually a Java one), which cannot be directly serialized as a case class, like this 
example:

```scala mdoc:reset-object
class WrappedString {
  private var internal: String = ""

  override def equals(obj: Any): Boolean = 
    obj match {
      case s: WrappedString => s.get == internal
      case _                => false
    }
  
  def get: String = internal
  def put(value: String) =
    internal = value 
}   
```

You can write a pair of explicit `TypeInformation[WrappedString]` and `Serializer[WrappedString]`, but it's extremely verbose,
and the class itself can be 1-to-1 mapped to a regular `String`. This library has a mechanism of type mappers to delegate serialization
of non-serializable types to existing serializers. For example:

```scala mdoc
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto._

class WrappedMapper extends TypeMapper[WrappedString, String] {
  override def map(a: WrappedString): String = a.get

  override def contramap(b: String): WrappedString = {
    val str = new WrappedString
    str.put(b)
    str
  }  
}

implicit val mapper: TypeMapper[WrappedString, String] = new WrappedMapper()
// will treat WrappedString with String typeinfo:
implicit val ti: TypeInformation[WrappedString] = mappedTypeInfo[WrappedString, String]
```

When there is a `TypeMapper[A, B]` in the scope to convert `A` to `B` and back, and type `B` has `TypeInformation[B]` available 
in the scope also, then this library will use a delegated existing typeinfo for `B` when it will spot type `A`.

Warning: on Scala 3, the TypeMapper should not be made anonymous. This example won't work, as anonymous implicit classes in 
Scala 3 are private, and Flink cannot instantiate it on restore without JVM 17 incompatible reflection hacks:

```scala mdoc:reset-object
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper

class WrappedString {
  private var internal: String = ""

  override def equals(obj: Any): Boolean = 
    obj match {
      case s: WrappedString => s.get == internal
      case _                => false
    }

  def get: String = internal
  def put(value: String) =
    internal = value
}  
  
class WrappedMapper extends TypeMapper[WrappedString, String] {
  override def map(a: WrappedString): String = a.get

  override def contramap(b: String): WrappedString = {
    val str = new WrappedString
    str.put(b)
    str  
  }
}
// anonymous class, will fail on runtime on scala 3
implicit val mapper2: TypeMapper[WrappedString, String] = new TypeMapper[WrappedString, String] {
  override def map(a: WrappedString): String = a.get

  override def contramap(b: String): WrappedString = {
    val str = new WrappedString
    str.put(b)
    str  
  }
}  
```

## Ordering

`SortedSet` requires a type-information for its elements and also for the ordering of the elements. Type-information of default orderings are not implicitly available in the context because we cannot make the assumption the user wants to use the natural ordering or a custom one.

Type-information of default ordering are available in `org.apache.flinkx.api.serializer.OrderingTypeInfo` and can be used as follows:
```scala mdoc:reset-object
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api._
import org.apache.flinkx.api.serializer.OrderingTypeInfo
import org.apache.flinkx.api.auto._
import scala.collection.immutable.SortedSet

case class Foo(bars: SortedSet[String])

object Foo {
  implicit val fooInfo: TypeInformation[Foo] = {
    // type-information for Ordering need to be explicitly put in the context
    implicit val orderingStringInfo: TypeInformation[Ordering[String]] =
      OrderingTypeInfo.DefaultStringOrderingInfo
    deriveTypeInformation
  }
}
```

It's also possible to derive the type-information of a custom ordering if it's an ADT:
```scala mdoc:reset-object
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api._
import org.apache.flinkx.api.serializer.OrderingTypeInfo
import org.apache.flinkx.api.auto._
import scala.collection.immutable.SortedSet

case class Bar(a: Int, b: String)

case object BarOrdering extends Ordering[Bar] {
  override def compare(x: Bar, y: Bar): Int = x.a.compare(y.a)
}

case class Foo(bar: SortedSet[Bar])

object Foo {
  implicit val fooInfo: TypeInformation[Foo] = {
    // Derive the type-information of custom Bar ordering
    implicit val barOrderingInfo: TypeInformation[Ordering[Bar]] =
      OrderingTypeInfo.deriveOrdering[BarOrdering.type, Bar]
    deriveTypeInformation
  }
}
```

## Schema evolution

This library supports two complementary mechanisms for evolving the schema of state stored in checkpoints and
savepoints: built-in compatibility rules that come "for free", and an opt-in annotation-based system for richer changes.

### Built-in compatibility

Without any annotation, the following changes are safe between checkpoint write and restore (starting from version 2.4.0):
* Case classes: you can reorder fields.
* Sealed traits: you can reorder subtypes and add new subtypes.
* For everything else (additions, renames, deletions, type changes, cross-field migrations), use the annotation-based schema evolution below.

### Annotation-based schema evolution

Annotate an ADT (case class, sealed trait or Scala 3 enum) with `@version(n)` to opt in it, then describe each
change with one of the evolution annotations on the ADT or on its fields/subtypes. On restore, the library
applies these operations against the former data read from the checkpoint so it matches the current source code.

This schema evolution feature commonly employs the following vocabulary to qualify version, class, field, etc.:
* `Former` describes the serialization time when the checkpoint was done.
* `Current` describes the deserialization time with the current source code.

An ADT without `@version` annotation is considered to have version 0 which makes it safe to add `@version(1)` to an
existing ADT and restore it from a checkpoint produced by the unversioned code.

For example, given this former schema serialized to the checkpoint (no annotations, version 0 is implicit):
```scala
import org.apache.flinkx.api._

sealed trait Event
case class View(ts: Long) extends Event
case class Purchase(price: Double) extends Event
case class Click(identifier: String, sessionId: Int, unused: String, history: List[ClickEvent]) extends Event
case class ClickEvent(date: String)

```

Current source code looks like this to reflect the evolutions:
```scala mdoc:reset-object
import org.apache.flinkx.api._

@version(1)
@renamed(since = 1, "Event")
@deletedClasses(since = 1, throwOnInstance = false, "Purchase")
@postDeserialize(updateAction)
sealed trait Action

@renamed(since = 1, "View")
case class Web(ts: Long) extends Action

@version(2)
@deletedFields(since = 1, "unused", "history")
@deletedClasses(since = 1, "ClickEvent")
@postDeserialize(updateClick)
case class Click(
    @renamed(since = 1, "identifier") id: String,
    @added(since = 2) ts: Long = System.currentTimeMillis(),
    @transformed(since = 1, intToString) sessionId: String
) extends Action

def intToString(i: Int): String = i.toString
def updateClick(formerVersion: Int, click: Click): Click =
  if (formerVersion == 0) click.copy(sessionId = click.sessionId + click.ts)
  else click
def updateAction(formerVersion: Int, action: Action): Action = action match {
  case Web(ts) => Web(ts + 1)
  case null    => Click(java.util.UUID.randomUUID().toString, sessionId = "Former Purchase")
  case e @ _   => e // ignore other cases
}
```

**Available annotations:**

| Annotation                                                           | Where                          | Effect                                                                                                                                                                                                                                                   |
|----------------------------------------------------------------------|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `@version(n)`                                                        | ADT                            | Declares the current schema version (`n >= 0`); opt in the ADT to evolution                                                                                                                                                                              |
| `@added(since = n)`                                                  | case class field               | Field was added in version `n`; requires a default value                                                                                                                                                                                                 |
| `@renamed(since = n, "oldName")`                                     | case class field, ADT, subtype | Field or class was formerly known under `"oldName"` before version `n`, or lived at another location (see [former class name resolution](#former-class-name-resolution))                                                                                 |
| `@transformed(since = n, mapper)`                                    | case class field               | Field's type changed in version `n`; `mapper` converts the formerly serialized value to the current type                                                                                                                                                 |
| `@deletedFields(since = n, "a", "b")`                                | case class                     | Fields `"a"` and `"b"` were deleted in version `n`; their serialized state is dropped on restore                                                                                                                                                         |
| `@deletedClasses(since = n, throwOnInstance = true, "OldClass1", …)` | ADT                            | Subtypes that have been removed, or field types that were referenced by a now-deleted field (see [former class name resolution](#former-class-name-resolution)). Throws by default when encountering an instance of deleted class during deserialization |
| `@postDeserialize(mapper)`                                           | ADT                            | Applies the `mapper` function taking as parameters the former version and the current ADT instance after its deserialization                                                                                                                             |

Field evolutions are applied in ascending `since` order. Within a single version, evolutions are applied in this canonical pipeline:
Delete → Rename → Transform → Add. This ordering enables annotation combinations such as:
* Rename a field and transform its value.
* Delete a field (its serialized state is dropped) and re-add a field with the same name from a default value.

> [!NOTE]
> You can clean up old evolution annotations as long as you don't restore a checkpoint of an older version. For example with `Click`:
> * if you didn't keep any v0 checkpoint, you can retain only `@version(2)` and `@added(since = 2)` annotations.
> * if you only have v2 checkpoints, you can remove all annotations, even `@version(2)`. Current `Click` will then be treated as a version 0.

#### Former class name resolution

Former class names in `@renamed` and `@deletedClasses` annotations must be in binary name format where nested classes are separated by `$` instead of dots.
See [Binary names](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/ClassLoader.html#binary-name) of `java.lang.ClassLoader` javadoc for more details or [JLS 13.1](https://docs.oracle.com/javase/specs/jls/se21/html/jls-13.html#jls-13.1) for the complete definition.

The former class names can be relative or absolute paths.

Any path referencing a package (dot-separated in binary name format) is an absolute path.
Start with a `/` to force absolute path (should be useful to reference unnamed package only).

Other paths are resolved relatively to the parent of the annotated class (i.e. next to the annotated class).
They can contain `$` to reference nested classes.

For example, given this version 0:
```scala
package org.example

sealed trait Brood

object Brood {
  case object Puppy extends Brood
  case object Kitten extends Brood
}
```

Version 1 looks like this to reflect the renames and deletions:
```scala
package org.example

@version(1)
@renamed(since = 1, "Brood")
@deletedClasses(since = 1, "Brood$Puppy")
sealed trait Animal

object Animal {
  @renamed(since = 1, "org.example.Brood$Kitten")
  case object Cat extends Animal
}
```

#### Outdated former evolution

It may arrive former evolutions get overwritten by newer evolutions. The rule of thumb is to describe how to restore a current state from these former checkpoints versions.

**Rename then delete a field:**

Given this version 0:
```scala
case class Dog(name: String, kind: String)
```
And this version 1:
```scala mdoc:reset-object
import org.apache.flinkx.api._

@version(1)
case class Dog(
  name: String,
  @renamed(since = 1, "kind") breed: String
)
```
If you want to delete `breed` field but still be able to restore from v0 and v1 checkpoints:
```scala mdoc:reset-object
import org.apache.flinkx.api._

@version(2)
@deletedFields(since = 1, "kind")
@deletedFields(since = 2, "breed")
case class Dog(name: String)
```

**Delete then recreate a class:**

## Compatibility

This project uses a separate set of serializers for collections, instead of Flink's own TraversableSerializer. So probably you
may have issues while migrating state snapshots from TraversableSerializer to this project serializers.
