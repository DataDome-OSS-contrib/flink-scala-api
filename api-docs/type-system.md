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

You can change the schema of an ADT and restore a savepoint of the former schema. An ADT is a case class, a case
object, a sealed trait or a Scala 3 enum. Some changes need nothing. The other changes need evolution annotations.

Two words tell the two sides of a restore apart:
* `former` qualifies an item as the savepoint records it: version, class name, field, subtype or enum value.
* `current` qualifies an item as the source code of the job that restores the savepoint declares it.

### Built-in compatibility

Since release 2.4.0, these changes need no annotation:
* Case class: reorder the fields.
* Sealed trait: reorder the subtypes, or add subtypes.
* Scala 3 enum: reorder the values, or add values.

For all the other changes, use the evolution annotations.

### Evolution annotations

To declare the evolutions of an ADT:
1. Import `org.apache.flinkx.api.evolution._`.
2. Put `@version(n)` on the ADT. Start `n` at 1, and increase it at each change of the schema.
3. Declare each change with an evolution annotation. Its `since` is the first version that contains the change.
4. Make the companion of the ADT extend `Evolving`, with `val evolutions = Evolutions[T]`.

```scala
package org.example

import org.apache.flinkx.api.evolution._

@version(2)
@renamed(since = 2, "FormerOrder")
@postEvolution(Order.fix)
case class Order(id: String, @added(since = 2) note: String = "none")

object Order extends Evolving[Order] {
  val evolutions = Evolutions[Order]

  private[example] def fix(formerVersion: Int, order: Order): Order = order // Visible where Order is declared
}
```

* An ADT without `@version` has the version 0. Thus, you can add `@version(1)` to an ADT that has savepoints.
* A case object has no companion: its sealed trait declares its evolutions.
* A Scala 3 enum value with parameters has the version of its enum, whose companion declares its evolutions. It accepts
  the annotations of a case class.
* If the companion of a versioned ADT does not extend `Evolving`, the derivation of its type information does not
  compile.
* A mapper must be visible from the declaration of the ADT.

| Annotation                                     | On                                         | Effect                                                                                                                                                                      |
|------------------------------------------------|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `@version(n)`                                  | ADT                                        | Declares the current schema version `n`, 0 or higher.                                                                                                                       |
| `@added(since = n)`                            | case class field                           | The field is added in version `n`. It must have a default value, which the migration evaluates for each instance.                                                           |
| `@renamed(since = n, "formerName")`            | case class field, enum value, ADT, subtype | Before version `n`, the field, the enum value or the class has the name `formerName`. See [former class names](#former-class-names).                                        |
| `@transformed(since = n, mapper)`              | case class field                           | The value of the field, and possibly its type, changes in version `n`. The mapper converts the former value to the current value.                                           |
| `@deletedFields(since = n, "a", "b")`          | case class                                 | The fields `a` and `b` are deleted in version `n`. The migration discards their former values.                                                                              |
| `@deletedClasses(since = n, "FormerClass", …)` | ADT                                        | The classes are deleted in version `n`: subtypes, enum values, or classes of deleted fields. An instance of them throws, or reads as `null` with `throwOnInstance = false`. |
| `@postEvolution(mapper)`                       | ADT                                        | After the other evolutions, the mapper gets the former version and the migrated instance. It gives the instance to keep.                                                    |

The migration applies the field evolutions in the order of their `since`. For one `since`, the order is: delete,
rename, transform, add. Thus, in one version, you can:
* rename a field, then transform its value;
* delete a field, then add a field with the same name.

### Example

The former schema, without annotations (version 0):
```scala
import org.apache.flinkx.api._

sealed trait Event
case class View(ts: Long) extends Event
case class Purchase(price: Double) extends Event
case class Click(identifier: String, sessionId: Int, unused: String, history: List[ClickEvent]) extends Event
case class ClickEvent(date: String)

```

The current schema:
```scala mdoc:reset-object
import org.apache.flinkx.api.evolution._

@version(1)
@renamed(since = 1, "Event")
@deletedClasses(since = 1, throwOnInstance = false, "Purchase")
@postEvolution(updateAction)
sealed trait Action

@version(1)
@renamed(since = 1, "View")
case class Web(ts: Long) extends Action

@version(2)
@deletedFields(since = 1, "unused", "history")
@deletedClasses(since = 1, "ClickEvent")
@postEvolution(updateClick)
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
  case e @ _   => e // The other subtypes do not change
}
```

In this example:
* `Event` takes the name `Action`, and `View` takes the name `Web`.
* `Purchase` is deleted: its instances read as `null`, and `updateAction` replaces them.
* `Click` deletes two fields, renames a field and transforms a field in version 1. It adds `ts` in version 2.

> [!NOTE]
> You do not need the annotation of an obsolete evolution: if its `since` is equal to or lower than the version of all
> the savepoints that you restore. For example, with `Click`:
> * with savepoints of version 1, keep only `@version(2)` and `@added(since = 2)`;
> * with savepoints of version 2 only, you need no annotation, not even `@version(2)`. `Click` then has the version 0.


### Former class names

`@renamed` and `@deletedClasses` take former class names:
* A former class name is a [binary name](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/ClassLoader.html#binary-name):
  a `$` separates a nested class from its enclosing class.
* A name with a package is absolute. A name that starts with `/` is absolute too: use it for the unnamed package.
* The library resolves the other names from the enclosing class, or from the package, of the annotated class.
* The class of a case object ends with `$`. `@renamed` adds it when you omit it. In `@deletedClasses`, a name without
  `$` refers to the class and to the case object of this name.
* A subtype has its own versions, which do not depend on the versions of its sealed trait. A renamed subtype declares its
  own `@version`, and the `since` of its rename counts in its own versions.
* The `since` of a deleted class counts in the versions of this class, 1 if it had no `@version`.

Version 0:
```scala
package org.example

sealed trait Brood

object Brood {
  case object Puppy extends Brood
  case object Kitten extends Brood
}
```

Version 1 renames `Brood` and `Kitten`, and deletes `Puppy`:
```scala
package org.example

@version(1)
@renamed(since = 1, "Brood")
@deletedClasses(since = 1, "Brood$Puppy")
sealed trait Animal

object Animal extends Evolving[Animal] {
  val evolutions = Evolutions[Animal]

  @version(1)
  @renamed(since = 1, "org.example.Brood$Kitten")
  case object Cat extends Animal
}
```

### Successive evolutions

Declare all the evolutions that the restore of each savepoint needs. For example:
```scala
case class Dog(name: String, kind: String)
```
Version 1 renames `kind` to `breed`:
```scala mdoc:reset-object
import org.apache.flinkx.api.evolution._

@version(1)
case class Dog(
  name: String,
  @renamed(since = 1, "kind") breed: String
)
```
Then version 2 deletes `breed`. To restore the savepoints of versions 0 and 1, declare the two deletions:
```scala mdoc:reset-object
import org.apache.flinkx.api.evolution._

@version(2)
@deletedFields(since = 1, "kind")
@deletedFields(since = 2, "breed")
case class Dog(name: String)
```

A class can reuse the former class name of a renamed or deleted class. This class must declare a version equal to or
higher than the `since` of the rename or of the deletion. Else the submission check fails with a
`FormerClassConflictException`. A class without an evolving companion has the version 0. For example:
```scala
sealed trait Pet
case class Horse(name: String) extends Pet
```
Version 1 renames `Horse` to `Pony`, and a new `Horse` reuses the name:
```scala
@version(1)
sealed trait Pet
object Pet extends Evolving[Pet] { val evolutions = Evolutions[Pet] }

@version(1)
@renamed(since = 1, "Horse")
case class Pony(name: String) extends Pet
object Pony extends Evolving[Pony] { val evolutions = Evolutions[Pony] }

@version(1) // Version 1 at least: the restore does not confuse its data with the data of the former Horse
case class Horse(name: String) extends Pet
object Horse extends Evolving[Horse] { val evolutions = Evolutions[Horse] }
```

### Restore

* When the Scala `StreamExecutionEnvironment` submits the job, it checks the declarations of the evolutions. A wrong
  declaration stops the submission.
* The evolutions are in the jars of the job, not in the job graph: you can build a `StateDescriptor` anywhere. At the
  first restore, the library finds the evolving companions in these jars, and initializes them.
* A former class name that no class has and no evolution declares stops the restore with a
  `MissingEvolutionException`.
* A rollback is incompatible if the former version is higher than the current version.
* A savepoint written before release 2.4.0 records no field names: the restore reads the case class fields by position,
  without field evolutions or post-evolution mappers. Restore it once with release 2.4.0 without a schema change, then
  take a savepoint.
* Keep `flink-scala-api` in the jar of the job: each job then has its own registry of evolutions. In the `lib`
  directory of the cluster, the jobs of a TaskManager share the registry. It stays correct, but it keeps the class
  loaders of the stopped jobs.

## Compatibility

This project uses a separate set of serializers for collections, instead of Flink's own TraversableSerializer. So probably you
may have issues while migrating state snapshots from TraversableSerializer to this project serializers.
