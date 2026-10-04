# State Descriptor Declaration Patterns

## Why this document exists

Annotation-based schema evolution only applies if the evolution rules of the current source code are
available on the TaskManager **before** the state is restored. Whether that holds depends entirely on
*how the application declares its `StateDescriptor`s* — something the library cannot observe and must
not assume.

This document extracts the declaration patterns found in a survey of production Flink applications, so
that the library can be designed against a catalogue of shapes rather than against any particular
codebase. It describes **what exists in the wild**, not what should be recommended; the remediation
options are a separate discussion.

## Method

Four independent Flink application codebases were inventoried (≈1500 Scala files, Scala 2.13, Flink 1.x
and 2.x, all using this library for type information). Every construction of a `StateDescriptor` and
every `getRuntimeContext.getXState(...)` call site was classified. Roughly 110 state declarations were
found in total.

Counting differs slightly between codebases — some count descriptors, some count state handles, and one
descriptor may back several handles — so the distribution below is indicative, not exact.

## The invariant that decides every case

Java serialization carries **instance fields, not statics**, and a TaskManager deserializes the operator
chain (functions, their fields, and the `StreamConfig` serializers) before it restores any state.

> A state's evolution rules reach the TaskManager in time if, and only if, the descriptor — or at least
> the `TypeInformation` it was built from — is reachable from the **serialized function instance**.

Two consequences that are easy to get wrong:

- A `lazy val` field is not evaluated when the client serializes the function, so nothing it references
  is part of the serialized graph.
- A `val` in a companion `object` is a per-JVM singleton. Even when the client initialises it (for
  example because it passes the descriptor to some graph-level API), the TaskManager initialises **its
  own copy**, at first touch, which is inside `open()` or `processElement` — after the restore.

## Pattern catalogue

| # | Pattern | Descriptor built | TypeInformation derived | Rules arrive in time |
|---|---|---|---|---|
| P1 | Companion `val` + handle in a `lazy val` | companion init, on TM | on TM | **no** |
| P2 | Companion `val` + handle in `open()` / `initializeState()` | companion init, on TM | on TM | **no** |
| P3 | Descriptor built inline in `open()` | on TM | on TM | **no** |
| P4 | Descriptor built inline in a `lazy val` body | on TM | on TM | **no** |
| P5 | Descriptor from a parameterised `def` factory | on TM, once per call | on TM | **no** |
| P6 | Eager `val` field of the function | on client | on client | yes |
| P7 | Descriptor as a constructor parameter | on client | on client | yes |
| P8 | Descriptor passed to a graph-level API | on client | on client | yes |
| P9 | `TypeInformation` in an instance field, descriptor built on TM | on TM | on client | yes |
| P10 | Descriptor built from a raw `Class[T]` | anywhere | never | n/a — no derivation at all |

### P1 — Companion `val`, handle in a `lazy val`

The most common shape by a wide margin.

```scala
class MyFunction extends KeyedProcessFunction[K, I, O] {
  @transient private lazy val state = getRuntimeContext.getState(MyFunction.Descriptor)
}
object MyFunction {
  private val Descriptor = new ValueStateDescriptor("s", implicitly[TypeInformation[MyState]])
}
```

Only the *handle* is lazy, which is why this pattern reads as safe and is usually intended as such. It
is not: nothing in the serialized `MyFunction` instance references `Descriptor`, so `MyFunction$`
initialises on the TaskManager, at the first record.

### P2 — Companion `val`, handle taken in `open()`

```scala
class MyFunction extends KeyedProcessFunction[K, I, O] {
  private var state: ValueState[MyState] = _
  override def open(p: OpenContext): Unit = state = getRuntimeContext.getState(MyFunction.Descriptor)
}
```

Same verdict as P1, reached through `open()` rather than through a lazy field.

### P3 — Descriptor built inline in `open()`

```scala
override def open(p: OpenContext): Unit =
  state = getRuntimeContext.getState(new ValueStateDescriptor("s", implicitly[TypeInformation[MyState]]))
```

The textbook Flink idiom, and the one the documentation of most projects shows.

### P4 — Descriptor built inline in a `lazy val` body

```scala
@transient private lazy val state =
  getRuntimeContext.getState(new ValueStateDescriptor("s", implicitly[TypeInformation[MyState]]))
```

Indistinguishable from P1 at a glance; the difference is whether the argument is a pre-existing `val` or
a `new` expression. Both classes of site were found in the same file in one codebase.

### P5 — Descriptor from a parameterised `def` factory

```scala
private def descriptor(cfg: Config) =
  new ValueStateDescriptor(s"s/${cfg.hash}", implicitly[TypeInformation[MyState]])

@transient private lazy val state = getRuntimeContext.getState(descriptor(conf))
```

Notable because it **cannot simply be hoisted to a field**: the state name depends on runtime
configuration, which is precisely why it was written as a `def`. Any remediation that assumes a
descriptor can be lifted to a client-side `val` has to account for this shape.

A generic variant of the same problem appears when a shared base trait exposes an abstract descriptor
factory used by several concrete operators.

### P6 — Eager `val` field of the function

```scala
class MyFunction extends KeyedProcessFunction[K, I, O] {
  private val descriptor = new ValueStateDescriptor("s", implicitly[TypeInformation[MyState]])
}
```

Built and derived on the client, serialized with the function.

### P7 — Descriptor as a constructor parameter

```scala
class MyFunction(descriptor: ValueStateDescriptor[MyState]) extends KeyedProcessFunction[K, I, O]
```

The strongest shape: the graph-building code owns the construction, and the instance travels as a field.
Rare in the surveyed codebases — a handful of sites.

### P8 — Descriptor passed to a graph-level API

```scala
stream.broadcast(MyFunction.BroadcastDescriptor)
```

The descriptor instance ends up in the job graph through the API call itself, independently of how the
function reaches it afterwards. Broadcast state is naturally in this shape.

### P9 — `TypeInformation` in an instance field, descriptor built on the TM

```scala
abstract class MyFunction[S] extends KeyedProcessFunction[K, I, O] {
  protected val stateInfo: TypeInformation[S]      // overridden with a concrete val per subclass
  @transient private lazy val state = getRuntimeContext.getState(new ValueStateDescriptor("s", stateInfo))
}
```

Worth isolating as its own pattern: the descriptor is allocated on the TaskManager, yet the evolution
rules still arrive, because the *type information* is a non-transient instance field derived on the
client. It shows that the descriptor is not what matters — the derived type information is.

### P10 — Descriptor built from a raw `Class[T]`

```scala
new ValueStateDescriptor("s", classOf[MyState])
```

Bypasses type information derivation entirely, so no evolution exists to lose and none can ever be
applied. Found only in unused helper code, but it is a shape the library should expect to meet.

## Distribution

Ordering by frequency across the four codebases:

1. **P1 dominates** — the large majority of state handles.
2. **P2, P3, P4** together account for most of the rest.
3. **P5** is rare but structurally important: it resists the obvious fix.
4. **P6, P7, P8, P9** are a small minority — on the order of a dozen sites out of ~110, and entirely
   absent from one of the four codebases.

In other words: under the invariant above, the **overwhelming majority of state declarations in real
applications do not carry their evolution rules to the TaskManager**, and the patterns that do are the
exception rather than the convention.

## Adjacent observations

- **Window accumulators** (`aggregate`, `reduce`) resolve their type information at the call site on the
  client, so accumulator state is in the safe family by construction.
- **Operator state** (`CheckpointedFunction`, `getOperatorStateStore`) was almost absent from the survey;
  the problem is dominated by keyed state.

## What the library may assume

Derived from the catalogue, and stated as requirements rather than solutions:

1. It **may not** assume the state serializer, the descriptor, or the state's type information is
   reachable from the job graph. P1–P5 are the norm.
2. It **may not** assume a descriptor can be lifted to a client-side value: P5 has a legitimate runtime
   dependency in the state name.
3. It **may** assume the application's records are typed with derived type information — record
   serializers do travel in the `StreamConfig` — but not that the state types appear among them.
4. It **should** assume that whatever it requires of the declaration style will be violated somewhere,
   and therefore that a missing declaration must be detected and reported rather than silently ignored.
