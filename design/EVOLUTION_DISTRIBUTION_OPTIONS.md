# Distribution of the Evolutions to the TaskManagers

## Purpose

The restore needs the evolutions of the current source code before it reads the state. This note explains how the
evolutions get to the TaskManagers. A survey of the state declarations in real applications sets the requirements, then
the facts of the restore and the options lead to the decision. The facts come from the Flink sources, version 2.0 and
version 1.20.1.

## 1. State descriptor declaration patterns

The survey covers four Flink applications: about 1500 Scala files, Scala 2.13, Flink 1.x and 2.x, and about 110 state
declarations. The counts are approximate.

### Invariant

Java serialization keeps the instance fields, not the static fields. A TaskManager deserializes the operator chain
before it restores the state. Thus the evolutions arrive in time only if the serialized function instance refers to
the descriptor, or to the type information of the state.

- The client does not evaluate a `lazy val` field: the serialized function does not contain its value.
- A `val` of a companion object is a singleton for each JVM. The TaskManager initializes its own copy at the first
  use, in `open()` or `processElement`, after the restore.

### Patterns

| #   | Pattern                                                       | Type information derived on | Evolutions in time |
|-----|---------------------------------------------------------------|-----------------------------|--------------------|
| P1  | Companion `val` descriptor, state handle in a `lazy val`      | TaskManager                 | no                 |
| P2  | Companion `val` descriptor, state handle in `open()`          | TaskManager                 | no                 |
| P3  | Descriptor built in `open()`                                  | TaskManager                 | no                 |
| P4  | Descriptor built in a `lazy val`                              | TaskManager                 | no                 |
| P5  | Descriptor from a `def` with a parameter of the configuration | TaskManager                 | no                 |
| P6  | Descriptor in a `val` field of the function                   | client                      | yes                |
| P7  | Descriptor as a constructor parameter of the function         | client                      | yes                |
| P8  | Descriptor given to a job graph API, for example `broadcast`  | client                      | yes                |
| P9  | Type information in a field, descriptor built in `open()`     | client                      | yes                |
| P10 | Descriptor built from a `Class[T]`                            | never                       | no evolution       |

P1, the most frequent pattern, looks safe because only the state handle is lazy:

```scala
class MyFunction extends KeyedProcessFunction[K, I, O] {
  @transient private lazy val state = getRuntimeContext.getState(MyFunction.Descriptor)
}
object MyFunction {
  private val Descriptor = new ValueStateDescriptor("s", implicitly[TypeInformation[MyState]])
}
```

The serialized `MyFunction` does not refer to `Descriptor`: the TaskManager initializes `MyFunction$` at the first
record.

P5 cannot move to a field: its state name depends on the runtime configuration. P9 shows that the type information,
not the descriptor, decides.

### Distribution

1. P1 is the large majority.
2. P2, P3 and P4 are most of the other declarations.
3. P5 is rare, but it prevents the obvious solution.
4. P6 to P9 are about 12 declarations of 110. One application has none.

Most real state declarations do not bring their evolutions to the TaskManager in time.

Window accumulators (`aggregate`, `reduce`) get their type information on the client: they are safe. Operator state
was almost absent from the survey.

### Requirements

1. Do not expect the job graph to contain the serializer, the descriptor or the type information of a state.
2. Do not expect that a descriptor can move to a value of the client (P5).
3. Expect derived type information for the record types, but not for the state types.
4. Expect code that does not follow a declaration rule. Detect and report a missing declaration.

## 2. Restore facts

A TaskManager does these steps in this order:

1. It creates the user class loader, and deserializes the `ExecutionConfig`.
2. It deserializes the operator chain: the operator factories, the user functions and the `StreamConfig` serializers.
3. It restores the state backends, then calls `initializeState`, then `open`.

Before the restore, only Java deserialization runs user code: `readObject` and the static initializers. A companion
object is a different class (`Foo$`): the deserialization of a `Foo` does not initialize it.

Some restores are eager. They deserialize all the values with the restored serializers in step 3, before any user code:

- heap keyed state;
- operator state (list, union and broadcast), also with RocksDB;
- heap timers.

The restore of RocksDB keyed state is lazy. It copies bytes, and Flink resolves the schema compatibility at `getState`.
Then `compatibleAfterMigration` reads the values with the restored serializer and writes them with the current
serializer.

Other facts:

- The serializer snapshots resolve their former class names in `readSnapshot`, thus during an eager restore.
- ForSt does not support `compatibleAfterMigration` in Flink 2.0: a migration fails on ForSt.
- The first checkpoint after a restore snapshots the restored serializer of a state that the job does not access.
- Java reflection does not see the Scala static annotations: they exist only in the Scala 2 pickle or in TASTy.
- The compilers do not write `PermittedSubclasses`: the subtypes of a sealed trait are not known at runtime.
- The user class loader gives the URLs of the jars of the job.

A rename adds a difficulty: the snapshot records the former class name, but the declaration is on the current class.
An index from the former class names to the current classes is necessary.

## 3. Options

The requirements exclude the options that depend on the pattern of the state declaration: O1, O2 and O4. They also
exclude O7, which asks for a declaration far from the ADT.

| Option | Principle                                                              | Verdict                                                         |
|--------|------------------------------------------------------------------------|-----------------------------------------------------------------|
| O1     | Resolve the former class names at the first use, not in `readSnapshot` | Lazy restores only. Not kept                                    |
| O2     | Initialize the companions of the user functions on the TaskManager     | P1 and P2 only, wraps the operator factories. Not kept          |
| O3     | Find the derivations in the functions by static analysis               | Rejected: a derivation cannot run out of its method             |
| O4     | Initialize the companions on the client, send the evolutions           | Not P3 to P5. Not kept                                          |
| O5     | Scan the class path of the client for the annotations                  | Rejected: the static annotations are not visible                |
| O6     | Compiler plugin that writes an index of the evolutions                 | All patterns, but two plugins to maintain. Not kept             |
| O7     | Explicit declaration of each ADT in `main` or in `META-INF/services`   | One line for each ADT, far from its annotations. Not kept       |
| O8     | Java annotations, read by reflection                                   | API change, renames not solved. Not kept                        |
| O9     | Scan the jars of the job on the TaskManager                            | Kept, for the companions that extend `Evolving`                 |
| O10    | Companion that declares the evolutions of its ADT                      | Kept                                                            |
| O11    | Stub class under the former class name                                 | Keeps the former names in the code. Not kept                    |
| O12    | Restore placeholder values, migrate them later                         | Rejected: user code gets the placeholder values                 |
| O13    | State backend wrapper                                                  | Rejected: it gives no evolution                                 |
| O14    | Support matrix in the documentation only                               | Minimum level only. Not kept                                    |
| O15    | Offline migration with the State Processor API                         | One operational step for each job. Not kept                     |
| O16    | Resolution on the JobManager                                           | Rejected: the JobManager does not read the serializer snapshots |

## 4. Decision

- **Evolving companion (O10).** The companion of a versioned ADT extends `Evolving` and holds
  `val evolutions = Evolutions[T]`. If it is missing, the derivation of the type information does not compile.
- **Companion scan (O9).** The first lookup of a class loader scans the jars of the job for the companions that extend
  `Evolving`. It initializes them and registers their declarations. The scan reads only the header of the class files.
- **Timing.** The lookup occurs in `readSnapshot`, thus before an eager restore, whatever the pattern of the state.
- **Renames.** The companion of the current class declares the former class name, so the registry finds it.
- **Submission check.** The Scala `StreamExecutionEnvironment` registers the declarations when it submits the job. A
  wrong declaration stops the submission, not the first restore.
- **Failure.** A former class name that no class has and no evolution declares stops the restore with a
  `MissingEvolutionException`.

The decision meets the requirements:

- Requirements 1 to 3: the evolutions come from the jars of the job, not from the job graph. Thus all the patterns,
  P1 to P9, work, and the declaration of a state descriptor has no rule to follow.
- Requirement 4: a missing declaration fails at compile time (no evolving companion), at submission (submission
  check), or at restore (`MissingEvolutionException`).

Consequences:

- The first restore initializes all the evolving companions of the job. An initializer that needs the runtime
  configuration fails there.
- Each class loader has its own registry. In the `lib` directory of the cluster, the registry keeps the class loaders
  of the stopped jobs.
- A migration does not work on ForSt.
