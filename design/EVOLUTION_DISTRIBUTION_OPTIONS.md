# Getting the Evolution Declarations to the TaskManagers

## Why this document exists

[STATE_DESCRIPTOR_PATTERNS.md](STATE_DESCRIPTOR_PATTERNS.md) establishes that most real state declarations (P1–P5)
never carry their evolution rules to the TaskManager in time, and that the library may not constrain how a
`StateDescriptor` is declared. This document is the remediation side: it fixes the facts about *when* a TaskManager
needs the rules and *where* they could come from, then catalogues every option found, with its coverage, its cost and
its failure modes. It ends with the combinations worth considering, not with a decision.

The facts below were checked against the Flink sources in `../flink` (master, `2.0-SNAPSHOT`) and, where it matters,
against the `flink-core` 1.20.1 sources. Line numbers refer to that checkout.

## How the rules are produced today

- The derivation body (`join`/`split` in `TypeInformationDerivation`) is **runtime code**: Magnolia generates the call at
  compile time, but the annotations are instantiated and `Evolutions.register` runs when the derived
  `TypeInformation` expression is *evaluated*. Nothing is registered by merely loading the ADT class.
- The registry is a JVM global keyed by class name. A snapshot resolves the former names it recorded through
  `Evolutions.get(formerName, formerVersion, cl)` **inside `readOuterSnapshot`**, i.e. as soon as the snapshot is read
  (`ScalaCaseClassSerializerSnapshot`, `CoproductSerializerSnapshot`, `ScalaCaseObjectSerializerSnapshot`).
- The annotations are `StaticAnnotation`s: they exist only in the Scala 2 pickle (`ScalaSignature`) or in TASTy. Java
  reflection cannot see them, and a classfile scan cannot find them without a pickle or TASTy reader. Verified with
  `scala-cli` on 2.13.18 and 3.3.8; the same check shows both compilers **do** emit `MethodParameters` (constructor
  parameter names are reflectively available) and neither emits `PermittedSubclasses` (sealed children cannot be
  enumerated at runtime).
- The local diff makes every ADT serializer carry its whole declaration and re-register it in `readObject`. This is
  correct and stays useful, but it only reaches the TaskManager for the types whose serializer travels in the job graph:
  record types and P6–P9.

## What a TaskManager does, in order

1. `Task.doRun` creates the user class loader, then Java-deserializes the `ExecutionConfig` with it
   (`Task.java:630-632`). The `SerializerConfigImpl` inside it carries registered Kryo serializer **instances**
   (`SerializerConfigImpl.java:58,134-140`), Java-serialized; `GlobalJobParameters` is stored as a `Map<String,String>`
   (`ExecutionConfig.java:450-468`), the same in 1.20.
2. `StreamTask.restoreInternal` builds the `OperatorChain` (`StreamTask.java:774-791`): the operator factories, the user
   functions inside them and the `StreamConfig` serializers are Java-deserialized with the user class loader
   (`OperatorChain.java:169,553,693,914`). A `StreamOperatorFactory` gets `StreamOperatorParameters`, hence the
   containing task, its `Environment` and its `ExecutionConfig`.
3. `restoreStateAndGates` → `initializeStateAndOpenOperators` (`StreamTask.java:812,858`): the backends are restored,
   then `initializeState`, then `open`. **The only user code that runs before the restore is Java deserialization**:
   `readObject` hooks and the static initializers of the classes it loads. Companion objects are separate classes
   (`Foo$`) and are not initialized by deserializing a `Foo`.

### Eager restores: the rules are needed during step 3, before any user code

- **Heap keyed state.** `HeapRestoreOperation` reads the `KeyedBackendSerializationProxy`, which calls `readSnapshot` on
  every state serializer snapshot (`HeapRestoreOperation.java:132-135`), then deserializes every value with
  `stateTable.getStateSerializer()` (`StateTableByKeyGroupReaders.java:72-79`). That serializer is
  `StateSerializerProvider.currentSchemaSerializer()`, which, with no new serializer registered yet, is
  `previousSerializerSnapshot.restoreSerializer()` (`StateSerializerProvider.java:149-186`). When the user later calls
  `getState`, `HeapKeyedStateBackend.tryRegisterStateTable` only rejects an `incompatible` result: the objects are
  already in the heap, there is no migration pass.
- **Operator state, whatever the keyed backend.** `OperatorStateRestoreOperation` deserializes list, union and
  broadcast values with the restored serializer (`OperatorStateRestoreOperation.java:222-263`);
  `DefaultOperatorStateBackend` only rejects `incompatible` afterwards. `CheckpointedFunction` and broadcast state are
  therefore in the eager family even on RocksDB.
- **Heap timers.** `InternalTimersSnapshotReaderWriters` restores the key and namespace serializers eagerly
  (`InternalTimersSnapshotReaderWriters.java:220-223`). Both types travel in the `StreamConfig`, so this only bites a
  user-defined window type.

### Lazy restores: the rules are needed at `getState`, after the user's own derivation

- **RocksDB keyed state.** The restore operations copy raw bytes; no state value is deserialized (the only
  `deserialize` call in the restore package is the heap-timers one). At `getState`,
  `updateRestoredStateMetaInfo` first materializes the previous serializer (`getStateSerializer()`, hence
  `restoreSerializer()`), then `updateStateSerializer(new)` runs
  `new.snapshotConfiguration().resolveSchemaCompatibility(old)`, and `compatibleAfterMigration` triggers
  `migrateStateValues`, which reads with the previous serializer and writes with the new one
  (`RocksDBKeyedStateBackend.java:752-797,806`). By then `initializeSerializerUnlessSet` has run on the descriptor
  (`AbstractKeyedStateBackend.java:379-381`), so a P3 derivation, or the P1/P2 companion initialization, has just
  happened on the same thread.
- **ForSt.** In this checkout `compatibleAfterMigration` throws `UnsupportedOperationException("State migration not
  support yet.")` (`ForStKeyedStateBackend.java:433-438`). Evolutions needing a migration cannot work on ForSt yet,
  whatever the library does.
- **First checkpoint after a restore.** `RegisteredKeyValueStateBackendMetaInfo.computeSnapshot` calls
  `getStateSerializer().snapshotConfiguration()` (`RegisteredKeyValueStateBackendMetaInfo.java:260-281`): a state the
  new code never accesses gets its previous serializer materialized *at checkpoint time*. Any lazy resolution must
  survive that call.

### Other verified facts

- `StateDescriptor.typeInfo` is a plain serialized field (`StateDescriptor.java:101`); only the default value has custom
  serialization. P6/P7 really do ship the `TypeInformation`, hence the serializer, hence its evolution.
- The user class loader is a `MutableURLClassLoader`; `SafetyNetWrapperClassLoader.getURLs()` delegates to it
  (`FlinkUserCodeClassLoaders.java:158,228-229`). The jars of a job can be listed from the class loader a snapshot
  receives in `readSnapshot`.
- Snapshot API in 2.x: `readSnapshot(version, in, userCodeClassLoader)`, `restoreSerializer()`,
  `resolveSchemaCompatibility(TypeSerializerSnapshot old)` called on the **new** serializer's snapshot
  (`TypeSerializerSnapshot.java:134`); 1.20 additionally keeps the deprecated old direction.
- On the client, `StreamExecutionEnvironment.getTransformations()` and `getStreamGraph(false)` expose the graph before
  submission, and `UdfStreamOperatorFactory.getUserFunction()` exposes the user function instances
  (`StreamExecutionEnvironment.java:2034,2455`).
- `TypeExtractor` honours a class-keyed `TypeInfoFactory` registry filled from `pipeline.serialization-config`
  (`SerializerConfigImpl.java:459-475`), but the state restore path never goes through `TypeExtractor`.

## The two independent sub-problems

**A — Timing.** Eager restores (heap keyed, operator/broadcast state, heap timers) need the rules before any user code
runs. Lazy restores (RocksDB, ForSt once it migrates) need them at `getState`, which the user's own derivation precedes.

**B — Source.** On a TaskManager the rules can only come from three places: shipped from the client (which only knows
what *it* evaluated), produced on the TaskManager by evaluating a derivation (which needs a trigger the library
controls), or read from class metadata (which needs metadata the compiler does not currently retain). A **rename**
adds a twist to every source: the snapshot knows the *former* name, the declaration sits on the *current* class, and
nothing links the two without a reverse index or a trace left under the former name.

Every option below attacks A, B, or both. None attacks the rename twist for free.

## Option catalogue

Coverage uses the pattern numbers of the survey. "Eager" means heap keyed state and operator state; "lazy" means
RocksDB keyed state.

### Group 1 — Move the resolution later (attacks A only)

**O1. Deferred resolution.** `readSnapshot` only stores the former names, versions and field names; resolution
against the registry moves to `restoreSerializer()` and `resolveSchemaCompatibility()`, or to the first `deserialize`.
- Covers: every pattern on lazy backends. Nothing new on eager restores.
- Pros: no user-facing change; small, local; strictly better than today; the new-direction
  `resolveSchemaCompatibility(old)` even lets the new snapshot hand its resolved `Evolution` to the old one, without any
  registry at all for that path.
- Cons: partial. An unresolved snapshot must still round-trip through `writeSnapshot`, and `restoreSerializer()` must
  tolerate a state the new code never accesses (see "first checkpoint after a restore"): it needs an unresolved
  pass-through serializer that can snapshot itself but throws on `deserialize`, otherwise a renamed-but-unused state
  fails the first checkpoint instead of the restore.
- Effort: low.

**O1'. Fail-fast diagnostics.** Not a fix, a required companion of every other option: when a former name cannot be
resolved during an eager restore, name the state, the operator, the pattern and the remediation instead of a
`ClassNotFoundException` or an arity mismatch from the constructor. A `postDeserialize`-only or `@version`-only change
restores *silently* without rules; the new snapshot can detect at `getState` time that the restored serializer ran
without the rules it now knows, and fail there rather than let migrated-less data through.

### Group 2 — Trigger the user's derivation on the TaskManager before the restore (attacks A for P1/P2)

**O2. Companion pre-initialization on the TaskManager.** Something library-owned that is Java-deserialized in step 2
initializes, by name, the companion of each user function class (`Class.forName(name + "$", true, cl)`) and of its
enclosing objects. The P1/P2 `val Descriptor = new ValueStateDescriptor(..., implicitly[TypeInformation[S]])`
evaluates, the derivation runs, the registry fills, before step 3.
- Vehicles: (a) a library `StreamOperatorFactory` wrapping the real factory, installed by the library's
  `DataStream`/`KeyedStream` API; (b) a per-job carrier object registered through
  `SerializerConfigImpl.registerTypeWithKryoSerializer(dummyClass, carrierInstance)`, deserialized in step 1 with a
  client-computed list of companion names (see O4); (c) the record serializers already in the graph, made to carry
  the list.
- Covers: P1, P2 (companion `val`s only — a companion `lazy val` is not evaluated by initialization), P9. Not P3, P4,
  P5, nor descriptors held in unrelated objects unless the class's bytecode is scanned for `GETSTATIC X$.MODULE$`
  references and those are initialized too (broader, riskier).
- Pros: zero user change on the dominant pattern; semantically neutral, the companion would have initialized on the
  TaskManager anyway, just later; the library already ships an ASM-based `ClosureCleaner`, so bytecode inspection is
  not new territory.
- Cons: (a) only sees operators created through the library wrappers, must delegate every `StreamOperatorFactory`
  sub-interface (`CoordinatedOperatorFactory`, `YieldingOperatorFactory`, `ProcessingTimeServiceAware`, ...) and changes
  the factory class recorded in the graph; (b) abuses the Kryo registration channel and depends on it staying
  Java-serialized in 2.x; early initialization of arbitrary enclosing objects may have side effects (a `Config`
  object connecting somewhere) and P5 is exactly the shape where the companion may need runtime configuration.
- Effort: medium.

**O3. Static analysis of the function to find the derivation sites.** Read `open`, the lazy-val initializers and
`processElement` with ASM to locate `TypeInformation` derivations and evaluate them ahead of time.
- Rejected: locating a call site is possible, evaluating it out of its method is not. Listed so it is not reopened.

### Group 3 — Compute the rules on the client and ship them (attacks B for what the client can see)

**O4. Client-side pre-initialization plus a shipped manifest.** In the library's `execute()`, walk
`getTransformations()`/`getStreamGraph(false)`, take every `UdfStreamOperatorFactory.getUserFunction()`, initialize
its companion and enclosing objects on the client (O2's trick, run where `main` runs), then ship the whole client
registry to the TaskManagers.
- Vehicles: the Kryo carrier (O2-b, one copy per job, `readObject` re-registers in step 1); the library operator
  factory wrapper (O2-a, carrying the declarations as a field); every record serializer in the graph (N copies, no new
  channel); `GlobalJobParameters` as a base64 blob (string channel, needs an operator wrapper to read it in step 2).
- Covers: P1, P2, P6–P9 and every record type, on eager and lazy restores alike. Not P3, P4, P5: their expressions
  never evaluate on the client.
- Pros: the rules are computed once, on the client, with the user's own implicit scope; deterministic; the manifest is
  exactly the `AdtDeclaration` objects the diff already makes serializable.
- Cons: same wrapper or channel caveats as O2; walking the transformations means handling each `Transformation`
  subtype (one-input, two-input, broadcast, sink v2, legacy sink, ...), with a silent miss being a safe no-op.
- Effort: medium.

**O5. Client-side classpath scan.** Find every evolved ADT in the user jar on the client and derive from there.
- Rejected as stated: static annotations are invisible to a scan, and a runtime `Class[_]` cannot feed a compile-time
  derivation. It only works as "O9 run on the client", i.e. after O8, and then brings nothing O9 does not.

### Group 4 — A build-time index (attacks B in full, including renames)

**O6. Compiler plugin.** A plugin for Scala 2.13 and Scala 3 sees every `@version`-annotated ADT at compile time and
generates, next to it, a synthetic entry (`Foo$EvolutionIndex implements EvolutionIndex`) whose body is a call to a
rules-only macro `Evolutions.declare[Foo]` (annotations, field names, defaults and mappers, no field typeclasses), plus
an index (`META-INF/services/...EvolutionIndex`, merged with the existing file to survive incremental compilation;
`sbt-assembly` concatenates services files by default). On the TaskManager, `Evolutions` runs the index of a class
loader at first need, in `readSnapshot`, before any eager restore.
- Covers: everything, every pattern, every backend, renames included.
- Pros: no constraint on user code; the index can also carry compile-time validation; the rules-only macro is the same
  building block O7 and O2/O4 need.
- Cons: two plugins to write and maintain, released per Scala minor (Scala 2 plugins are binary-bound to the compiler
  version); a `addCompilerPlugin` line in every user build, and the plugin's absence must be detected (a `@version`
  seen by the derivation macro with no index entry can fail compilation); the incremental-compilation merge leaves
  stale entries that the runtime must tolerate; the derivation of a synthesized call site does not see the user's
  implicits, which is fine for the *rules* (they only depend on annotations, defaults and mappers) but forbids using
  it for the full `TypeInformation`.
- Effort: high.

**O6'. Build-tool plugin.** An sbt/Mill/Gradle plugin reading TASTy or pickles after compilation and generating the
index.
- Cons: one plugin per build tool, a second compilation pass for the generated declarations; strictly less attractive
  than O6.

**O6''. Macro annotation.** Make `@version` (or a new `@evolved`) a macro annotation that adds the registration to the
companion at expansion time.
- Cons: Scala 2.13 has `-Ymacro-annotations`, but Scala 3's `MacroAnnotation` is experimental in 3.3 LTS; and it only
  provides code, not a trigger: someone must still initialize the companion (see O10), so renames stay unsolved.

### Group 5 — Explicit, central registration by the user (a different constraint than the descriptor one)

**O7. `Evolutions.declare[T]` in one place.** The rules-only macro of O6, called by the user once per evolved ADT:
- (a) in `main`, before `execute()`, shipped through O4's channel; or
- (b) in an `EvolutionsProvider` implementation listed in `META-INF/services`, run by the library on the TaskManager
  at first need, through `ServiceLoader.load(..., userCodeClassLoader)`; or
- (c) as a list of ADT class names in the job configuration, with the library initializing the named companions on
  the TaskManager (needs O10's self-registering companions to have any effect).
- Covers: everything, including renames (the *current* class is what the user names).
- Pros: simple, explicit, no plugin, both backends.
- Cons: a constraint on user code, milder than the descriptor one (central, one line per ADT, not per state) but a
  constraint; a forgotten declaration is only detectable when the restore fails (class not found, arity mismatch) or
  through O1' at `getState` time; duplicates knowledge the annotations already carry.
- Effort: low.

### Group 6 — Read the rules from class metadata on the TaskManager (attacks B without evaluation)

**O8. Java annotations plus a reflective rules reader.** Redefine the evolution annotations in Java with `RUNTIME`
retention. On an unresolved former name that still loads, the TaskManager reads `@version`, `@renamed`,
`@deletedFields`, `@deletedClasses`, `@added` reflectively, takes constructor parameter names from
`MethodParameters`, defaults from `apply$default$N`, and feeds the existing `EvolutionBuilder`.
- Covers: field-level evolutions and deletions on any name that still exists, eager and lazy alike. Not top-level
  renames (needs O9 or O11). Renamed *subtypes* need either the declaration moved to the parent
  (`@renamedMembers` on the sealed trait, since children cannot be enumerated) or a narrow jar scan of the parent's
  package.
- Pros: no plugin, no registration for the unrenamed majority; works for operator state and heap.
- Cons: an API break on a feature just released: Java annotations cannot hold the `postDeserialize`/`transformed`
  lambdas, which become `Class[_ <: Mapper]` or method references; Magnolia cannot instantiate Java annotations, so
  both macros must read annotation arguments from trees; a second rules reader (reflective) that must stay equivalent
  to the first (macro); still incomplete for renames.
- Effort: high.

**O9. Runtime jar scan on the TaskManager.** Once per class loader, list the jars of the `MutableURLClassLoader`, scan
the constant pool of each class (flink-shaded ASM, or a minimal parser) for the library's annotation descriptors,
restricted to configured package prefixes, and build the reverse index of `@renamed`.
- Requires O8 (nothing to find in a classfile otherwise).
- Covers: renames, therefore everything when combined with O8.
- Pros: zero configuration for the user beyond an optional package filter.
- Cons: scanning a fat jar takes seconds without a filter; directories and nested jars must be supported for tests,
  IDEs and some deployments; the scan can only see classes physically present in the jars.
- Effort: medium, on top of O8.

**O10. Self-registering companions, triggered by name.** The ADT's companion registers its own rules when it
initializes (`object Dog extends Evolved[Dog]`, where `Evolved[T]` takes an implicit or given built by the rules-only
macro of O6; the `semiauto` style, `implicit val ti = deriveTypeInformation[Dog]` in the companion, already has this
effect). The library, when a snapshot names a former class it cannot resolve, initializes that class's companion by
name and retries: the *snapshot pulls the registration in* during the restore, before any eager deserialization,
whatever the backend and the descriptor pattern. A sealed trait's companion declares the whole family, so a renamed or
deleted subtype resolves through its parent.
- Covers: every pattern, eager and lazy, for every name that still exists; renamed subtypes through the parent;
  `@deletedClasses` field types through the enclosing class. Not a renamed *top-level* state type (its former name
  loads nothing), unless combined with O9 scanning for classes implementing the marker, or O11.
- Pros: no plugin, no Java annotations, one line per evolved ADT at the *declaration* site (the same place as the
  annotations), free for `semiauto` users; the derivation macro can require the companion to extend the marker when
  `@version` is present, turning a forgotten registration into a compile error.
- Cons: a constraint on the ADT declaration, mild but real; relies on companion initialization order (a companion
  whose initializer needs runtime configuration would fail during restore); top-level rename still open.
- Effort: low to medium.

**O11. Forwarding stub under the former name.** `@movedTo[NewName] class OldName` (or an empty object) keeps a loadable
class under the former name; resolving a rename then starts from something that loads, and O8 or O10 do the rest.
- Pros: trivial mechanism, no index, no scan.
- Cons: the point of a rename is usually to get rid of the old name; leaves stubs in the code base for as long as old
  savepoints must restore; covers only renames.

### Group 7 — Change the restore mechanics so that the rules are not needed during the restore

**O12. Two-phase restore with an opaque intermediate value.** The restored serializer reads former values into a
library placeholder (former name, version, field values); the conversion happens when the new serializer arrives.
- Rejected: the heap `StateTable` and the operator `ListState` hand the stored objects straight to user code; there is
  no library hook between the two, so placeholders would leak into `state.value()`.

**O13. Library state backend wrapper.** A delegating `StateBackend` that runs a hook before `createKeyedStateBackend`
restores.
- Rejected as a solution: it is a hook point, not a source of rules, and it constrains `state.backend`. Mentioned only
  because it is the one place besides step 1 and 2 where library code could run before an eager restore without
  wrapping operators.

**O14. Support matrix.** O1 alone, documented as "evolutions on eager restores need P6–P9 or an explicit declaration".
- Honest, cheap, and exactly what the user wants to avoid; kept as the floor every combination must beat.

### Group 8 — Rewrite the savepoint instead of the restore

**O15. Offline migration with the State Processor API.** A library-provided job or CLI reads the old savepoint with the
old serializers and writes a new one with the new code; its `main` derives every state type explicitly, so the rules
exist where it runs.
- Pros: works for every pattern and backend; battle-tested Flink path; no runtime constraint.
- Cons: an operational step between two versions, per job; the user must still list the state types for the tool,
  which is O7 in another form.

### Group 9 — Resolve on the JobManager

**O16. Pre-resolution at submission.** In application mode `main` runs on the JobManager, which also reads the
savepoint at submission.
- Rejected: the `_metadata` file holds state handles, not serializer snapshots; the keyed state files are opened by
  the TaskManagers only. There is nothing to resolve on the JobManager.

## Coverage at a glance

| Option | P1/P2 | P3/P4 | P5 | Top-level rename | Eager restores | Lazy restores | User constraint | Build constraint | Effort |
|---|---|---|---|---|---|---|---|---|---|
| O1 deferred resolution | lazy only | lazy only | lazy only | lazy only | no | yes | none | none | low |
| O2 TM companion init | yes | no | no | with O4 manifest | yes | yes | none | none | medium |
| O4 client init + manifest | yes | no | no | if derived on client | yes | yes | none | none | medium |
| O6 compiler plugin | yes | yes | yes | yes | yes | yes | none | plugin | high |
| O7 explicit `declare` | yes | yes | yes | yes | yes | yes | one line per ADT | none | low |
| O8 Java annotations + reflection | yes | yes | yes | no | yes | yes | API change | none | high |
| O8 + O9 jar scan | yes | yes | yes | yes | yes | yes | API change, package filter | none | high |
| O10 self-registering companion | yes | yes | yes | no (O9/O11) | yes | yes | one line per ADT | none | low–medium |
| O11 forwarding stub | rename only | rename only | rename only | yes | yes | yes | keep stubs | none | low |
| O15 offline migration | yes | yes | yes | yes | n/a | n/a | operational step | none | medium |

## Combinations worth considering

1. **Floor.** O1 + O1'. RocksDB users get every pattern at once; eager restores fail with an actionable message.
   Nothing else is acceptable without at least this.
2. **Pragmatic, no build change.** O1 + O4 (client companion initialization, manifest in a single Kryo-carrier or in
   the record serializers) + O10-lite (initialize a former name's companion on demand, which already serves `semiauto`
   users) + O7(b) as the documented escape hatch for P3–P5 on eager restores + O1'. Covers the survey's dominant
   patterns with no user change, and every remaining case with one central line.
3. **Complete, zero user constraint.** O6, with O1 and O1' underneath. Pays with two compiler plugins and a build line.
4. **Complete, metadata-driven.** O8 + O9 + O1. Pays with an API redesign of the annotations and a second rules reader.

Whatever the combination, two properties are worth requiring of it: the restore must **fail loudly** when a rule it
needed was not available, and a state the new code never touches must **round-trip unresolved** through the next
checkpoint.
