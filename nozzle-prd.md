# Nozzle — Complete Product Requirements Document

**Working name:** Nozzle

**Product:** Open-source, Drizzle-native sharding and fleet management for Cloudflare D1

**Status:** Implementation in progress; unreleased

**Release policy:** One complete public release. Nozzle is either complete against this document or it is not released.

**Public standard:** Every committed file and release artifact is assumed to be publicly visible.

Normative terms such as **MUST**, **MUST NOT**, **SHOULD**, and **MAY** describe release requirements.

This file is the sole product source of truth. Requirements, architecture decisions, rationale, rejected alternatives, risks, and release-blocking proof obligations MUST be maintained here together. Other documentation may explain this contract but MUST NOT redefine it.

Normative product requirements and accepted architecture decisions may exist only in this file. Supporting guides, READMEs, schemas, examples, `AGENTS.md`, skills, RFCs, ADRs, and generated evidence are derivative. A conflict is a release failure and this file wins. An RFC or ADR has no accepted force until its decision is incorporated into section 29.

## 1. Product contract

Nozzle MUST make a fleet of Cloudflare D1 databases behave like one deliberately partitioned Drizzle application without pretending that the fleet is one globally transactional SQL database.

A developer defines a normal Drizzle schema, declares a partition boundary, and writes familiar code:

```ts
const db = nozzle.for(workspaceId)

await db.insert(projects).values({
  workspaceId,
  name: "Nozzle",
})
```

Nozzle owns the operational work around that query:

- physical D1 database creation, adoption, registration, quarantine, and retirement;
- deterministic partition-to-bucket and bucket-to-shard routing;
- native direct bindings and generated router Workers;
- typed Drizzle clients in both direct and router topologies;
- schema validation and tenant-scoped query enforcement;
- capacity planning based on storage, load, latency, errors, and cost;
- fleet-wide migrations, backfills, drift detection, and mixed-version safety;
- online bucket and tenant movement with mutation capture and fenced cutover;
- D1 read-replica sessions through route-aware session tokens;
- bounded cross-shard reads and explicit durable sagas;
- backup, restore, disaster recovery, and safe deletion;
- observability, auditability, cost estimation, and a local dashboard;
- local development, remote integration, scale, chaos, security, and compatibility testing;
- complete documentation, examples, and agent guidance.

The simple path MUST work for a beginner. The complete system MUST remain inspectable and controllable by an experienced operator.

## 2. Release philosophy

Nozzle has no public MVP, staged feature edition, or partially safe release.

- Features required by this document MUST be implemented, documented, and tested before the first public release.
- Safety, recovery, observability, and testing are product features, not follow-up work.
- Internal engineering may be divided into workstreams, but those workstreams do not redefine the release contract.
- A feature hidden behind an undocumented flag, manual database repair, or maintainer-only procedure does not count as complete.
- A feature whose safety depends on a Nozzle-operated service does not count as complete.
- There are no paid editions, proprietary scale features, or hosted-only recovery paths.

## 3. Product statement

> **Nozzle is the Drizzle-native control plane for scaling Cloudflare D1 across many databases: automatic for beginners, explicit for experts, recoverable without vendor support, and entirely open source.**

## 4. Target users and workloads

Nozzle targets:

- Cloudflare-native SaaS applications partitioned by organization, workspace, account, or tenant;
- consumer applications partitioned by user, household, device, or content owner;
- platforms that provision isolated data groups for generated applications or customer projects;
- time-oriented workloads partitioned by entity and time window;
- framework and library authors that need a reusable D1 fleet abstraction;
- teams that want D1 isolation and scale-out without writing a fleet manager.

Nozzle is appropriate only when application data has an explicit partition boundary. It MUST refuse configurations that cannot define one safely.

## 5. Product principles

1. **One complete release:** Nozzle is released only after all acceptance criteria pass.
2. **Excellent default path:** one command and one explicit partition declaration produce a working application.
3. **No database per user by default:** auto mode groups partitions into virtual buckets and buckets into physical databases.
4. **Explicit partition boundary:** Nozzle never guesses a dangerous partition key.
5. **Drizzle-native:** schemas, types, relations, queries, and migrations stay familiar within the documented distributed constraints.
6. **Enforced safety:** important guarantees are enforced in code, generated schema, and state machines rather than described only in documentation.
7. **Operational honesty:** no fake global ACID, invisible distributed joins, global uniqueness claims, or exactly-once claims.
8. **Failure is expected:** every long-running operation is checkpointed, idempotent, resumable, and observable.
9. **Control-plane independence:** normal routed queries do not depend on a live central control-database lookup.
10. **Provider-native runtime:** normal queries use D1 bindings. Management APIs are used for control-plane work.
11. **User-owned operation:** all infrastructure, telemetry, credentials, state, and backups remain in the user's environment.
12. **No exfiltration:** Nozzle sends no fleet or application data to Nozzle maintainers.
13. **Public-quality repository:** secrets, personal data, local state, and careless generated output are never committed.
14. **Stupidly simple:** the public mental model, configuration, normal query path, generated output, and recovery procedures remain as simple as correctness allows.
15. **Measured efficiency:** Nozzle optimizes real hot paths and provider constraints, not hypothetical cleverness. Necessary complexity stays isolated behind small, testable contracts.

### 5.1 Simplicity and efficiency standard

Nozzle is a complex infrastructure product, but complexity is not itself a feature.

- The smallest correct design with the fewest services, states, dependencies, and abstractions is preferred.
- Normal application code MUST remain simple even when routing, fencing, or topology logic is sophisticated internally.
- Algorithms on the request path MUST have bounded time and memory behavior appropriate to Workers limits.
- Fleet-wide work SHOULD use precomputation, batching, pagination, streaming, incremental reconciliation, and concurrency limits rather than repeated full-fleet scans.
- Optimization MUST be supported by a benchmark, scale requirement, provider limit, or measured production-shaped workload.
- A complex algorithm MUST be isolated behind a simple interface and accompanied by documented invariants, complexity analysis, failure behavior, and an understandable reference implementation.
- Optimized and reference implementations MUST be compared with differential and property-based tests.
- A simpler implementation remains preferred when performance is within the declared budget.
- Operators MUST be able to understand a generated plan without understanding the internal optimization algorithm.
- Nozzle MUST NOT add a service, queue, cache, database, abstraction layer, or persisted state solely because it may be useful later.

## 6. Explicit boundaries and non-goals

Nozzle MUST NOT claim to provide:

- cross-shard ACID transactions;
- transparent distributed joins;
- globally consistent snapshots across independent D1 databases;
- global foreign keys or database-enforced global uniqueness;
- exactly-once queue or saga execution;
- arbitrary raw SQL with the same safety guarantees as the scoped API;
- recovery of a D1 database after Cloudflare has permanently deleted it;
- protection from out-of-band schema changes that remove Nozzle's safety metadata or triggers;
- runtime use of Cloudflare management credentials for normal application queries;
- support for an unlisted Drizzle, Wrangler, Node.js, or Workers runtime version.

Tenant isolation through the safe API is not a sandbox against hostile code executing in the same Worker. Code with raw D1 binding access, an enabled unsafe client, or administrative credentials is inside the database trust boundary. Untrusted code MUST run in a separate Worker or sandbox and MAY require dedicated placement.

Nozzle-managed fleets MUST treat Nozzle as the exclusive schema-change coordinator. Out-of-band schema changes are drift and MUST block unsafe operations until reconciled.

## 7. Platform capability model

Cloudflare limits and capabilities change. Nozzle MUST NOT rely on an unversioned list of constants.

The Cloudflare adapter MUST maintain a versioned capability registry containing:

- documented default limits and the date/source from which they were obtained;
- values discovered from Cloudflare APIs or Wrangler where a supported interface exists;
- account-plan and authentication-profile information where discoverable;
- operator overrides for approved limit increases that Cloudflare does not expose programmatically;
- feature probes for read replication, jurisdiction, Time Travel, router deployment, remote bindings, and structured CLI output;
- conservative fallbacks when a capability cannot be verified.

Nozzle MUST distinguish:

- documented platform defaults;
- live account usage;
- live account-specific quotas;
- user-declared raised quotas;
- assumptions that could not be verified.

If Cloudflare exposes no documented quota endpoint, Nozzle MUST say so and require an explicit override rather than pretending to discover the value.

Current design assumptions include, but are not hardcoded as permanent truth:

- D1 is designed to scale across many smaller databases;
- a paid D1 database has a 10 GB maximum size;
- paid accounts have documented default database and total-storage limits that may be raised by Cloudflare;
- a Worker has an approximate binding ceiling derived from its script-metadata budget;
- a D1 database processes queries serially and may return overload errors;
- D1 binding, SQL, query-duration, row-size, parameter, and subrequest limits apply;
- Cloudflare's management API has global and product-specific rate limits;
- read replication requires D1 Sessions for sequential consistency;
- D1 jurisdictions are chosen at database creation and are not freely mutable;
- D1 export can make the source database unavailable;
- Time Travel restores in place and does not provide a general clone primitive;
- Service-bound Workers deploy independently and require compatible deployment ordering.

Every plan MUST record the capability snapshot used to produce it.

## 8. Architecture

### 8.1 Resource separation

Nozzle MUST provision separate resources for separate blast radii:

1. **Control database:** Nozzle fleet metadata only.
2. **Application-global database or databases:** developer-declared global tables.
3. **Physical shard databases:** sharded application data and shard-local Nozzle safety metadata.
4. **Router Workers:** generated query routers when direct bindings are not selected, plus bounded shard-access leaves deployed when managed data operations require them.
5. **Operation/controller Worker and Workflows:** user-owned on-demand operation execution, with optional continuous reconciliation.
6. **Optional telemetry resources:** user-owned Analytics Engine, log, trace, or external observability destinations.
7. **Optional backup storage:** user-owned R2 for long-retention exports and manifests.

Application-global tables MUST NOT share the Nozzle control database in production. Local development MAY collapse resources only when the behavior and blast-radius difference are clearly identified.

Every fleet MUST belong to exactly one declared Cloudflare account. Nozzle MAY manage multiple accounts as separate command targets and fleets, but a fleet MUST NOT span accounts and ordinary movement MUST NOT cross an account boundary.

### 8.2 Logical routing model

Auto mode uses:

```txt
partition key -> canonical key bytes -> SHA-256 digest -> shared bucket or optional sparse reserved-bucket override -> physical shard
```

An override MUST map a partition to a reserved bucket, never directly to a physical shard. The bucket remains the only physical ownership unit.

Shared and reserved buckets use disjoint namespaces. For `bucketBits`, ordinary hashing selects exactly one dense shared-bucket ID in `[0, 2^bucketBits)`. Reserved bucket IDs are allocated sparsely and monotonically in `[2^bucketBits, 2^32)` and therefore can never be selected by ordinary hashing. A reserved ID has an explicit sparse route and local ownership record, is never inferred from its numeric value alone, and is not reused within a fleet after retirement. The virtual-bucket count refers to dense shared buckets; reserved buckets do not require a second dense route table.

Routing identity MUST include:

```txt
fleet ID
environment ID
topology version
hash version
bucket ID
physical shard ID
route epoch
ownership state
jurisdiction
schema compatibility range
```

The shared virtual-bucket count MUST be selected at fleet creation and remain immutable except through an explicit full-fleet repartition operation. The ordinary default is 65,536 (`2^16`) shared buckets. An explicitly selected high-scale profile MAY use 1,048,576 (`2^20`) shared buckets only after route-manifest and Worker-memory validation. The selected count and sparse reserved-bucket budget MUST be validated against the maximum intended shard count, movement granularity, route-manifest size, Worker memory, and topology target.

### 8.3 Canonical partition hashing

The hashing specification MUST define:

- supported key types;
- byte encoding for strings, integers, UUIDs, and binary identifiers;
- Unicode normalization;
- rejection of ambiguous floating-point and object keys;
- hash algorithm, seed, and version;
- modulo or bucket-selection algorithm;
- cross-runtime test vectors;
- a safe procedure for changing the hash version.

Hash version 1 MUST use native SHA-256 over unambiguous length-framed bytes containing a fixed versioned domain separator, stable 32-byte public fleet seed, canonical type tag, and canonical key bytes. Ordinary strings use exact UTF-8 without case folding or Unicode normalization; integers are accepted only when the supported D1 binding mode preserves them exactly; UUIDs require an explicitly declared canonical UUID key type; binary keys preserve exact bytes. Ambiguous, malformed, floating-point, null, array, and object keys MUST be rejected. Bucket selection MUST use the required leading digest bits for the power-of-two bucket space. Route overrides MUST use the full 256-bit digest; a truncated 128-bit fingerprint MAY be used only where a collision cannot affect correctness, such as bounded telemetry. Hashing occurs lazily on first query execution and the scoped client caches the result. Hashing is not a privacy guarantee.

Hash version 1 has the following exact wire contract:

- The SHA-256 preimage begins with the four fixed bytes `4e 5a 48 01` (`NZH` followed by version byte `01`).
- It then contains four fields in the fixed order domain, fleet seed, key type, and canonical key. Every field is encoded as a one-byte field tag, a four-byte unsigned big-endian byte length, and exactly that many value bytes. Extra, repeated, missing, or reordered fields are invalid.
- Field tag `01` is the exact UTF-8 bytes of `nozzle.partition.v1`; field tag `02` is exactly 32 public fleet-seed bytes; field tag `03` is exactly one key-type byte; field tag `04` is the canonical key bytes.
- Key-type byte `01` is an ordinary string, encoded as exact UTF-8 after rejecting unpaired UTF-16 surrogate code units. Empty strings are valid. No normalization, trimming, locale conversion, or case folding occurs.
- Key-type byte `02` is a signed integer. Version 1 accepts only JavaScript safe integers whose value is preserved exactly by the supported D1 binding path and encodes them as eight-byte two's-complement signed big-endian values. Negative zero, non-integers, unsafe integers, `NaN`, and infinities are invalid. A future exact-integer mode requires a new declared capability and hash version unless it produces these identical bytes.
- Key-type byte `03` is an explicitly configured UUID. Input MUST use the hyphenated `8-4-4-4-12` hexadecimal form without braces; hexadecimal input is case-insensitive, canonical text is lowercase, and the key bytes are the exact 16 decoded bytes. Other UUID spellings are invalid.
- Key-type byte `04` is binary and accepts a `Uint8Array`; its exact bytes, including an empty value, are preserved. Other array, view, object, and implicit string encodings are invalid.
- Fleet seeds are generated with a cryptographically secure random source at fleet creation, stored publicly as unpadded base64url in configuration and manifests, decoded to exactly 32 bytes before hashing, and immutable for the fleet. Fixtures and adopted fleets MAY supply an explicit seed, but invalid lengths are rejected.
- Version 1 shared-bucket spaces are exactly 16 or 20 bits. Ordinary bucket selection interprets the first four digest bytes as an unsigned big-endian integer and takes its leading configured bucket bits. This yields shared-bucket IDs in `[0, 2^bucketBits)` without modulo bias. It never selects a sparse reserved-bucket ID.
- Published cross-runtime vectors MUST include the preimage bytes, digest, 16-bit bucket, and 20-bit bucket for every supported type plus rejected-input vectors. Any change to a byte above is a new hash version and requires an explicit full-fleet repartition operation; it is never a configuration-only change.

The same key and hash version MUST produce the same bucket on every supported operating system, runtime, architecture, and package manager.

### 8.4 Route manifests and cache behavior

Normal queries MUST NOT read the control database on every request.

Nozzle MUST publish immutable, checksummed route manifests. A manifest MUST contain enough information to resolve a partition to a physical binding or router and MUST be small enough for the supported Worker memory and bundle limits.

Manifest format version 1 stores a dense shared-bucket route table, sorted compact shard descriptors, sorted sparse reserved-bucket route records, and sorted full-digest overrides. Each reserved record contains bucket ID, shard index, route epoch, and ownership state. Every override targets one declared reserved record. Every shard descriptor is referenced by at least one shared or reserved route.

The implementation baseline rejects an encoded manifest above 24 MiB, more than 2 MiB of aggregate shard-descriptor UTF-8 text, more than 65,536 sparse reserved records, or more than 65,536 overrides. Loading copies caller-owned bytes once before the first asynchronous integrity check, verifies and decodes that same immutable copy, and retains bounded copies only. These conservative caps MAY change only with Worker-memory and startup benchmarks, a peak-memory analysis, updated capability evidence, and the decision-change procedure in section 29.7.

Every application and router Worker using a fleet MUST be registered with its deployment identity, protocol and schema ranges, topology version, and destination-reachability attestation. Nozzle-managed deploys register automatically. External deployment pipelines MUST publish the same signed attestation. Cutover MUST block when an active registered caller is missing, incompatible, or cannot reach the destination.

The runtime MUST define:

- initial manifest loading;
- cache lifetime and refresh;
- stale-while-refresh behavior;
- manifest integrity validation;
- topology rollback;
- route override precedence;
- behavior when the control plane is unavailable;
- behavior when a stale route reaches a former owner;
- bounded safe retry after a pre-mutation route rejection.

### 8.5 Physical ownership and fencing

Every physical shard MUST store local bucket-ownership records. A route entry in the control database alone is not sufficient authority to accept writes.

Each local ownership record MUST include:

- bucket ID;
- route epoch;
- canonical persisted state `unassigned`, `preparing`, `copying`, `catching_up`, `read_only`, `writable`, `quarantined`, `retired`, or `intervention_required`;
- movement role `none`, `source`, or `destination`, which describes the operation role without creating a second writable state vocabulary;
- operation ID responsible for a transition;
- controller fencing token that authorized the transition;
- schema version;
- last verified checkpoint.

Local ownership metadata MUST reject bucket-ID mutation, route-epoch rollback, fencing-token rollback, operation-ID replacement without a higher token, illegal state transitions, deletion, and any transition from a non-writable state into `writable` without a strictly newer route epoch. Controller statements MUST additionally use compare-and-swap predicates over the prior operation ID and fencing token. Generated write guards MUST reject a write before mutation when the local shard is not the writable owner. This is the final protection against stale application or router deployments.

Each shard also stores an active schema-identity row and persistent `nozzle_partition_fences` rows containing the full hash-versioned partition digest plus the canonical typed partition value. Every safe statement performs an atomic local ownership, schema-identity, and digest-fence guard. Generated insert, update, and delete triggers independently enforce ownership and the typed partition fence from `NEW` or `OLD` row values, so an older client that lacks the current statement guard still fails before mutation. Schema compatibility remains a statement, deployment-barrier, and migration-ledger check because a D1 trigger cannot identify the calling Worker version. A digest lookup is an optimization and collision-resistant identity check; it does not replace the trigger's type-preserving value comparison.

No valid state may contain two writable owners for one bucket.

When one partition is promoted from a shared bucket to a reserved bucket, every former source MUST retain a type-preserving partition fence. The fence MUST reject stale writes for that partition before mutation while allowing unrelated partitions in the shared bucket to remain writable.

### 8.6 Direct topology

In direct topology, the application Worker binds directly to physical D1 databases and constructs native Drizzle D1 clients.

Direct topology MUST:

- reserve configurable metadata and binding headroom;
- measure the generated Worker's actual deployment metadata with a dry run;
- reject a topology that only fits by relying on an approximate documented maximum;
- generate deterministic, collision-free binding names;
- support additive binding deployment before route activation;
- remove unused bindings only after the safety window and topology verification.

### 8.7 Router topology

In router topology, the application calls generated router Workers through service bindings. Leaf routers own bounded sets of D1 bindings.

Router topology MUST:

- use one normal application-to-leaf-router Service Binding hop before the leaf accesses its local D1 binding;
- remain below Worker invocation, subrequest, memory, CPU, and response-size limits;
- deploy dependencies before callers;
- use backward-compatible RPC protocol versions during rollout;
- support partial deployment recovery and rollback;
- health-check every generated router and binding;
- garbage-collect obsolete routers only after route and binding verification.

Router mode requires a real Nozzle Drizzle transport. The safe path MUST carry a versioned structured execution plan, not arbitrary SQL text. The leaf MUST independently validate schema identity, canonical partition and full digest, bucket, epoch, query shape, and limits before compiling the plan with the supported Drizzle adapter. The transport MUST implement the supported prepared-statement, typed parameter, result, atomic batch, error, metadata, session, timeout, and cancellation behaviors with a versioned type-preserving wire codec. Unsafe raw SQL is a separate policy-gated capability. A D1 binding is not treated as an object that can simply be returned to the application over RPC.

Execution-plan wire format version 1 represents null, boolean, finite supported numbers, and strings as their native JSON types. A BLOB is the frozen tagged object `{ "type": "blob", "hex": "<lowercase-even-length-hex>" }`; no other object is a bound value. Every plan carries the exact 64-character lowercase full partition digest used for its route and fence lookup. Builder-created in-process plans and validated decoded wire plans are separately authenticity-marked and bound to the validated schema registry before compilation, so a structurally similar caller object cannot bypass schema, scope, value, or limit validation. The compiler restores BLOBs to `Uint8Array` D1 bindings. Any change to these representations requires a new execution-plan version and direct/router equivalence tests.

Router result wire format version 1 carries raw D1 rows so the caller's independently compiled schema registry performs the same Drizzle result decoding as direct mode. Native D1 BLOB results, including the byte-array form returned by workerd, are normalized to the same lowercase tagged-hex representation; other object values are rejected. A single response is capped at 16 MiB of decoded value data, 10,000 rows, 256 columns per row, and 2 MiB per string or BLOB. An atomic router batch is capped at 49 data plans so one independent authorization statement plus the data statements remain bounded. These caps intentionally remain below the current 32 MiB Workers RPC serialization limit and may change only through section 29.7.

### 8.8 Control-database schema

The control database MUST include versioned equivalents of:

- `nozzle_fleets`
- `nozzle_config_versions`
- `nozzle_topology_versions`
- `nozzle_shards`
- `nozzle_buckets`
- `nozzle_route_versions`
- `nozzle_route_overrides`
- `nozzle_placement_constraints`
- `nozzle_schema_artifacts`
- `nozzle_schema_versions`
- `nozzle_migrations`
- `nozzle_operations`
- `nozzle_operation_effects`
- `nozzle_operation_steps`
- `nozzle_operation_transitions`
- `nozzle_provider_attempts`
- `nozzle_provider_attempt_outcomes`
- `nozzle_d1_resources`
- `nozzle_leases`
- `nozzle_idempotency_keys`
- `nozzle_capacity_samples`
- `nozzle_backups`
- `nozzle_controllers`
- `nozzle_audit_log`

`nozzle_operations`, `nozzle_operation_steps`, and immutable transition receipts are the canonical envelope for every mutating command, including provisioning, migration, movement, saga, backup, restore, retirement, and deletion. Domain-specific tables are materialized checkpoints keyed by and foreign-keyed to that canonical operation; they MUST NOT form an independent operation authority. A contradiction between the generic ledger and a domain checkpoint enters `intervention_required`.

Operations MUST be reconstructible from checksum-verified canonical input, capability snapshot, plan, step, transition, and subordinate receipt records without relying on process memory. Configuration and topology versions MUST be immutable after publication.

Every operation-plan step declares required or conditional activation; required is the default and every plan contains at least one required step. An unused conditional step advances from unattempted `pending` to terminal `not_required` only through an exactly fenced transition carrying immutable branch-decision evidence. It has no fabricated attempt, result, or fencing metadata and does not satisfy a success dependency. An ordinary operation without an explicit settlement step is successful when every step is either genuinely `succeeded` or validly `not_required`.

An operation may declare at most one required settlement step. When present, that step alone gates the top-level terminal status: work-step failure or intervention remains nonterminal until compensation and branch cleanup finish, while running and unknown work still surface as running and reconciling. The settlement step then records the verified domain terminal result as succeeded, failed, or intervention-required. This prevents a generic operation from becoming terminal while its durable domain protocol is still progressing.

Control schema version 1 persists lease rows instead of deleting them, so fencing tokens can never reset. Lease acquisition, renewal, release, and authorization use D1 server time, the shared pure lease reference model, and exact optimistic compare-and-swap predicates. A takeover advances the token by exactly one and is permitted only after release or authoritative expiry; shard-local triggers independently reject token rollback, token jumps, unfenced identity changes, active-lease takeover, and deletion. Compare-and-swap retries are bounded at 16 before the operation enters an actionable intervention state.

### 8.9 Shard-local schema

Every physical shard MUST contain versioned equivalents of:

- `nozzle_shard_meta`
- `nozzle_bucket_ownership`
- `nozzle_partition_fences`
- `nozzle_movement_capture`
- `nozzle_mutation_outbox`
- `nozzle_schema_version`
- `nozzle_migration_log`
- `nozzle_idempotency_keys`

Every sharded row MUST contain a reserved `__nozzle_bucket INTEGER NOT NULL` column. Nozzle MUST inject and validate it consistently, index it where required for ownership checks and movement, hide it from normal application result types, and reserve the entire `__nozzle_` identifier prefix against user schema collisions.

## 9. Configuration and primary API

The beginner configuration remains intentionally small:

```ts
export default defineNozzle({
  schema,
  mode: "auto",
  partitionKey: "workspaceId",
  globalTables: [users, accounts],
})
```

Advanced configuration MUST support:

- fleet and environment identity;
- Cloudflare authentication profile and account selection;
- direct, router, or automatic topology;
- bucket count and hash policy;
- placement and residency policy;
- capacity thresholds and predictive headroom;
- reconciliation execution mode;
- migration and movement concurrency;
- backup, retention, and restore policy;
- telemetry storage, sampling, and retention;
- cost guardrails;
- unsafe feature opt-ins;
- account-specific quota overrides.

The primary runtime API MUST include:

```ts
const nozzle = createNozzle({ env, schema, config })

const db = nozzle.for(workspaceId)
const globalDb = nozzle.global()

await db.batch([
  db.insert(projects).values(project),
  db.insert(events).values(event),
])
```

The compact beginner field `mode` is an alias for placement mode only. The canonical advanced shape uses `placement.mode` and `topology.mode` as separate concepts. Supplying both `mode` and `placement.mode` is valid only when they are identical; disagreement MUST fail configuration validation. Runtime topology MUST never be inferred from a placement-mode value.

Additional APIs include:

```ts
await nozzle.session(workspaceId, token)
await nozzle.locate(workspaceId)
await nozzle.fanOut(options)
await nozzle.saga("transfer.v1", { idempotencyKey, input })
```

All public APIs MUST have stable error identities, runtime validation, TypeScript types, examples, and compatibility tests.

## 10. Schema model and tenant isolation

### 10.1 Schema classification

Every application table MUST be classified as exactly one of:

- sharded;
- application-global;
- Nozzle-internal;
- explicitly unmanaged.

No table may silently fall into a class.

### 10.2 Required sharded-table rules

Every sharded table MUST:

- contain the declared partition key or an explicitly declared derivation;
- declare a non-null stable primary key; implicit `rowid` is not a supported movement identity;
- index the partition key in the access patterns required for movement and scoped queries;
- include the partition key on both sides of every database-enforced relationship between sharded tables;
- include the partition key in tenant-local unique constraints;
- use foreign keys that remain within one partition and physical shard;
- prohibit ordinary partition-key mutation;
- prohibit ordinary primary-key mutation; model a key change as an explicit delete and insert;
- avoid globally meaningful auto-increment identifiers;
- use a documented globally unique or partition-scoped identifier strategy;
- avoid reserved `nozzle_*` and internal column names.

The schema compiler MUST reject or require an explicit unsafe declaration for:

- cross-shard foreign keys;
- cross-shard relations;
- global uniqueness assumptions;
- application-global to sharded cascade operations;
- sharded and global joins presented as one database query;
- partition-key changes;
- unsupported D1 SQL, data types, statements, or limits;
- migrations that remove Nozzle safety metadata or triggers.

Auto and other movable placement modes MUST reject a self-referential or cyclic sharded foreign-key graph unless a user-supplied movement adapter passes Nozzle's bounded-copy, constraint, type-fidelity, crash-recovery, and remote verification contract. The default mover supports acyclic graphs in topological order. Final foreign-key validation MUST NOT be disabled.

### 10.3 Scoped query enforcement

`nozzle.for(key)` MUST provide a tenant-scoped query surface.

The supported safe API MUST:

- inject or verify partition predicates for reads, updates, and deletes;
- inject and validate partition values for inserts;
- validate joins and relations against the same partition;
- reject mismatched values;
- prevent accidental full-shard scans through a tenant-scoped client;
- distinguish application-global queries from sharded queries;
- preserve familiar Drizzle types and query composition where safe.

Raw SQL MUST use an explicitly named unsafe API. Unsafe SQL MUST be documented as outside tenant-isolation guarantees, logged with redaction, and optionally disabled in production. Nozzle MUST NOT use post-generation SQL parsing or rewriting as its tenant-isolation boundary.

If a Drizzle API cannot be safely intercepted or represented, Nozzle MUST reject it rather than silently weaken isolation.

### 10.4 Direct administrative writes

Nozzle MUST document the effect of writes performed through the D1 dashboard, Wrangler, REST API, or another Worker.

- Generated ownership and mutation-capture triggers MUST protect supported direct data writes where possible.
- Direct schema modification is unsupported and produces drift.
- `nozzle doctor` and `nozzle verify` MUST detect missing or changed internal tables, triggers, indexes, and ownership records.

## 11. Placement modes

The complete release includes:

- **Auto:** virtual buckets with capacity-aware automatic placement and reconciliation.
- **Hash:** rendezvous or jump hashing over a fixed shard fleet with explicit remapping behavior.
- **Directory:** explicit bucket-to-shard placement, with sparse partition-to-reserved-bucket overrides.
- **Dedicated:** one nominated tenant or entity assigned a reserved bucket on an isolated physical database.
- **Time bucket:** entity plus time-window routing.
- **Custom:** a typed user-provided routing adapter with invariant checks.

All placement modes MUST obey jurisdiction, schema compatibility, capacity, quarantine, and ownership constraints.

Changing placement mode is a planned, resumable fleet operation. It MUST NOT imply that existing data moves automatically without an inspectable movement plan.

Dedicated placement MUST be available as an isolation policy for tenants requiring database-level separation.

## 12. Residency and location policy

Nozzle MUST model jurisdiction as a hard constraint and location as a preference where supported.

Placement policy MUST support:

- global, EU, and FedRAMP jurisdictions exposed by the current provider capability registry;
- tenant-specific residency overrides;
- permitted destination jurisdictions;
- read-replication policy;
- location hints for write locality;
- prohibitions against cross-jurisdiction movement, backup, and fan-out;
- validation that a destination's immutable jurisdiction is compatible before copying any data.

Nozzle MUST never silently move data to a less restrictive jurisdiction.

## 13. Consistency and sessions

### 13.1 Local transactions

Atomicity exists only within one physical D1 database. Nozzle's topology-independent public primitive MUST be an explicit D1 batch of pre-built statements routed to one shard, with atomic success or rollback tested against real D1. An unsafe direct-topology client MAY expose a supported native Drizzle transaction API, but arbitrary interactive transaction callbacks are topology-specific and are not part of Nozzle's portable contract.

### 13.2 Route-aware session tokens

Nozzle MUST NOT expose a bare D1 bookmark as a complete cross-request session token.

An opaque Nozzle token MUST carry at least:

```ts
type NozzleSessionToken = {
  fleetId: string
  shardId: string
  routeEpoch: number
  d1Bookmark: string
}
```

Tokens MUST be integrity-protected, versioned, bounded in size, and safe to reject. Version 1 uses the `nz1` envelope, deterministic JSON fields, HMAC-SHA-256 with a non-extractable key or at least 32 bytes of key material, an 8 KiB total token cap, 255-byte fleet and shard identifiers, and a 4 KiB bookmark cap. Token contents are opaque protocol state, not an encryption or confidentiality boundary.

If a bucket moves after a token is issued, Nozzle MUST detect the old shard and epoch, establish an appropriately fresh destination session, and return a replacement token. A source bookmark MUST NOT be passed blindly to a different D1 database.

### 13.3 Cross-shard consistency

Nozzle MUST describe fan-out results as best-effort across independent databases unless a narrower guarantee is explicitly implemented. There is no implied global snapshot.

## 14. Fleet operations

### 14.1 Operation engine

Every mutating operation MUST be represented as a persisted state machine with:

- operation and step IDs;
- idempotency keys;
- immutable input plan and capability snapshot;
- fenced lease ownership;
- preconditions and postconditions;
- retry classification;
- progress and cost counters;
- human-readable recovery instructions;
- machine-readable structured output;
- explicit reversible and irreversible checkpoints;
- complete audit events.

Restarting a command with the same operation ID MUST resume safely. Starting an incompatible operation while another holds the relevant fence MUST fail clearly.

Every physical provider dispatch follows this protocol:

1. persist and audit the step transition to `running` under a live fenced lease;
2. persist an immutable provider-attempt acceptance receipt containing the provider target, endpoint, request checksum, attempt ID, actor binding, and lease fence before dispatch;
3. never dispatch without that receipt;
4. recover an orphaned provider step with no accepted receipt as proven not dispatched and therefore eligible for its first physical attempt;
5. recover an accepted receipt without a terminal receipt as `unknown`;
6. use a terminal receipt to reconcile the generic step even if the original executor died before recording the step outcome;
7. reject a late response under an expired fence as authority for a control-state transition; and
8. observe every unknown effect before another physical attempt.

The operation transition receipt, step state, derived operation status, and audit event MUST commit atomically. Provider receipts are subordinate immutable evidence, and the subsequent audited transition MUST reference their checksum exactly. The audit chain is totally ordered per environment using D1 server time. Provider outcome certainty and logical repeatability are separate facts: `unknown` always requires observation, while `retryable_failed` means non-application has already been proven.

Provider-attempt purpose is immutable. An `effect` receipt is accepted only for the exact active attempt of a `running` step under the same fence. A `reconciliation` receipt is non-mutating, is accepted only while the step is `unknown`, and requires a strictly newer active fence. A provider-backed step can record success, definite rejection, or unknown outcome only when the corresponding terminal receipt state and outcome checksum match exactly; reconciliation requires a separate confirmed observation receipt.

### 14.2 Provisioning and adoption

Nozzle MUST:

- create, inspect, register, adopt, bind, quarantine, retire, and delete D1 databases;
- use deterministic public-safe resource names and ownership markers;
- paginate all list operations;
- respect Cloudflare response rate-limit headers and global API limits;
- use jittered exponential backoff;
- handle unknown outcomes after network failure;
- adopt a deterministically named resource only after validating identity and state;
- separate desired state from observed state;
- record externally created or modified resources as drift;
- support multiple Cloudflare accounts, authentication profiles, and environments as explicitly separate targets and fleets without accidental cross-target mutation.

Resource creation MUST NOT assume the provider accepts an idempotency key when it does not. Nozzle must implement idempotency at its own operation layer.

A D1 resource's canonical identity is its provider-target checksum, fleet and environment, logical shard, collision-resistant resource-generation ID, and provider UUID. A database name is a selector, never sufficient identity. Every generation receives a public-safe random token sealed into the operation input and generated name. The installed shard marker repeats the generation, fleet, shard, creation operation, plan, provider UUID, and target identities.

The D1 resource lifecycle stores stable materialization facts only: `planned`, `registered`, `ready`, `quarantined`, `retired`, `deleted`, and `abandoned`. Transient execution facts such as creating, updating, deleting, running, failed, or unknown exist only in the canonical operation step. `planned -> registered` atomically records the provider UUID; `registered -> ready` requires a fresh matching direct observation; quarantine recovery requires a fresh observation recorded after quarantine; `retired -> deleted` requires a fresh structured direct-UUID absence observation recorded after retirement; `planned -> abandoned` is allowed only before any provider binding. The UUID remains in the deleted tombstone and can never be rebound.

Desired intent, recorded provider binding, and observed provider state are distinct fields. Every observation is version-stamped against the resource state so an old healthy observation cannot recover a quarantine and an old absence observation cannot confirm deletion. Every materialization version is linked atomically to an append-only `nozzle_operation_effects` receipt containing the succeeded canonical transition, exact fence, prior and next resource versions, evidence checksum, canonical reconstructible record, and record checksum.

Name-only adoption is prohibited. A marked resource may be resumed or adopted automatically only when the marker and every immutable property match. An unmarked database requires an explicit adoption authorization plus provider-target, UUID, schema, data, jurisdiction, and conflict inspection.

Canonical jurisdiction is `global`, `eu`, or `fedramp`. `global` is encoded by omitting Cloudflare jurisdiction and normalized from an omitted or null observed value. Jurisdiction is enforced immutable desired state. A primary-location hint is a creation-time preference, is not observable afterward, and cannot establish identity or drift. Nozzle rejects supplying both jurisdiction and a location hint.

Unknown create is reconciled through the documented provider-side name search, complete pagination of that search, exact local name filtering, direct UUID inspection, generation-token matching, and immutable-property validation; it is never retried blindly. The provider's `total_count` for a name search is account-global and MUST NOT be treated as the filtered result count. If an ambiguous create has no visible exact-name candidate, the operation remains `await_create_visibility`; zero visible results do not prove non-application and cannot authorize another create. Unknown delete is reconciled by a structured Cloudflare API `404` from direct GET of the exact recorded UUID; an unstructured 404 or list omission is insufficient. Delete always targets a recorded UUID and requires a matching marker, zero ownership, routes, bindings, and recovery obligations, an elapsed safety window, and sealed irreversible authorization.

### 14.3 Capacity planning

Capacity planning MUST consider:

- database size and projected growth;
- account-wide storage and projected growth;
- database count, Worker count, binding metadata, and reserved headroom;
- query duration and serial throughput;
- overload and transient error rates;
- read and write QPS;
- rows read, rows written, and response bytes;
- hot buckets and hot tenants;
- schema migration and movement temporary capacity;
- jurisdiction-specific available shards;
- D1, Workers, Workflows, Queues, telemetry, logging, and backup cost.

Default policy starts conservatively and is configurable:

- target occupancy: 60%;
- begin scale-out planning: 70%;
- stop new placement: 80%;
- emergency write guard: configurable below the verified provider limit;
- minimum account and binding headroom: explicit and validated;
- trend-based action before a threshold is reached.

Storage thresholds alone are insufficient. A small but overloaded database MUST be eligible for scale-out or dedicated placement.

### 14.4 Reconciliation

Reconciliation MUST compare desired, recorded, and observed state. It MUST produce an inspectable plan before mutation and MUST be safe under concurrent processes, API rate limits, partial failure, and stale observations.

Reconciliation MUST never recreate or delete a resource based solely on a missing cache entry.

A pagination-complete D1 inventory means only that every requested offset page decoded consistently. It is not a provider snapshot. Every scan records its target, start and end time, page evidence, UUID set, response checksums, and rate-limit evidence. Incomplete, inconsistent, duplicated, or over-budget scans permit diagnostics only. A recorded UUID missing from a list is verified by direct GET before any state change. Creation uses a fresh exact-name observation under a target-and-name lease, and every mutation revalidates exact resource identity and immutable properties immediately before dispatch.

One durable rate gate per provider target and credential-identity checksum coordinates Nozzle controllers. `Retry-After` is a minimum delay; `Ratelimit` and `Ratelimit-Policy` update the gate before bounded jittered backoff. Nozzle MUST disclose that provider-account activity outside Nozzle is not fully observable.

## 15. Migrations and deployment coordination

### 15.1 Migration artifacts

Nozzle MUST accept Drizzle-generated SQL while owning fleet orchestration.

Every migration artifact MUST include:

- content checksum;
- parent schema version;
- resulting schema version;
- supported application and router version range;
- D1 and SQLite compatibility validation;
- classification such as online-safe, table rebuild, backfill, destructive, or Nozzle-internal;
- expected storage and operational cost;
- rollback or forward-recovery strategy;
- human approval requirements.

`push`-style unreviewed schema synchronization MUST NOT be allowed against a production fleet.

### 15.2 Schema inspection and drift

Drift detection MUST compare canonical representations of:

- tables and columns;
- types, defaults, nullability, and generated expressions;
- primary, unique, and foreign keys;
- indexes;
- views;
- triggers;
- relevant PRAGMA-controlled behavior;
- Nozzle ownership, movement, and isolation structures;
- migration ledger and schema checksums.

Formatting and harmless ordering differences MUST not create false drift. Semantic differences MUST be reported precisely.

### 15.3 Fleet migration protocol

Fleet migrations MUST support:

- preflight syntax, schema, capability, quota, and temporary-capacity checks;
- pre-migration Time Travel bookmarks where supported;
- representative canary selection across size, load, region, jurisdiction, and topology;
- configurable concurrency below API and D1 load limits;
- halt thresholds based on failure rate, latency, overload, or verification failure;
- checkpoint and resume;
- idempotent handling of already-applied artifacts;
- post-migration schema and query smoke tests;
- explicit mixed-version operation;
- expand-deploy-backfill-contract workflows;
- destructive-change approval and irreversible checkpoints;
- recovery after process death and partial fleet completion.

Large data changes MUST run as resumable backfills rather than one unbounded SQL statement.

#### 15.3.1 Partial fleet failure

The public contract is deliberately simple: a fleet migration succeeds only when every required shard reaches and verifies the target schema version. If any required shard fails, the migration command and operation fail and the fleet is not declared converged.

Because independent D1 databases cannot participate in one cross-shard transaction, Nozzle MUST preserve and manage partial progress rather than pretending successful shards were rolled back.

On the first shard failure, the default policy MUST:

1. stop scheduling new migration waves;
2. allow already accepted provider operations to finish or cancel only where cancellation is documented as safe;
3. record an apply state for every shard as `pending`, `running`, `applied`, `retryable_failed`, `blocked_failed`, or `unknown`, and record verification independently as `pending`, `verified`, `failed`, or `unknown`;
4. keep the fleet in a visible `mixed_blocked` state;
5. refuse any contract/destructive phase or global schema-version activation;
6. retry only failures classified as transient when the migration step is proven idempotent;
7. require a corrected artifact, explicit operator decision, or successful resume for permanent and unknown failures;
8. resume the same operation without reapplying verified successful shards.

The configurable failure budget defaults to zero. A non-zero budget may control when Nozzle stops launching additional canary or migration work, but it MUST NOT allow an operation with failed required shards to be reported as successful.

Successful shards remain on the additive, backward-compatible schema while recovery occurs. Application and router compatibility ranges MUST support both old and new shard versions during this state.

Automatic rollback of successful shards is prohibited unless the migration declares and tests a safe reverse operation and no incompatible writes have occurred. Time Travel is destructive and rewinds data, so it MUST NOT be used as an automatic fleet rollback. Forward recovery is the default.

The required-shard set MUST be an immutable membership snapshot sealed into the operation plan before the first provider mutation. It includes every non-retired shard that owns application data, is referenced by an active or safety-window route, or is required by the migration policy. A shard provisioned or adopted after that snapshot MUST remain at a schema compatible with the active fleet barrier and MUST NOT receive ownership until a later migration operation proves it converged. Quarantine does not remove a shard from an already sealed required set. Retirement may remove one only through a separate completed operation that proves it has no data, route, binding, backup, or recovery obligation and records the exceptional membership decision in both operation ledgers.

The scheduler MUST persist a single monotonic halt event with the operation fencing token and control-database sequence. After observing that event, no controller may submit another shard application from a later wave. Provider work durably recorded as accepted before the halt may finish and MUST be reconciled; work merely selected or held in memory is not accepted work.

Resuming after a halt requires a durable decision checksum under an active controller lease whose fencing token is newer than the halt token. That authorization is immutable and idempotently replayable until another shard fails; a later failure clears it and re-halts the operation. Every transition MUST read back and validate the exact persisted shard or operation state rather than trusting provider mutation counters, which can include or obscure trigger-side writes. Verified shards remain untouched throughout forward recovery.

Migration artifact identity is immutable. A schema version and artifact checksum pair MUST never be reused for different SQL or a different canonical target schema. A corrected artifact is either byte-identical to the sealed artifact or is a new forward migration with a new identity. It MAY recover already successful and failed shards through different idempotent steps only when every path proves the same canonical target schema and compatible migration-ledger history.

Every contract or destructive artifact MUST be safe while contracted and uncontracted required shards coexist. Before its first canary, all active application and router versions MUST have stopped depending on the removed behavior, and the target versions MUST work against both schemas. If that mixed state cannot be proven, the contract phase MUST be rejected; a cross-shard atomic contraction is not available.

Safe reverse execution requires machine-observable evidence, not an operator assertion, that no incompatible write capability was active and no incompatible write was accepted after the forward barrier. The evidence MUST bind registered caller attestations, schema write-capability versions, route epochs, shard migration ledgers, and the forward operation interval. Without complete evidence, recovery proceeds forward.

An `unknown` shard MUST be reconciled by inspecting its canonical schema, immutable migration-ledger checksum, required Nozzle triggers and indexes, and post-migration smoke verification. It becomes applied and verified only when every target check passes; it becomes pending-repair only when an idempotent, non-destructive repair is proven safe; otherwise it becomes intervention-required. Nozzle MUST NOT blindly re-execute an unknown table rebuild, destructive artifact, or non-idempotent backfill.

Retryability is the conjunction of provider failure classification and artifact-step repeatability. A transient provider error alone never authorizes retry when the step's effect cannot be safely observed or repeated.

Migration success has one exact oracle: every shard in the sealed required set has the target canonical schema, the target immutable ledger checksum, and a passing post-migration verification; no required shard is pending, running, failed, or unknown; every active application and router compatibility range includes the target; and the fleet schema barrier advances atomically only after those conditions hold. A failure budget changes scheduling pressure only and never this success oracle.

### 15.4 Application, router, and schema rollout

Nozzle MUST coordinate compatible deployment order:

1. add backward-compatible schema or router capabilities;
2. deploy leaf routers and verify them;
3. deploy callers or route manifests that use the new capability;
4. run backfills;
5. verify fleet convergence;
6. remove obsolete code, bindings, or schema only after the compatibility window.

Generated deployments MUST be recoverable when only a subset of routers or application versions deploy successfully.

### 15.5 Nozzle's own upgrades

Nozzle MUST version and migrate its own:

- control schema;
- shard-local schema;
- route manifest format;
- router RPC protocol;
- session-token format;
- operation-state format;
- configuration schema.

Historical Nozzle releases MUST have tested upgrade paths to the public release. Unsupported downgrade paths MUST be blocked with explicit recovery guidance.

## 16. Bucket and tenant movement

### 16.1 Movement constraints

Movement MUST validate before copying:

- destination jurisdiction and location policy;
- destination schema compatibility;
- destination storage, throughput, and binding capacity;
- account-wide temporary storage headroom;
- absence of conflicting ownership or operations;
- capture support for every table and data type;
- foreign-key dependency graph;
- rejection or verified custom handling of self-referential and cyclic constraints;
- movement cost and expected duration.

D1 export MUST NOT be used as the online subset-movement mechanism because export may make a database unavailable and exports an entire database rather than one bucket.

### 16.2 Mutation capture

Nozzle MUST use generated transactional key-journal triggers and a shard-local outbox as its mutation-capture mechanism. Capture triggers MUST be installed and versioned with the application schema and emit only for buckets or typed partitions in a movement capture state. Each entry stores a table identity, type-preserving stable primary key, operation hint, sequence, schema version, and integrity metadata rather than duplicating whole row images.

Replay MUST read the current source row by primary key, type-preservingly upsert it when present, and delete it from the destination when absent. A later mutation MUST produce a later journal sequence. Partition promotion MUST substitute the destination reserved bucket during copy and replay rather than treating the internal source bucket as application data.

Mutation capture MUST handle:

- insert, update, and delete;
- composite primary keys;
- text, numeric, null, JSON, and BLOB values;
- schema-version changes;
- monotonic ordering;
- deduplication and idempotent replay;
- outbox retention and backpressure;
- detection of removed or modified triggers;
- cost and write-amplification accounting.

If a table or mutation cannot be captured safely, online movement MUST be blocked rather than downgraded silently.

The default mover MUST reject application-defined database triggers on movable sharded tables because base copy and state-based replay could duplicate or omit their side effects. A custom movement adapter MAY support them only by passing the complete movement, crash, constraint, and remote verification contract. Nozzle's own ownership and journal triggers remain mandatory.

### 16.3 Copy and replay

Base copy MUST:

- use deterministic keyset pagination rather than unstable offset pagination;
- copy tables in dependency order;
- use bounded row, byte, query-duration, and batch sizes;
- record per-table and per-range checkpoints;
- be restartable without duplicate logical rows;
- tolerate source activity through the capture mechanism;
- preserve application values exactly;
- avoid loading a bucket or table into Worker memory at once.

Replay MUST apply captured changes in order and use destination-side idempotency records.

### 16.4 Movement execution transport

Bulk movement, replay, verification, and large backfills MUST run through a user-owned operation Worker and bounded leaf Workers with D1 bindings. The operation Worker MUST use byte-bounded keyset pages, idempotent destination batches, and Workflow checkpoints that persist cursors and watermarks rather than unbounded row payloads.

Direct application topology MUST remain direct; operational leaves are outside its normal query path and MAY be deployed on demand. Administrative leaf RPC MUST require a short-lived operation-scoped capability unavailable to application query Workers. Leaf Workers MUST NOT receive Cloudflare management credentials.

Operational row writes MUST use generated empty administrative views rather than weakening base-table ownership triggers or adding a persistent bypass value to application rows. An administrative view validates a short-lived, bucket-, table-, operation-, mode-, expiry-, use-, and fencing-bound capability, creates a single trigger-local authorization context, performs the base-table upsert or delete, consumes one use, and removes the context within the same SQLite statement. Base-table guards accept a non-writable movement state only while that exact context exists. A failed statement rolls back the data change, capability consumption, and context together; no context may survive a statement. Replay receipts are committed in the same D1 batch as the operational write.

The Cloudflare management API MUST NOT be the default bulk row-transfer mechanism. Any explicit fallback MUST fit a measured rate, duration, cost, row-size, and recovery budget.

### 16.5 Fenced cutover

Cutover MUST follow a persisted protocol:

1. acquire a fenced bucket-operation lease;
2. verify source and destination epochs;
3. mark source ownership read-only so stale routes reject before mutation;
4. drain and verify the final mutation tail;
5. mark destination ownership writable at the next route epoch;
6. publish the immutable route update;
7. verify reads and writes through the public runtime path;
8. retain source data in quarantine for a configured safety window;
9. delete source rows only after an explicit irreversible checkpoint.

The protocol MUST define rollback before cutover, rollback after destination activation, and forward recovery after partial publication.

### 16.6 Verification

Movement verification MUST include:

- row counts per table and partition;
- versioned SHA-256 digest chains over type-preserving row encodings in canonical primary-key order after source fencing and tail drain;
- primary- and foreign-key validation;
- schema and trigger validation;
- capture-tail emptiness at cutover;
- read-after-write tests through direct and router topology;
- source rejection and destination acceptance tests;
- session-token transition tests.

## 17. Cross-shard operations

### 17.1 Bounded fan-out reads

Fan-out is explicit and MUST require budgets for:

- maximum shards;
- concurrency;
- timeout and deadline;
- rows and response bytes;
- Worker CPU and subrequests;
- estimated cost;
- partial-result policy.

Fan-out MUST support:

- count, sum, min, max, and custom reducers;
- deterministic global top-K merge;
- stable global ordering where requested;
- cursor-based pagination with topology-versioned continuation tokens;
- per-shard bookmarks or session information;
- structured partial results and shard-specific failures;
- cancellation of unnecessary remaining work.

Fan-out MUST NOT imply a global snapshot or transparent distributed join.

The first page seals the query checksum, schema identity, route-manifest checksum, exact shard set, immutable total order, partial policy, cumulative budgets, per-shard positions and bookmarks, and expiry. Continuations use keyset predicates, never SQL offset. Nozzle appends canonical shard ID and primary key as hidden tie-breakers and rejects safe cursor mode when a requested ordering column is not declared immutable.

Per-shard positions advance only through rows actually emitted. A topology or route-manifest change returns `RouteVersionConflictError`; version 1 does not translate cursors across movement. If any shard fails and partial results are allowed, the result is marked incomplete and carries no continuation cursor. Cumulative rows, bytes, CPU, subrequests, estimated cost, pages, and deadline carry forward so a continuation cannot reset budgets. Version 1 tokens encrypt the validated state with AES-256-GCM, use a fresh 96-bit nonce and version-bound additional authenticated data, expire, and have an 8 KiB encoded ceiling. Thus ordering values and primary keys are neither plaintext nor merely signed. State above the cap is rejected rather than moved into an implicit server-side cursor store.

Before dispatch, the executor validates current topology, query, and schema identity and proves that every active shard fits the remaining estimated cost, CPU, and subrequest budgets. It reserves at least one candidate row and byte per active shard, divides the configured buffered-row and buffered-byte ceilings across those shards, and may therefore return fewer than the requested page size rather than exceed Worker memory. Each request receives the smaller of the response-page target and its row share plus a byte share. Concurrency is bounded, each request uses the smaller of its timeout and remaining deadline, a required failure cancels queued and in-flight work, and late responses after timeout or cancellation cannot alter the result. Actual usage above the estimate is reported as a capacity failure and cannot produce a continuation.

Built-in reducers consume one validated partial per sealed shard in canonical shard-ID order. Count and integer sum use arbitrary-precision integers and reject lossy numeric inputs; floating sum uses a deterministic compensated algorithm and rejects non-finite inputs or overflow; min and max ignore null partials like SQL aggregates and use the same numeric or UTF-8 binary comparison as ordered fan-out. Custom reducers are invoked in the same canonical order. An allowed partial reduction is explicitly incomplete, while the default zero-failure policy fails closed.

Concurrent inserts, updates, and deletes may appear in or disappear from later pages. No global as-of set is claimed. With fixed topology, complete shards, immutable ordering, and a quiescent dataset, each unchanged row is emitted exactly once. The implementation uses a bounded heap-based K-way merge with `O(K log S)` work and is differential-tested against a straightforward collect-and-sort reference.

### 17.2 Sagas

Multi-shard writes use explicit durable sagas. Saga definitions are immutable, versioned descriptors registered at build or deploy time. Each step names versioned forward, observe, and compensation actions plus an artifact checksum. A required action version cannot be removed while a nonterminal saga or retained recovery record references it.

Deployments seal a deterministic handler manifest before accepting saga work. One action ID and version identifies exactly one artifact and one handler kind; changing code requires a new version. A descriptor is accepted only when every exact effect and observation reference is present. Adding an unrelated handler changes the deployment manifest but does not alter an already sealed saga plan because the descriptor itself binds every referenced artifact.

Every invocation stores one canonical, schema-versioned operation input envelope containing the saga and descriptor identity, overall public input, and the exact per-step input values. The operation checksum covers the complete envelope, while each step input also receives the domain-separated checksum used by action receipts. The envelope is capped at one MiB and reconstructs every first dispatch after process or deployment restart; checksums alone are never treated as durable input storage.

Saga support MUST include:

- caller-supplied or generated idempotency keys;
- durable step inputs, outputs, and attempts;
- idempotent forward actions;
- explicit compensation functions;
- retryable and terminal error classification;
- compensation-failed and operator-intervention states;
- duplicate-delivery handling;
- timeout and cancellation policy;
- complete audit history;
- no exactly-once guarantee.

Nozzle MUST surface partial commitment honestly.

Saga version 1 is deliberately serial. It executes forward actions in sealed order, stops launching forwards after terminal failure, and reconciles an unknown forward before compensation. Confirmed forward effects compensate in strict reverse order. An unknown compensation is reconciled before continuing. Compensation is a new effect, not rollback; failed or unknown compensation surfaces partial commitment and may require intervention. Cancellation and timeout are durable requests that stop new actions, reconcile in-flight unknowns, and then compensate confirmed effects; they are not claims that in-flight work stopped.

Every D1 saga action writes its shard-local idempotency and result receipt atomically with the application mutation. External actions require an adapter that proves idempotency or provides an observation oracle. A noncompensable mutation is rejected unless it is marked irreversible, ordered last, and protected by sealed authorization. Sagas use the canonical operation envelope rather than an independent orchestration authority.

The D1 action adapter accepts only trusted mutation plans that all name one exact sealed physical target: shard binding, shard ID, bucket ID, route epoch, schema ID, and full partition digest. Before dispatch it canonicalizes the exact action input, ordered mutation plans, and deterministic output, and binds their domain-separated checksums to the accepted Control attempt, descriptor action artifact, saga, operation, step, phase, and idempotency key. It executes each authorization/data statement pair followed by one plain shard-receipt `INSERT` as a single D1 batch. The receipt insert has no conflict suppression; a duplicate or contradictory receipt, stale ownership, inactive schema, or partition fence aborts and rolls back the complete batch. A final receipt trigger repeats the ownership, schema, and fence check so a zero-row guarded mutation can never be mistaken for a committed effect.

After every apparent success, error, or timeout, the adapter opens a new `first-primary` session on the same sealed D1 binding and observes the exact idempotency key. An exact checksum-verified receipt proves `applied`; authoritative absence proves `not_applied`; a mismatched or malformed receipt, failed observation, or unavailable primary is `indeterminate`. Recovery never reroutes an effect observation through the current route manifest. The immutable receipt stores the accepted Control checksum, action artifact identity, physical target, canonical output, and input, mutation, output, and receipt checksums. Its D1 server timestamp is audit metadata and is deliberately excluded from effect identity.

Each saga projection version and its operation-effect receipt commit in one Control D1 batch and reference one exact immutable operation-transition ID under the active fence. Canonical operation step IDs are `saga:init`, `saga:settle`, `saga:termination`, and `saga:<phase>:<descriptor-step-id>`. The projection store rejects a transition whose step, state, or attempt identity does not match the requested saga transition. Action receipt acceptance additionally requires both the matching canonical operation-step state and the matching saga action state, so an operation transition alone cannot authorize dispatch. Saga action operation steps are declared independently in the generic plan because a failed forward must not make compensation ineligible through ordinary success-only dependency edges; the sealed serial saga state machine remains the eligibility oracle.

The saga plan compiler emits required `saga:init` and `saga:settle`, conditional `saga:termination`, and every possible conditional forward and compensation action before execution. Action steps use `saga_receipt`, have no ordinary dependency edges, and bind the descriptor artifact, schema, static input, lease, and recovery contract. An irreversible forward is an irreversible checkpoint and has no compensation step. Terminal orchestration marks every unchosen conditional path `not_required` using the checksum-verified terminal saga projection as decision evidence, then advances `saga:settle` only after the complete projection/effect chain verifies.

Control-plane saga attempt receipts are append-only dispatch and observation evidence; they do not replace the shard-local receipt that a D1 application mutation commits atomically with its data change. Attempt inputs, evidence, outputs, and errors are canonical JSON capped at one MiB, with domain-separated payload, acceptance, and outcome checksums. The version 2 acceptance receipt binds the exact descriptor action key, saga and operation identity, phase, purpose, idempotency key, payload, lease fence, and tagged causal-attempt identity. A first forward effect has no cause; an observation names the exact unknown effect attempt it investigates; and compensation names the exact confirmed forward effect it counteracts. Control schema checks and triggers independently enforce those causal relationships against the checksum-verified saga projection and immutable attempt history. An accepted attempt without a terminal outcome remains accepted evidence of possible dispatch and is recovered as unknown; absence of an acceptance receipt is the only Control-plane proof that dispatch did not occur.

The handler invocation boundary accepts only exact, canonicalizable result envelopes and caps every JSON value at one MiB before persistence. Effect exceptions and timeouts are redacted and classified `unknown`; observation exceptions and timeouts are redacted and classified `indeterminate`. The boundary abort signal is advisory only: a timeout never proves that an external effect stopped or was not applied, and late handler completion cannot change the chosen durable outcome.

## 18. Backup, restore, and deletion

### 18.1 Recovery objectives

Every environment MUST declare or accept documented defaults for:

- recovery point objective;
- recovery time objective;
- Time Travel retention expectations;
- long-retention export policy;
- control-manifest retention;
- restore-drill frequency.

### 18.2 Backups

Nozzle MUST integrate D1 Time Travel as the primary point-in-time recovery mechanism where supported.

Optional long-retention backups MUST:

- export to user-owned R2 or a configured destination;
- disclose that export can make a D1 database unavailable;
- run only under an explicit maintenance and availability policy;
- encrypt sensitive manifests or exports when configured;
- record checksum, schema, shard identity, jurisdiction, route epoch, and creation bookmark;
- verify downloadability and integrity;
- expire according to a user-owned retention policy.

Control manifests MUST be exported independently of application databases and MUST be integrity-protected.

### 18.3 Shard restore

A shard restore MUST:

1. fence and remove the shard from writable routing;
2. record the current external topology and ownership state;
3. perform the provider restore;
4. account for canceled in-flight queries;
5. reinstall or reconcile current Nozzle metadata, triggers, and route epochs;
6. prevent restored historical metadata from resurrecting ownership;
7. verify schema and bucket contents;
8. return the shard only after routed smoke tests pass.

Restoring application data and restoring routing authority are separate concerns.

### 18.4 Control-database recovery

Control recovery MUST reconstruct state from:

- signed exported manifests;
- shard-local ownership and schema records;
- Cloudflare resource inventory;
- immutable audit and operation records where available.

Reconstruction MUST select a safe authoritative route epoch and MUST never blindly trust a restored older central route map over newer shard-local fencing state.

### 18.5 Restore drills

`nozzle restore-drill` MUST create or select an isolated recovery target, exercise the documented recovery path, verify data and routing, and clean up safely. A backup that has not passed a restore drill is not considered verified.

### 18.6 Retirement and deletion

Retirement MUST require:

- zero writable bucket ownership;
- zero required route references;
- zero active bindings after topology rollout;
- successful schema and data verification on destinations;
- completed safety window;
- optional verified export according to policy;
- explicit irreversible approval;
- audit record of the final provider response.

Automatic deletion MUST be disabled unless the environment explicitly opts in. Nozzle MUST not claim that a provider-deleted D1 database can be restored.

## 19. Automation modes

### 19.1 Local and CI control plane

Nozzle MUST run from a developer machine or CI using Wrangler authentication profiles or narrowly scoped environment credentials.

Local and CI execution MUST support:

- dry-run planning without mutation;
- non-interactive execution;
- resumable operation IDs;
- structured output and stable exit codes;
- GitHub Actions examples with strict resource prefixes and cleanup;
- no credential values in generated output or logs.

Wrangler OAuth support MUST invoke a pinned compatible Wrangler with an explicitly selected profile. Nozzle MUST NOT read Wrangler's private credential files. It MUST use JSON output for identity and observation where available; a mutating Wrangler command without structured output is an unknown-outcome step whose result is established by a later structured list or inspection. Operations without a safe Wrangler observation path require a narrowly scoped API token.

### 19.2 User-owned controller

The complete release MUST include an optional controller deployed into the user's Cloudflare account for continuous monitoring and reconciliation.

The controller MAY use Workers, Workflows, Queues, and user-owned telemetry resources. The control database remains the long-term operation ledger even when provider workflow history expires.

Two mutation policies MUST be available:

- **recommendation mode:** controller records and surfaces plans; a local or CI operator applies them;
- **autonomous mode:** controller applies approved policy using a narrowly scoped management token stored as a user-owned secret.

Autonomous mode is explicit opt-in. Normal runtime routers never receive the management token.

## 20. Observability and cost

### 20.1 Data ownership

Nozzle sends no telemetry to Nozzle maintainers.

Operational telemetry required for automation is stored in the user's Cloudflare account, local machine, or configured destination. The user controls sampling, retention, and export.

### 20.2 Required signals

Nozzle MUST expose:

- query latency, query counts, rows read, rows written, and response bytes;
- storage and projected growth;
- overload, retry, timeout, and error rates;
- bucket distribution and movement status;
- hot shards, buckets, and tenants;
- route-cache hit, miss, refresh, and stale-rejection rates;
- manifest, topology, and route epochs;
- schema-version distribution and drift;
- migration, backfill, saga, backup, and restore progress;
- Worker, router, binding, and API-rate-limit utilization;
- controller health and lease ownership;
- cost estimates and cost-guard events.

Provider D1 metrics alone are insufficient for per-bucket or per-tenant visibility. Nozzle runtime instrumentation MUST provide those dimensions with bounded sampling and cardinality controls.

### 20.3 Outputs

Observability outputs include:

- CLI status and diagnostics;
- structured redacted logs;
- Workers logs and traces;
- OpenTelemetry-compatible logs and traces;
- user-owned Analytics Engine metrics where configured;
- Prometheus-compatible export from the local/controller surface;
- a fully local open-source dashboard;
- optional user-configured external destinations.

Nozzle MUST not claim that Cloudflare's native OpenTelemetry export currently supports metrics when it supports only logs and traces.

### 20.4 Local dashboard

The dashboard MUST:

- bind to loopback by default;
- protect state-changing actions against CSRF and accidental exposure;
- never display secret values;
- show the source and age of every metric;
- show uncertainty for estimated or delayed data;
- present operation recovery instructions;
- meet WCAG 2.2 AA accessibility requirements;
- work without a Nozzle-hosted service.

### 20.5 Cost model

Cost estimates MUST include:

- D1 rows read, rows written, storage, and index/write amplification;
- movement copy, verification, triggers, and mutation outbox overhead;
- Workers CPU and requests;
- Service Binding execution effects;
- Workflows and Queue operations and retries;
- Analytics Engine, logs, traces, and external export;
- R2 backup storage and operations;
- temporary double storage during migration or movement.

Cost guards MUST be available per operation and per environment.

## 21. Security

### 21.1 Credentials and permissions

- Local and CI control-plane commands use Wrangler OAuth profiles or narrowly scoped API tokens.
- The user-owned controller and complete non-interactive CI path use a narrowly scoped API token with structured Cloudflare APIs.
- Runtime Workers use bindings and do not receive management credentials.
- An autonomous controller token is opt-in, stored as a user-owned secret, and scoped to the minimum required account and resources.
- Nozzle MUST validate required permissions before mutation and explain missing permissions without printing credentials.
- Secret values MUST never appear in plans, manifests, generated configuration, logs, fixtures, snapshots, or support bundles.

### 21.2 Tenant and route security

- Tenant scope is enforced by the safe query API and generated database guards.
- Route manifests and session tokens are integrity-protected.
- Partition keys are hashed where raw values are unnecessary.
- Router RPC exports only the documented capability surface.
- Every mutation is tied to an actor, environment, operation ID, and idempotency key.
- Unsafe raw SQL and administrative operations are explicitly identified and auditable.

### 21.3 Data security and residency

Nozzle relies on Cloudflare's documented D1 encryption in transit and at rest while preserving user-selected jurisdiction constraints. Nozzle MUST clearly distinguish provider encryption from optional application-level encryption.

### 21.4 Supply chain

The project MUST provide:

- dependency and license review;
- secret scanning;
- signed releases and provenance;
- checksums for release artifacts;
- reproducible builds;
- protected publishing with trusted automation;
- package-content allowlists;
- security policy and private vulnerability reporting;
- documented dependency-update and compromised-release response.

## 22. Packages and generated artifacts

The release includes the following provisional package boundaries:

- `@nozzle/core`
- `@nozzle/drizzle`
- `@nozzle/cloudflare`
- `@nozzle/cli`
- `@nozzle/testing`
- `create-nozzle`

The unscoped npm name `nozzle` is already registered by an unrelated project. No public package name is final until the project controls the relevant npm organization or package. Distribution MUST use an owned namespace, avoid dependency-confusion ambiguity, and complete an independent name and trademark review. The private workspace package MAY retain the working name.

The package names and boundaries MAY change before release if namespace ownership or a smaller public API requires it, but all required runtime, router, controller, telemetry, dashboard, and test functionality MUST be publicly available. The installed CLI binary SHOULD remain `nozzle` if the final name is cleared.

The current implementation compatibility baseline is exact and deliberately narrow: Node.js 22 and 24 LTS, TypeScript 5.9.3 for the repository toolchain, Drizzle ORM 0.45.2, Wrangler 4.110.0, Workers types 5.20260710.1, Vitest 4.1.10, and `@cloudflare/vitest-pool-workers` 0.18.4. These versions are implementation inputs, not a release claim: every declared Node.js, operating-system, architecture, package-manager, Workers-runtime, authentication, topology, and D1 combination still requires the complete compatibility evidence in section 25.22. A version is added, removed, or ranged only through an explicit compatibility decision and passing compile, local-runtime, generated-artifact, and remote-D1 probes. Unsupported versions fail installation or `doctor` rather than running optimistically.

Third-party declaration files are checked through the supported public import and type-fixture matrix; the repository MAY use TypeScript `skipLibCheck` to avoid type-checking unrelated optional database drivers bundled inside Drizzle. Nozzle's own source, emitted declarations, public type fixtures, and packed-package consumer projects MUST remain strictly checked without hiding errors through `any`, unsafe double casts, or skipped fixtures.

Generated artifacts include:

- Wrangler JSONC configuration or deterministic generated overlays;
- direct and router binding manifests;
- router Worker projects and RPC schemas;
- typed environment declarations;
- route and topology manifests;
- schema and migration manifests;
- movement-trigger and outbox migrations;
- local D1 fixtures;
- deployment and recovery plans;
- machine-readable CLI and API schemas.

Generated files MUST be deterministic, formatted, checksummed where appropriate, clearly marked, and either intentionally tracked or written under an ignored local-state path.

Wrangler configuration remains authoritative user infrastructure input for one installation, not a competing product source of truth. Nozzle MUST merge or generate configuration without silently discarding user settings or comments.

## 23. CLI

Required commands include:

```bash
# Provisional until the initializer package is reserved.
npx create-nozzle
nozzle init
nozzle dev
nozzle generate
nozzle config validate
nozzle deploy
nozzle migrate
nozzle upgrade
nozzle status
nozzle verify
nozzle plan
nozzle reconcile
nozzle limits
nozzle topology
nozzle adopt
nozzle rebalance
nozzle move
nozzle drain
nozzle quarantine
nozzle backup
nozzle restore
nozzle restore-drill
nozzle gc
nozzle uninstall
nozzle doctor
```

Every mutating command MUST support:

- `--dry-run`;
- structured JSON output with a versioned schema;
- non-interactive mode;
- explicit environment, account, and authentication profile;
- idempotency and resumable operation IDs;
- concurrency and rate-limit controls;
- lock timeout;
- `--yes` only where confirmation can safely be bypassed;
- `--no-color` and stable exit codes;
- explicit rollback, forward-recovery, or irreversible-operation instructions.

Human and JSON output MUST be tested independently. Human text may improve between releases; machine output follows documented compatibility rules.

## 24. Stable errors

Errors MUST be typed, stable, actionable, redacted, serializable across router RPC, and include remediation.

Required error families include:

- configuration and capability errors;
- authentication, permission, account, and environment errors;
- partition and tenant-scope errors;
- shard, route, manifest, and epoch errors;
- schema, migration, backfill, and drift errors;
- capacity, cost, and jurisdiction errors;
- API rate-limit and provider-transient errors;
- operation lease, resume, idempotency, and intervention errors;
- movement copy, replay, verification, and cutover errors;
- saga and compensation errors;
- backup, restore, and deletion errors;
- unsupported Drizzle or raw-SQL errors.

Representative stable identities include:

```txt
PartitionKeyMissingError
PartitionKeyMismatchError
TenantScopeRequiredError
UnsafeQueryRequiredError
ShardUnavailableError
RouteVersionConflictError
StaleRouteRejectedError
SchemaDriftError
CapacityGuardError
JurisdictionViolationError
MigrationFailedError
MovementVerificationError
OperationResumeRequiredError
OperationInterventionRequiredError
CrossShardTransactionUnsupportedError
ProviderRateLimitedError
```

## 25. Comprehensive test and verification program

Testing is a release-defining product feature. Nozzle is not complete because it has many tests; it is complete only when the tests establish the documented safety and compatibility claims.

### 25.1 Test principles

- Every public guarantee MUST have one or more named tests and a documented oracle.
- Every normative requirement, numbered acceptance criterion, non-negotiable invariant, and release-blocking proof obligation MUST have a stable evidence identifier declared in this file and cited by its tests. The release verifier MUST reject unknown identifiers, uncovered identifiers, source-only evidence where packed-artifact evidence is required, stale or skipped evidence, commit or artifact hash mismatches, remote-only claims backed only by simulation, and a non-empty remote-resource cleanup ledger.
- The machine-readable acceptance report MUST bind each evidence identifier to test identity, source commit, package and generated-artifact checksums, capability snapshot, topology, runtime versions, deterministic seed where applicable, result-artifact checksums, remote-resource ledger, and verified cleanup result. Generated evidence remains an output of this contract and MUST NOT become a competing product source of truth.
- Safety-critical behavior MUST be tested as invariants across generated states, not only example cases.
- Every persisted state-machine transition MUST have success, retry, duplicate, crash-before, crash-after, and recovery coverage.
- Every discovered defect MUST receive a regression test before it is considered fixed.
- Tests MUST be deterministic by default. Property and fuzz seeds MUST be printed and retained on failure.
- Release tests MUST run against the exact package tarballs and generated artifacts intended for publication.
- Local simulation is necessary but not sufficient. Remote Cloudflare tests are mandatory.
- A skipped, focused, quarantined, or known-failing safety test blocks release.
- Flaky tests are defects. Re-running a failed test until it passes is not an acceptable release policy.

### 25.2 Coverage and mutation gates

Minimum release gates:

- routing, canonical hashing, ownership, fencing, movement, migration, restore, saga, lease, and operation state-machine modules: 100% decision and branch coverage;
- the same safety-critical modules: at least 95% mutation score, with surviving mutants reviewed and documented individually;
- repository overall: at least 95% line coverage and 90% branch coverage;
- no coverage exclusions in safety-critical production code;
- generated code is tested through generation snapshots, schema validation, compilation, integration, and runtime behavior rather than excluded without replacement evidence.

Coverage numbers are a floor, not proof of correctness. Property, model, integration, and chaos tests remain mandatory.

### 25.3 Unit tests

Use Vitest for isolated behavior including:

- configuration parsing, defaults, validation, and upgrades;
- capability registry and conservative fallback behavior;
- canonical partition encoding and hash test vectors;
- bucket selection and placement;
- route manifests, cache refresh, integrity checks, and overrides;
- topology and binding generation;
- schema classification and compiler rules;
- tenant-scope query transformations;
- migration planning and classification;
- capacity, cost, and jurisdiction policy;
- error classification, serialization, and redaction;
- retry, backoff, rate-limit, and pagination logic;
- operation plans, checkpoints, leases, fencing tokens, and audit events;
- session-token encoding, validation, rotation, and movement handling;
- fan-out reducers, ordering, cursors, partial results, and budgets;
- saga progression and compensation;
- backup, restore, quarantine, retirement, and deletion preconditions;
- human and versioned JSON CLI output.

### 25.4 Property-based tests

Use `fast-check` or an equivalent generator to establish invariants over large generated inputs:

- the same canonical key and hash version always produce the same bucket;
- different supported runtimes produce the published hash vectors;
- each bucket has exactly one writable owner;
- no legal operation sequence creates two writable owners;
- route epochs are monotonic;
- stale manifests cannot mutate a former owner;
- a partition override always resolves to a reserved bucket rather than directly to a shard;
- a promoted partition cannot be written back into its former shared bucket while unrelated partitions remain writable;
- a full-digest override collision is rejected rather than conflated;
- rebalancing preserves total ownership without gaps after completion;
- aborting before cutover preserves source authority;
- forward recovery after cutover converges on destination authority;
- generated manifests and configurations are deterministic;
- resume from every checkpoint is equivalent to uninterrupted execution;
- duplicate execution is equivalent to one logical execution;
- partition-scoped queries never widen their tenant predicate;
- placement never violates jurisdiction or capacity constraints;
- fan-out pagination neither duplicates nor omits rows for a fixed topology and quiescent dataset; under concurrent mutation it satisfies the documented monotonic keyset contract without claiming a snapshot;
- configuration parse/serialize/upgrade round trips preserve meaning;
- error redaction never reveals generated secret values.

Generated counterexamples MUST be minimized and stored as durable regression fixtures.

### 25.5 Model-based state-machine tests

Maintain executable reference models for:

- bucket ownership;
- partition promotion and cutback;
- movement;
- schema migration;
- topology deployment;
- saga execution;
- backup and restore;
- shard retirement;
- controller leader election and leases.

The implementation and reference model MUST be driven through randomized command sequences and compared after every step.

The ownership model MUST include at least:

```txt
unassigned
preparing
copying
catching_up
read_only
writable
quarantined
retired
intervention_required
```

`source` and `destination` are recorded as movement roles rather than encoded into ownership state names. Only canonical state `writable` authorizes application mutation.

Illegal transitions MUST be tested and rejected.

### 25.6 Type tests

Use `tsd`, `expect-type`, or equivalent compile-time tests for:

- Drizzle schema inference;
- scoped client query and result types;
- insert partition-key validation;
- global versus sharded clients;
- direct versus router clients;
- session-token types;
- custom placement and telemetry adapters;
- environment bindings generated by Wrangler and Nozzle;
- unsupported cross-shard operations;
- invalid configuration rejection;
- supported and unsupported raw-SQL escape hatches;
- public package exports under all supported module-resolution modes.

Type tests MUST contain positive and negative fixtures and MUST run against the packed release artifacts.

### 25.7 Schema compiler and SQL tests

Maintain generated and hand-written schemas covering:

- simple and composite primary keys;
- foreign-key graphs and cycles;
- indexes, unique constraints, views, triggers, and generated values;
- nullable and non-null partition keys;
- text, integer, real, boolean mapping, timestamp mapping, JSON, and BLOB values;
- reserved-name collisions;
- sharded, global, internal, and unmanaged tables;
- supported D1 SQL and deliberately unsupported SQLite features;
- migrations that rebuild tables;
- destructive and ambiguous changes;
- schema drift in each object type.

Generated SQL MUST be parsed or executed against the supported local D1 runtime, not validated only by snapshots.

### 25.8 Tenant-isolation and adversarial query tests

Isolation tests MUST create multiple tenants sharing one physical shard and attempt to cross boundaries through:

- selects without predicates;
- updates and deletes without predicates;
- mismatched insert values;
- joins and relations;
- aliases, subqueries, common-table expressions, unions, and nested predicates;
- prepared statements and reused query builders;
- raw SQL and unsafe APIs;
- malformed and adversarial partition keys;
- stale route manifests;
- direct and router topology;
- movement and mixed-schema windows.

The safe API MUST never return or mutate another tenant's rows. Dedicated mode MUST additionally verify database-level isolation.

Isolation tests MUST include stale clients writing a promoted tenant to its former shared bucket while other tenants continue writing that bucket.

### 25.9 Worker and D1 local integration tests

Use `@cloudflare/vitest-pool-workers`, Wrangler, Miniflare, and workerd to test:

- real D1 prepared statements and result metadata;
- D1 batch transaction rollback;
- generated ownership and mutation triggers;
- direct bindings with multiple local D1 databases;
- router Workers and Service Binding RPC;
- on-demand operation Workers, administrative leaf RPC, and capability rejection;
- local route manifests and topology updates;
- D1 Sessions and bookmark behavior;
- local migrations, backfills, movement, restore reconstruction, and cleanup;
- Workflows and Queue integration where used;
- remote bindings in dedicated, clearly identified test jobs.

Because the Workers Vitest pool may inject Node compatibility flags, a separate deployment and runtime suite MUST prove that generated production configuration declares every compatibility feature actually required.

### 25.10 Router transport compatibility suite

The custom router-side Drizzle transport MUST be tested against the same behavior matrix as native direct mode:

- prepared statement parameter types and nulls;
- all supported query result forms;
- inserts, updates, deletes, returning clauses where supported, and selects;
- joins, relations, aliases, subqueries, and common-table expressions within one partition;
- batch transaction success and rollback;
- D1 metadata preservation;
- D1 error identity and cause preservation;
- session bookmarks;
- RPC serialization limits;
- large and streamed results according to documented limits;
- timeout, cancellation, caller disconnect, and router restart;
- mixed compatible caller and router versions.

For every supported Drizzle operation, direct and router mode SHOULD produce semantically equivalent results and errors.

### 25.11 Remote Cloudflare integration tests

CI MUST create isolated temporary Cloudflare resources and validate the real platform:

- OAuth profile and API-token authentication paths;
- account and permission detection;
- D1 creation, listing, pagination, inspection, update, and deletion;
- location and jurisdiction creation constraints;
- direct binding deployment;
- router and Service Binding deployment order;
- on-demand operation-plane deployment, authenticated invocation, restart, and cleanup;
- generated configuration and `wrangler types`;
- real D1 queries, atomic batches, topology-specific native transactions where claimed, errors, metadata, and limits;
- migrations across multiple databases;
- read replication enablement and D1 Sessions;
- Time Travel bookmark and restore;
- provider export behavior under an explicit test maintenance window;
- GraphQL metrics and experimental-insight capability detection;
- API rate-limit headers, throttling, and backoff;
- Workers logs, traces, and configured telemetry destinations;
- resource cleanup after success, failure, cancellation, and CI termination.

Remote resources MUST use a unique public-safe prefix, creation ledger, maximum budget, TTL tag or equivalent ownership metadata, and a guaranteed cleanup job. Cleanup MUST verify absence rather than assuming delete success.

No production or personal database may be used as a test target.

### 25.12 Remote scale tiers

The release suite MUST include:

- a small remote fleet used continuously for ordinary integration tests;
- a larger remote fleet used for release qualification within a declared budget;
- a 50,000-shard control-plane simulation using production planning, state, manifest, and operation code;
- millions of generated partition keys and a route map at the documented maximum supported size.

Simulation MUST use the same planning, state, manifest, and operation code as production. Provider calls MAY be represented by a contract-tested fake whose responses are derived from recorded, redacted Cloudflare API schemas and failure cases.

### 25.13 API contract tests

The Cloudflare adapter MUST have contract tests for:

- pagination;
- structured success and error envelopes;
- HTTP 429 and `Retry-After` behavior;
- transport timeouts and connection loss;
- malformed or partial responses;
- unknown new enum values and fields;
- successful provider mutation followed by a lost response;
- a non-JSON Wrangler mutation followed by structured reconciliation;
- eventual observation delay after creation, deployment, update, and deletion;
- API and Wrangler version differences;
- capability absence and conservative fallback.

Nozzle MUST use structured APIs or output where available and MUST NOT depend on scraping unstable human CLI text for correctness.

### 25.14 Movement data matrix

Movement tests MUST cover:

- empty buckets;
- one row and millions of rows;
- every supported data type;
- nulls, empty strings, zero values, maximum-size values, Unicode, and BLOBs;
- simple and composite primary keys;
- deep acyclic foreign-key graphs;
- rejection of self-referential and cyclic foreign keys without a verified custom movement adapter;
- insert/update/delete storms during copy;
- repeated updates to one row;
- delete followed by reinsert;
- schema version changes before movement and blocked schema changes during movement;
- outbox backpressure;
- destination full or overloaded;
- source and destination transient failures;
- duplicate replay;
- checksum mismatch;
- cutover with stale direct and router clients;
- partition promotion from a shared bucket, reserved-bucket routing, persistent former-source fencing, and cutback;
- forced full-digest override collision rejection;
- session tokens issued before cutover;
- rollback before cutover and forward recovery after cutover;
- quarantine expiry and source cleanup.

### 25.15 Failure-injection and crash tests

Every long-running operation MUST have an automatically generated failure-injection matrix. Inject failure:

- before and after every persisted step;
- before and after every provider mutation;
- before and after every route or ownership change;
- after a provider succeeds but before Nozzle records success;
- during lease renewal;
- during audit logging;
- during controller handoff;
- during final verification;
- during cleanup.

Faults include:

- process termination;
- runtime restart;
- duplicate Queue delivery;
- stale or stolen lease;
- stale lease during an external provider call and sealed destructive authorization;
- clock skew within documented assumptions;
- API timeout, 429, 5xx, malformed response, and DNS failure;
- D1 overload, reset, memory, CPU, and query timeout errors;
- partial copy and replay;
- migration failure;
- route-cache staleness;
- partial router deployment;
- control database temporarily unavailable;
- telemetry and audit destination unavailable;
- destination capacity exhaustion.

After each fault, the operation MUST either complete safely or stop in a documented recoverable state.

### 25.16 Concurrency and race tests

Test overlapping and adversarial attempts including:

- two controllers reconciling the same fleet;
- concurrent moves for the same bucket;
- migrations during movement;
- restore during deployment;
- retirement while a binding is still referenced;
- simultaneous route refresh and cutover;
- lease expiry during a slow provider request;
- competing execution attempts after irreversible authorization is sealed;
- duplicate operation IDs and conflicting idempotency keys;
- fan-out while topology changes;
- saga compensation while the original worker resumes.

Linearizability-style history checking SHOULD be used for ownership and fencing operations.

### 25.17 Migration fixture fleet

Maintain fixture fleets for:

- fresh installation;
- every historical application schema included in compatibility guarantees;
- every historical Nozzle control and shard-local schema;
- every supported Drizzle migration layout;
- expand-deploy-backfill-contract sequences;
- interrupted and resumed migrations;
- canary failure;
- schema and trigger drift;
- destructive migration rejection;
- mixed application, router, and schema versions;
- failed Nozzle upgrade and forward recovery;
- Time Travel restore after a bad migration.

Migration fixtures MUST be immutable once published so regressions remain reproducible.

The migration suite MUST also test one, many, and all-shard failure in every wave; failures after the provider applied a migration but before Nozzle recorded success; mixed-version application traffic; transient retry; permanent failure; unknown outcome reconciliation; zero and non-zero scheduling failure budgets; and resume without reapplying verified shards.

### 25.18 Backup and recovery tests

Tests MUST demonstrate:

- retrieval and validation of Time Travel bookmarks;
- restore cancellation of in-flight work;
- restoration of one shard without route corruption;
- prevention of resurrected historical ownership metadata;
- control-database reconstruction from manifests and shard inspection;
- verified R2 export integrity;
- recovery with missing, stale, or corrupted manifests;
- undo of a Time Travel restore where supported;
- documented behavior after irreversible database deletion;
- RPO and RTO measurement;
- automated restore drills and cleanup.

### 25.19 Fan-out and saga tests

Fan-out tests MUST cover:

- deterministic reducers;
- global sorting and top-K;
- cursor pagination across uneven and empty shards;
- topology change between pages;
- per-shard timeout and partial-result policies;
- response-byte and cost budget enforcement;
- cancellation and late responses;
- no global-snapshot claim.

Saga tests MUST cover every combination of forward success/failure, retry, duplicate delivery, compensation success/failure, process restart, and operator intervention for generated step counts within practical bounds.

### 25.20 Performance tests

Use k6 and purpose-built harnesses to measure:

- canonical hashing and cached route resolution;
- manifest loading, refresh, compression, and memory;
- native direct versus router transport latency;
- router RPC serialization and result sizes;
- tenant-scope query transformation overhead;
- ownership-trigger and mutation-outbox write amplification;
- migration and backfill throughput;
- movement base-copy and replay throughput;
- control-plane planning for 50,000 shards;
- millions of partition keys and maximum route-map size;
- bounded fan-out behavior under slow and failed shards;
- dashboard and CLI behavior on large fleets.

Release budgets include:

- cached route resolution adds less than 1 ms Worker CPU at p99 in the supported benchmark environment;
- normal direct queries add no network lookup for routing;
- router queries add only the documented generated Service Binding path;
- route data and runtime caches remain below an explicit memory budget with safety headroom inside the Worker limit;
- no algorithm used on the request path scales linearly with total fleet size unless the operation is explicitly fleet-wide;
- large CLI and dashboard views paginate or stream rather than load the full fleet into memory.

Benchmarks MUST publish hardware/runtime details and distinguish local simulation from remote measurements.

### 25.21 Load and soak tests

Release qualification MUST include:

- sustained mixed reads and writes across many buckets;
- hot-tenant and hot-shard workloads;
- route refresh and topology rollout under traffic;
- migration and movement under traffic;
- controller reconciliation under API throttling;
- a minimum 72-hour soak with no unexplained memory growth, ownership divergence, stalled operation, unbounded outbox, or unrecovered error.

Soak failures block release and produce retained diagnostics and regression tests.

### 25.22 Compatibility matrix

CI MUST cover the declared supported matrix of:

- current and previous supported Node.js LTS releases;
- exact supported Wrangler ranges;
- exact supported Drizzle ORM and Drizzle Kit ranges;
- Workers compatibility dates and required flags;
- npm, pnpm, Yarn, and Bun where claimed;
- macOS, Linux, and Windows CLI behavior;
- x64 and ARM64 where claimed;
- direct and router topology;
- local simulated and remote D1 bindings;
- free-plan behavior and paid-plan behavior where practical;
- authentication profiles and scoped API tokens;
- supported jurisdictions and read-replication settings.

Unsupported combinations MUST fail during installation or `doctor` with an actionable message.

### 25.23 Security tests

Security testing MUST include:

- dependency, malware, license, and secret scanning;
- SQL injection and unsafe-query boundary tests;
- tenant isolation and confused-deputy tests;
- malicious configuration, manifest, identifier, path, and environment input fuzzing;
- path traversal and symlink attacks in generators and backup handling;
- session-token and route-manifest tampering;
- replay and idempotency abuse;
- RPC capability exposure review;
- administrative leaf-RPC operation-key forgery, replay, expiry, and rotation;
- log, error, support-bundle, and snapshot redaction;
- compromised or insufficient Cloudflare token behavior;
- package tarball content inspection;
- provenance, signature, and checksum verification;
- dashboard CSRF, local binding, and content-security testing;
- denial-of-service limits for fan-out, result size, configuration size, and operation creation.

A tenant-isolation, route-ownership, credential-exposure, migration-corruption, or data-loss defect always blocks release.

### 25.24 CLI and developer-experience tests

In clean temporary projects, tests MUST execute the documented workflow:

```bash
npx create-nozzle
nozzle init
nozzle dev
nozzle generate
nozzle config validate
nozzle migrate
nozzle verify
nozzle deploy
nozzle status
```

Test:

- interactive and non-interactive modes;
- TTY and non-TTY output;
- JSON schema compatibility;
- cancellation and resume;
- wrong account and environment prevention;
- paths containing spaces and Unicode;
- read-only file systems and permission failures;
- package-manager-specific execution;
- upgrade and uninstall;
- actionable error remediation;
- no secret or personal path leakage.

Every documentation command and code example MUST compile and execute in CI.

### 25.25 Generated-artifact and reproducibility tests

- Running generation twice with identical inputs MUST produce a byte-identical result.
- Generation on supported operating systems MUST be semantically identical and formatted consistently.
- Generated Wrangler configurations MUST validate against the installed Wrangler schema.
- Generated Workers MUST type-check, dry-run deploy, and pass startup checks.
- Release builds from the same source and toolchain MUST be reproducible.
- Package tarballs MUST match an explicit file allowlist and contain no local state, credentials, logs, coverage, or unrelated generated artifacts.
- Every documented npm package MUST be controlled by the project, and dependency manifests MUST NOT resolve the product to the unrelated unscoped `nozzle` package.

### 25.26 Dashboard tests

Test the local dashboard for:

- large-fleet pagination and filtering;
- accurate source and age labeling;
- operation progress and recovery instructions;
- incomplete and delayed metrics;
- error and empty states;
- keyboard navigation;
- screen-reader semantics;
- contrast, zoom, and reduced motion;
- WCAG 2.2 AA automated and manual checks;
- loopback binding and CSRF protection;
- no secret display.

### 25.27 Cost tests

Use deterministic workload fixtures to compare predicted and observed:

- rows read and written;
- index and trigger amplification;
- movement temporary storage;
- Worker, Workflow, Queue, telemetry, logging, and backup usage;
- guard behavior when a plan exceeds its budget.

Cost-model drift beyond a documented tolerance blocks release until recalibrated or clearly reported.

### 25.28 Documentation and supportability tests

- Link-check all documentation.
- Compile and execute every example.
- Validate machine-readable schemas and snippets.
- Run tutorials from clean environments.
- Perform recovery labs using only public documentation.
- Have an operator unfamiliar with the implementation recover injected migration, movement, and control-database failures without maintainer-only knowledge.
- Validate `llms.txt`, `AGENTS.md`, and the installable Nozzle skill against current public APIs.

### 25.29 CI execution classes

The repository MAY divide test execution by trigger, but every class is mandatory before release:

- **Per change:** formatting, lint, type, unit, property, schema, local integration, security scans, package checks.
- **Scheduled:** extended property runs, mutation tests, compatibility matrix, remote Cloudflare integration, cleanup audit.
- **Release qualification:** full remote fleet, chaos matrix, migration history, restore drills, scale, performance, cost, accessibility, reproducibility, and 72-hour soak.

Classification controls execution time, not product scope.

### 25.30 Test result policy

Release requires:

- all required jobs green on the release commit;
- zero skipped, focused, quarantined, or expected-failing release tests;
- zero untriaged flakes in the qualification window;
- published coverage, mutation, benchmark, compatibility, remote-resource cleanup, and soak reports;
- signed test provenance linked to the release artifact;
- a machine-readable release acceptance report mapping every requirement to evidence.

## 26. Documentation and learning assets

Documentation is release scope and MUST include:

- five-minute quickstart;
- beginner auto-mode guide;
- complete architecture and trust-boundary documentation;
- partition-key and identifier design;
- global versus sharded schema rules;
- tenant-isolation guarantees and unsafe escape hatches;
- local development and remote bindings;
- direct and router topology;
- deployment and mixed-version rollout;
- migrations, backfills, and drift;
- movement protocol and failure recovery;
- read replication and Nozzle session tokens;
- fan-out consistency and pagination;
- saga design and idempotency;
- residency and jurisdiction;
- capacity, cost, and limit planning;
- backup, Time Travel, restore drills, quarantine, and deletion;
- observability and telemetry ownership;
- security and credential models;
- upgrade and uninstall;
- troubleshooting and error reference;
- migration from a single D1 database and adoption of an existing fleet;
- production-readiness checklist;
- complete API, CLI, configuration, manifest, and JSON-output references.

Required learning assets:

- multiple complete applications for SaaS, consumer, time-bucket, and dedicated-tenant designs;
- interactive architecture diagrams;
- runnable tutorials;
- failure-recovery labs;
- public benchmark methodology and results;
- documented architectural decisions and threat model;
- `llms.txt`;
- repository `AGENTS.md`;
- installable `skills/nozzle/SKILL.md`;
- copy-paste guidance for Codex, Claude Code, Cursor, and similar tools;
- machine-readable API schemas and examples.

The agent skill MUST teach tools to:

- select a safe partition key;
- classify global and sharded tables;
- use the scoped API correctly;
- avoid unsupported cross-shard assumptions;
- generate and review migrations;
- interpret route, drift, capacity, jurisdiction, and recovery errors;
- run the official tests before presenting work as complete;
- protect public repositories from secrets and local artifacts.

## 27. Open-source project requirements

The entire product is public:

- runtime and router transport;
- CLI and generators;
- controller and operation engine;
- dashboard and telemetry adapters;
- testing packages and failure harnesses;
- deployment and recovery automation;
- documentation, skills, examples, benchmarks, and design records.

The repository MUST include:

- an approved open-source license selected before public release or external contribution intake;
- governance model;
- code of conduct;
- contribution guide;
- security policy;
- RFC and architectural-decision process;
- maintainer responsibilities;
- compatibility and deprecation policy;
- reproducible and signed release process.

Until a license is selected, packages MUST be marked private and unlicensed to prevent accidental publication.

## 28. Complete release acceptance criteria

Nozzle is ready only when all criteria below are satisfied:

1. A beginner can create and deploy a working auto-mode application in under 15 minutes using public documentation.
2. An existing supported single-D1 Drizzle application can be adopted with documented and mechanically verified schema changes.
3. The public safe query API enforces partition scope and cannot read or mutate another tenant in adversarial tests.
4. Auto mode starts with one physical shard and scales through virtual buckets without database-per-user behavior.
5. Direct and router topologies provide the documented equivalent Drizzle behavior.
6. The supported topology handles 50,000 registered physical databases in production-code simulation and has a documented provider-valid path beyond one Worker's binding budget.
7. Capacity planning considers storage, throughput, latency, overload, topology, jurisdiction, temporary headroom, and total cost.
8. Every shard is migrated, versioned, drift-checked, and compatible with the active application and router versions.
9. Bucket and tenant movement survive failure before and after every persisted checkpoint without losing a committed write.
10. No test can produce two writable owners for one bucket.
11. Stale application and router deployments cannot mutate a former owner.
12. Read-replica session tokens remain safe across route changes and bucket movement.
13. Fan-out boundaries, ordering, pagination, partial results, consistency, and budgets are explicit and tested.
14. Sagas are durable, idempotent, compensatable, and honest about partial commitment.
15. Restoring a shard or the control database cannot resurrect stale route ownership.
16. Backup integrity and restore drills meet the declared RPO and RTO.
17. Jurisdiction constraints are preserved through provisioning, movement, replication, backup, restore, and deletion.
18. Every mutating command has dry-run, structured output, audit history, idempotency, resume, and recovery behavior.
19. The controller works entirely in the user's environment and no operation depends on a Nozzle-hosted service.
20. Observability provides enough state to diagnose and recover without private maintainer access.
21. Cost estimates and guards include all Nozzle-induced provider usage.
22. The complete local, remote, compatibility, property, model, mutation, chaos, recovery, security, performance, scale, cost, accessibility, reproducibility, and soak suites pass.
23. There are zero skipped safety tests and zero untriaged flakes in release qualification.
24. Every documentation example compiles and runs.
25. Release artifacts are signed, reproducible, provenance-attested, and contain no secrets, personal data, local state, or unintended files.
26. A recognized open-source license, governance, security policy, and contribution process are in place.
27. The machine-readable release acceptance report maps every requirement in this document to passing evidence.
28. Every published package name and scope is controlled by the project, and the final product name has completed namespace and legal review.

Failure of any criterion means Nozzle is not released.

## 29. Architecture decision register and proof obligations

This section resolves implementation ambiguity in the preceding requirements. These are binding product decisions, not optional suggestions or deferred ideas.

### 29.1 Meaning of bulletproof

No distributed system is literally failure-proof. For Nozzle, bulletproof means:

- safety invariants are explicit and enforced in more than one layer;
- every mutation has a durable identity and observable outcome;
- unknown outcomes remain unknown until reconciled;
- stale components fail closed before mutation;
- recovery is a normal state-machine path, not a private repair script;
- optimized code is checked against a simple reference model;
- remote Cloudflare tests validate assumptions local simulation cannot prove;
- release is blocked until every proof obligation below has passing evidence.

### 29.2 Non-negotiable invariants

#### Ownership

- At most one physical shard is writable for a bucket.
- A shard accepts a write only when its local ownership record is writable at the active route epoch.
- Central metadata cannot silently override newer shard-local fencing state.
- Route epochs never decrease.
- An ownership transition belongs to one durable operation ID.

#### Tenant scope

- Every safe sharded read is constrained to the selected partition and bucket.
- Every safe insert contains the selected partition and computed bucket.
- Every safe update and delete is constrained to the selected partition and bucket.
- Ordinary updates cannot change partition keys, primary keys, or internal bucket values.
- Joins cannot cross partitions.
- Unsafe clients are visibly different in name, type, policy, and audit output.

#### Schema and movement

- Routed traffic reaches only a schema compatible with the active application and router protocol.
- Online movement requires exact source and destination application-data schemas.
- A fleet migration succeeds only after every required shard verifies.
- Partial migration success is persisted and resumed rather than hidden or automatically rewound.
- Final movement verification occurs after source fencing and mutation-tail drain.

#### Operations and residency

- Repeating a step does not change its logical result.
- A provider success followed by a lost response becomes `unknown`, not `failed`.
- Expired leases cannot authorize protected D1 transitions after a newer fencing token exists.
- Destructive work requires an immutable irreversible authorization.
- Data, backups, and restores never cross a prohibited jurisdiction or account boundary.

### 29.3 Binding architecture decisions

#### Public API and data model

1. `nozzle.for(key)` returns a Nozzle-owned scoped query surface, not an unrestricted native Drizzle proxy.
2. The safe builder produces a versioned structured execution plan before SQL generation. Post-generation SQL parsing is not an isolation boundary.
3. `nozzle.for(key)` remains synchronous; canonical hashing and route resolution occur lazily on first asynchronous execution and are cached.
4. Portable atomicity is an explicit single-shard batch of pre-built statements. Arbitrary interactive transaction callbacks are topology-specific.
5. Global data uses separate named database groups; it never shares the production control database.
6. One fleet belongs to one Cloudflare account. The CLI may manage multiple accounts only as separate targets and fleets.
7. Every sharded row contains `__nozzle_bucket INTEGER NOT NULL`, while the application partition key remains present.
8. Every sharded table has a stable non-null primary key. Relationships and tenant-local uniqueness include the partition key.
9. Generated database triggers provide final write ownership enforcement. The scoped builder provides read isolation.
10. A promoted tenant leaves a persistent typed partition fence on every former source.

#### Hashing, placement, and routing

11. Hash version 1 is the exact framed SHA-256 encoding defined in section 8.3. The full digest identifies overrides; truncated values never affect correctness.
12. Bucket-space size is immutable after fleet creation: 65,536 by default or 1,048,576 for a verified high-scale profile.
13. Auto mode uses a stable bucket directory. Adding a shard never remaps data without an explicit movement operation.
14. A tenant override maps the full digest to an unused reserved bucket, never directly to a shard.
15. Route manifests use a dense shared-bucket-to-shard table, compact shard descriptors, sorted sparse reserved-bucket routes in the disjoint reserved namespace, sorted full-digest overrides, versioning, and integrity checks.
16. Default auto placement is a deterministic constrained greedy planner with stable tie-breaking and an operator-readable score breakdown.
17. A more complex planner is allowed only after published benchmarks show material benefit and differential tests preserve the reference invariants.

#### Runtime topology

18. Direct and router modes share one scoped execution-plan adapter and one versioned, type-preserving result model.
19. Direct topology remains active while dry-run metadata measurement leaves comfortable binding and transition headroom.
20. Router topology adds one normal application-to-leaf Service Binding hop; there is no always-on gateway Worker.
21. Direct-to-router transition is additive: deploy leaves, deploy dual reachability, activate routes, verify, then remove old bindings.
22. The directory service is cold-path only. An uncertain route returns a retryable error instead of guessing.
23. Safe router RPC accepts only validated structured plans. Unsafe raw SQL and administrative movement RPC are separate policy-gated capabilities.
24. Every active application and router deployment registers protocol, schema, topology, and reachability attestations. Unknown or incompatible callers block cutover.

#### Movement and migrations

25. Movement capture uses generated key-journal triggers. Journal entries contain typed stable keys and metadata, not whole old/new rows.
26. Replay reads current source state: upsert when the key exists and delete when absent. Later source changes always produce later sequences.
27. Capture starts before base copy. Copy uses deterministic byte-bounded keyset pages; replay is ordered and idempotent.
28. The default mover supports acyclic foreign-key graphs in topological order. Cycles, self-references, and application-defined triggers require a fully verified custom adapter or are rejected.
29. Bulk copy, replay, verification, and large backfills use user-owned operation and bounded leaf Workers, not the Cloudflare management API.
30. Brief visible unavailability during a fenced cutover is preferable to dual writable owners or unverified dual writes.
31. Fleet migrations are expand, compatibility rollout, backfill, and contract operations.
32. One failed required shard means failure. Successful shards remain on a compatible additive schema while the fleet enters `mixed_blocked` and recovers forward.
33. Applied shard state and active fleet schema are separate; the fleet barrier advances only after all required shards verify.

#### Control plane, automation, and recovery

34. Control D1 is the durable ledger; user-owned Workflows execute long-running steps. Queue delivery is not in the default control path.
35. Leases use D1 server time and fencing tokens. External provider calls remain idempotent or unknown-outcome steps because Cloudflare APIs do not honor Nozzle fencing tokens.
36. Desired, recorded, and observed state remain distinct. Destructive reconciliation requires agreement or an explicit recovery decision.
37. Audit records are append-only at the application layer and hash-chained, without claiming protection from an administrator controlling the entire account.
38. Recommendation mode is the automation default. Autonomous mutation is explicit opt-in and never gives management credentials to runtime query Workers.
39. Wrangler-profile authentication invokes a pinned Wrangler and uses structured observation. API-token mode uses narrowly scoped structured APIs. Private Wrangler credential files and human output are never correctness inputs.
40. Runtime telemetry is user-owned, bounded, sampled, and free of raw partition keys and bound values.
41. Time Travel is the primary point-in-time recovery mechanism. Exports are explicit maintenance operations, not invisible hot-shard backups.
42. Restored ownership metadata is fenced and reconciled; it never becomes authoritative automatically.
43. Deletion follows quarantine, verification, a safety window, and irreversible authorization.
44. Public source configuration is tracked; live topology, credentials, logs, backups, local databases, and operational state are ignored.
45. Generated files declare generator version, input checksum, commit safety, edit policy, and regeneration command.
46. Public packages use only a namespace controlled by the project. The unrelated unscoped `nozzle` npm package is never a release target.
47. Generic operations are the canonical mutation envelope; domain tables are checked materializations rather than independent operation authorities.
48. Provider dispatch requires a durable accepted-attempt receipt; accepted without a terminal outcome becomes unknown after fencing, while receipt absence proves no dispatch.
49. Provider target, fleet and environment, resource generation, and D1 UUID form resource identity; names alone never authorize adoption or deletion.
50. Fan-out continuations are keyset-based, topology-pinned, cumulative-budgeted, and explicitly non-snapshot.
51. Saga version 1 is serial, descriptor-driven, receipt-backed, and reverse-compensating.
52. This Markdown file and its stable requirement markers are the only normative requirement registry.
53. D1 resource lifecycle stores stable materialization facts; transient execution state belongs only to the canonical operation step.
54. A zero-result observation after ambiguous create remains unresolved and can never authorize an automatic second create.
55. Fan-out continuation state is confidential as well as authenticated: version 1 uses bounded AES-256-GCM tokens and never falls back to hidden server-side cursor state.
56. Fan-out execution preflights every sealed budget, uses bounded per-shard buffer shares, cancels unnecessary work, ignores late responses, and treats actual usage drift above budget as failure.
57. Fan-out reducers run in canonical shard order; exact integer aggregation and deterministic compensated floating aggregation are distinct public contracts.
58. Every saga projection is an exactly fenced materialization of a named canonical operation transition; action dispatch requires agreement between the operation step and saga projection, and serial or reverse-compensation eligibility comes only from the sealed saga state machine.
59. Saga action inputs and outcomes use append-only, domain-separated Control receipts; those receipts prove dispatch history but never substitute for the shard-local atomic mutation receipt required of D1 action adapters.
60. A saga action operation step declares the `saga_receipt` effect protocol. Dispatch requires its exact accepted Control receipt; under a newer fence, receipt absence proves the attempt was not dispatched, while receipt presence without a terminal outcome remains unknown. An operation outcome may commit only when the exact terminal saga receipt agrees with its attempt, purpose, step, and checksum.
61. Conditional operation paths are sealed explicitly and settle as evidenced `not_required`, never as fake successful attempts; required steps cannot be skipped, and at least one required step anchors every plan.
62. Saga handlers are deploy-time, kind-checked, artifact-checksummed registrations; the deterministic compiler seals all possible paths while the descriptor—not unrelated registry membership—binds the exact executable versions.
63. Handler exceptions, timeouts, malformed returns, and late completion cross one strict boundary: effect ambiguity becomes `unknown`, observation ambiguity becomes `indeterminate`, durable JSON is canonical and bounded, and private exception text is never persisted.
64. The canonical operation input envelope is the durable source for saga and per-step inputs; it is descriptor-bound, checksum-verified, one-MiB bounded, and sufficient to reconstruct a first dispatch without process memory.
65. Long-lived conditional operations may use one required settlement step as their sole top-level terminal gate; saga plans always use `saga:settle`, so forward failure cannot terminate the generic operation before compensation and conditional cleanup finish.
66. A D1 saga effect is authoritative only through a shard-local receipt inserted last in the same rollback-capable D1 batch as its trusted mutations; every outcome is reconciled through a new `first-primary` session on that exact sealed binding, with exact presence, absence, and indeterminate evidence kept distinct.
67. Every saga observation and compensation acceptance is causally bound to one immutable earlier attempt in both its versioned checksum and Control schema: observations investigate the current unknown effect, while compensation counteracts the exact forward effect confirmed by the saga projection.

### 29.4 Mechanisms intentionally rejected

The baseline architecture rejects:

- a central route lookup for every query;
- KV as writable route authority;
- raw SQL parsing as the tenant-isolation boundary;
- arbitrary SQL on the safe router RPC surface;
- automatic dual writes as movement authority;
- whole-row old/new outbox images;
- automatic Time Travel rollback after partial fleet migration;
- an always-on gateway between the application and leaf routers;
- Queues in the default control path;
- an opaque solver for ordinary placement;
- a minimal-perfect-hash override map before measurement requires it;
- application-global data in the control database;
- management credentials in runtime routers;
- one fleet spanning Cloudflare accounts;
- silent support for unknown Drizzle or Wrangler versions;
- treating an unknown provider outcome as success or failure;
- provider deletion before quarantine and verification.
- treating offset-paginated provider inventory as a snapshot;
- name-only adoption or deletion;
- blind retry of an ambiguous provider mutation;
- offset-based fan-out continuation;
- continuing an ordered cursor after partial shard failure;
- arbitrary invocation-time saga closures;
- compensation before an unknown forward outcome is reconciled;
- independent domain operation authorities.

### 29.5 Risk register

1. **Scoped Drizzle surface is too narrow:** publish an exact compatibility table and reject unsupported constructs rather than weaken scope.
2. **Drizzle internals change:** support exact ranges through version-specific adapters and contract tests.
3. **Trigger overhead is too high:** benchmark ownership and journal guards; block release if required safety misses the budget.
4. **Manifest rollout is slow:** deploy reachability before authority, retain shard-local rejection, and use cold-path refresh.
5. **Controller credentials are powerful:** default to recommendations, minimize scopes, separate runtime credentials, and audit every action.
6. **Control D1 overloads:** keep it off the query path, batch and paginate, and prove 50,000-shard planning with production code.
7. **Provider capabilities change:** use a versioned capability registry, probes, exact compatibility ranges, and conservative unknown behavior.
8. **Automatic movement oscillates:** require sustained thresholds, hysteresis, cooldown, movement cost, and minimum expected benefit.
9. **Dedicated tenants bypass bucket fencing:** use reserved buckets and persistent former-source partition fences.
10. **Product or package naming is unavailable:** keep names provisional, publish under a controlled namespace, and complete independent naming review.
11. **Per-environment audit-head contention exceeds the bounded retry budget:** benchmark concurrent controllers and scale workloads against the production append path; change the partitioning strategy only if the simple total order misses the release budget.

### 29.6 Release-blocking proof obligations

These are not future iterations. Every obligation MUST have passing evidence before release.

1. **Scoped builder feasibility:** prove the supported Drizzle-compatible subset injects partition and bucket constraints before SQL generation without bypasses through aliases, joins, subqueries, relations, prepared builders, or direct router calls.
2. **Router equivalence:** prove direct and router adapters return equivalent typed values, metadata, stable errors, batch rollback, session behavior, and limit enforcement.
3. **Journal and replay fidelity:** prove every supported primary-key and row type survives journaling, current-state replay, races, deletes, reinserts, BLOBs, and exact integer modes without row-limit overflow or schema-order dependence.
4. **Pre-mutation stale rejection:** prove remotely that every supported write path reaches ownership or partition fencing before application data changes and that rejection is distinguishable from an unknown post-mutation failure.
5. **Deployment reachability barrier:** prove every registered active deployment is enumerated and can reach a destination; missing, forged, expired, partial, and incompatible attestations block cutover.
6. **Manifest scale:** prove default and high-scale manifests fit bundle, startup, memory, and p99 routing budgets in both topologies.
7. **Lease and provider fencing:** prove an expired controller cannot commit a protected D1 transition; prove external effects remain attributable, idempotent or unknown, and destructive calls require sealed authorization.
8. **Restore convergence:** prove restoring older control or shard state cannot create two writable owners when newer surviving metadata exists.
9. **Provider quota truth:** identify discoverable quotas and require explicit verified overrides for raised limits without documented APIs.
10. **Telemetry-to-action quality:** prove bounded sampling identifies sustained hot shards and tenants without unacceptable cardinality, cost, delay, or oscillation.
11. **Public repository safety:** prove source, package tarballs, examples, generated files, reports, and releases contain no credentials, live account metadata, personal paths, or local state.
12. **Partition-promotion fencing:** prove stale writes to a promoted tenant's former shared bucket fail while unrelated tenants remain writable; prove collision rejection and cutback.
13. **Distribution namespace:** prove every documented package is controlled, protected, provenance-tested, and cannot resolve to the unrelated unscoped package.
14. **Credential adapters:** prove the Wrangler-profile/API-token matrix, absence of private auth-file access, and structured reconciliation of every non-JSON mutation.
15. **Operational data plane:** prove remote movement through operation and leaf Workers with bounded memory and payloads, restart at every checkpoint, capability rejection, key rotation, mixed topologies, and measured throughput and cost.
16. **Relational movement boundary:** prove acyclic graphs copy and replay in valid order and unsupported cycles, self-references, and triggers fail configuration unless a custom adapter passes the complete safety contract.
17. **Provider inventory and identity:** prove inventory churn, duplicate names, stale scans, unknown create and delete, direct-UUID absence checks, and cross-target mutation rejection.
18. **Provider crash gaps:** prove every crash point before and after step acceptance, provider-attempt acceptance, terminal receipt, generic step transition, audit append, and process or deployment restart.
19. **Canonical envelope consistency:** prove operation transition, step state, derived status, audit event, provider receipt reference, and every domain checkpoint are atomic or deterministically reconcilable and contradictions fail closed.
20. **Fan-out continuation:** prove concurrent-write semantics, topology mismatch, keyset tie-breakers, cursor tampering and expiry, partial failure without continuation, and cumulative budgets.
21. **Saga recovery:** prove handler-version retention; shard-local effect-receipt atomicity; duplicate and contradictory receipt rollback; stale route, schema, and partition-fence rejection; exact-primary observation on the original binding; every forward and compensation unknown-outcome combination; cancellation; timeout; and process or deployment restart.
22. **Resource projection recovery:** prove every operation-effect/resource-commit crash point, append-only reconstruction, stale observation rejection, identity immutability, tombstone retention, and concurrent version races.

### 29.7 Decision-change rule

A binding decision changes only when a platform fact contradicts it, a proof obligation fails, a benchmark violates a release budget, a simpler design preserves the guarantees, or a security review finds unacceptable risk.

The same change MUST update the affected requirement, tests, threat model, migration or recovery procedure, and acceptance evidence in this file. No change may quietly reduce a public guarantee.

## 30. Primary platform references

This PRD is based on current public documentation and MUST be revalidated during implementation and release qualification:

- [Cloudflare D1 limits](https://developers.cloudflare.com/d1/platform/limits/)
- [Cloudflare D1 management API](https://developers.cloudflare.com/api/resources/d1/)
- [Cloudflare D1 pricing](https://developers.cloudflare.com/d1/platform/pricing/)
- [Cloudflare D1 Workers Binding API](https://developers.cloudflare.com/d1/worker-api/)
- [Cloudflare D1 read replication](https://developers.cloudflare.com/d1/best-practices/read-replication/)
- [Cloudflare D1 migrations](https://developers.cloudflare.com/d1/reference/migrations/)
- [Cloudflare D1 Time Travel](https://developers.cloudflare.com/d1/reference/time-travel/)
- [Cloudflare D1 data location](https://developers.cloudflare.com/d1/configuration/data-location/)
- [Cloudflare D1 metrics and analytics](https://developers.cloudflare.com/d1/observability/metrics-analytics/)
- [Cloudflare D1 debugging and retry guidance](https://developers.cloudflare.com/d1/observability/debug-d1/)
- [Cloudflare API rate limits](https://developers.cloudflare.com/fundamentals/api/reference/limits/)
- [Cloudflare Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- [Cloudflare Service bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/)
- [Cloudflare Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
- [Cloudflare Workflows limits](https://developers.cloudflare.com/workflows/reference/limits/)
- [Cloudflare Queues delivery guarantees](https://developers.cloudflare.com/queues/reference/delivery-guarantees/)
- [Cloudflare Workers testing](https://developers.cloudflare.com/workers/testing/vitest-integration/)
- [Cloudflare Workers OpenTelemetry export](https://developers.cloudflare.com/workers/observability/exporting-opentelemetry-data/)
- [Drizzle ORM with Cloudflare D1](https://orm.drizzle.team/docs/sqlite/connect-cloudflare-d1)
- [npm registry record for the occupied unscoped `nozzle` package](https://registry.npmjs.org/nozzle)
