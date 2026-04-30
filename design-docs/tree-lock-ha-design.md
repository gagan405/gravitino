# Gravitino TreeLock HA Design
[Ref Issue](https://github.com/apache/gravitino/issues/10474)

Gravitino is a **metadata catalog**. It is a unified registry that knows about tables, schemas, and catalogs across many different actual data systems (Hive, Iceberg, Delta, etc.). It does not store the actual table data. It stores _information about_ those tables.

The metadata (information about the tables) are stored in a hierarchical format:

* metalake : top level tenant (workspace/account)
* catalog: a connection to actual data system (hive/ iceberg etc.,)
* db/schema: namespace within the catalog
* table: info about one specific table - column definitions, format etc.

Each entity kind has its own `*_meta` table; one row there is one entity of that kind.

## Problem Statement

Before we get into the problem, we need to understand the background: why we need locks, and how they are implemented.
### Locking requirement in Read/Write

#### Write
Certain write operations are structural. i.e., they do not modify a single row in isolation, but instead modify a parent node and all of its descendants atomically. Examples include:

- `renameTable` : renames the table in the entity store (and may update related rows) while keeping parent/child metadata aligned
- `dropSchema` :  deletes the schema row and every descendant row
- `createTable` : inserts a new table row under an existing schema (and related rows such as columns), extending that subtree

Without coordination, two concurrent structural writes against the same part of the hierarchy can interleave destructively. For example, one node creating a table in `db` while another node is dropping `db` can result in an orphaned table row whose parent no longer exists. 
#### Read

A read operation against a deep path, for example `loadTable(metalake.catalog.db.t1)` :  traverses the full hierarchy. It implicitly assumes that the ancestor nodes (`metalake`, `catalog`, `db`) are stable for the duration of the traversal.

If a concurrent structural write affects `db` (e.g. drop or a rename-like change) while a read is traversing into it, the read may:

- Resolve `db` to its old path, then find its children have already been rewritten to the new path, returning an empty result for a table that exists
- Read `t1`'s row with a namespace that no longer matches its parent schema row - returning internally inconsistent metadata

Gravitino's read path typically issues multiple SQL statements that are not necessarily bounded in one transaction (resolve ancestors, then load the target). A structural write that commits between those steps can produce a torn view. Repeatable read in a **single** transaction would share one snapshot across those statements, but the store access pattern does not generally do that end-to-end; the gap is split reads, not that only `SERIALIZABLE` isolation would fix it.

### Current Lock Implementation

Gravitino implements a `TreeLock` - an in-memory tree of `TreeLockNode` objects managed by a `LockManager`. The tree mirrors the metadata hierarchy exactly. Each node holds a `ReentrantReadWriteLock`.

When any operation executes, it calls `TreeLockUtils.doWithTreeLock(identifier, lockType, operation)`, which:

1. Traverses from the root node down to the target node, acquiring a **read lock on every ancestor**
2. Acquires the **requested lock type (read or write) on the target node itself**
3. Executes the operation
4. Releases all locks in reverse order on completion or exception

For structural writes (rename, drop), the write lock is placed on the **parent** of the target, not the target itself, because the operation modifies the parent's child set. For example, renaming `db.t1` write-locks `db`, not `t1`.

This design means:

- Concurrent reads on different branches of the tree proceed without blocking each other
- A structural write on a node blocks all operations anywhere in its subtree
- A read traversing a path blocks any structural write on any ancestor of that path

The `LockManager` also runs two background threads: a node eviction thread that removes unused `TreeLockNode` objects from memory when the tree grows too large, and a deadlock detection thread that logs any thread holding a lock for more than 30 seconds.

### Problems with current implementation

1. **`LockManager` is per-process.** With HA and a shared metadata DB, logical exclusion does not extend across Gravitino instances. Conflicting structural work can run in parallel; cross-node read/write ordering is not enforced. Any lock diagnostics are local to that JVM.

2. **Wide call surface.** `TreeLockUtils.doWithTreeLock` sits under metalake/catalog managers, many `*OperationDispatcher` classes, tags, authz, etc., i.e. most metadata entry points implicitly rely on this lock.

3. **Insidious failures.** Races tend to show up as store skew (orphans, bad namespaces, dupes), often well after the fact and downstream of the bad write.

## Goals
1. Correct Cross-Node Coordination in HA Mode

The primary goal is to ensure that structural write operations — rename, drop, create at schema level or above — are correctly serialized across all Gravitino nodes in an HA deployment. The solution must make cross-node lock state visible to all participants.

 2. [Preferably] No New Single Point of Failure/ New Infrastructure

The locking mechanism _should_ not introduce a new component whose unavailability renders Gravitino inoperable. Solutions that route all lock acquisition through a dedicated external service — a ZooKeeper ensemble, a Redis instance, an etcd cluster — trade the HA problem for an availability problem.

3. Safe and Reversible Rollout

The solution must be deployable incrementally, with explicit control over each transition step, and with the ability to revert to the current `TreeLock` behavior at any point without a code deployment. Concretely:

- A runtime configuration flag should control which path is active, allowing operators to switch back immediately if unexpected behavior is observed
- [Preferably] The migration should not require any downtime or coordinated restart of all nodes simultaneously
- Each phase of the rollout should be independently observable: metrics and logs should make it clear which lock path handled each operation and whether any conflicts were detected
## Options
#### (Preferred) Option 1 — Distributed Lock Table

Add a `gravitino_distributed_locks` table to the **existing** metadata database (no separate coordination cluster). **Structural writes** acquire a leased row keyed by scope path (e.g. `/metalake/catalog/db`) before mutating entity tables : the same *claim + TTL* pattern as ephemeral ZK nodes or Redis `SET NX`, expressed in SQL.

- **Writes:** distributed claim (and, during rollout, existing `TreeLock`); release after the structural work commits.
- **Reads:** no distributed claim; hierarchical relational-store reads run in one `REPEATABLE READ` transaction so the DB view is not torn. Calls into external catalogs (Hive, Iceberg, …) sit outside that txn and need their own product-level consistency story.

**Meets all goals:** cross-node visibility via the shared DB; no new coord service; dialect-specific JDBC like the rest of the store; feature flag and explicit ordering with `TreeLock` during migration.

**Note:** This option requires quite a substantial refactors at various existing code points. Repeatable-Read require relational-store refactors (session/txn boundaries, `*MetaService` call chains); fencing needs DDL and mutating SQL on structural paths; dispatchers pick up wrappers and clearer engine-vs-store ordering. New lock modules sit on top of that internal work.

The implementation details are noted [below](#implementation-details).
#### Option 2 : External Distributed Lock Service (ZooKeeper / Redis / etcd)

Replace or wrap `TreeLock` with a well-established distributed locking backend. ZooKeeper provides ephemeral nodes with automatic session expiry. Redis provides `SET NX PX` with TTL. etcd provides lease-based locks with built-in fencing tokens.

The hierarchical lock semantics of `TreeLock` map naturally onto ZooKeeper's node paths. Lease expiry and fencing token support are first-class features rather than something Gravitino has to implement itself.

**Why we are not choosing this:**

It directly violates Goal 2. Every one of these systems is a new piece of infrastructure that must be provisioned, monitored, secured, and kept highly available. If the ZooKeeper ensemble or Redis instance becomes unavailable, all Gravitino metadata operations fail, including reads and simple writes that have no need for distributed coordination.

#### Option 3 : Object Storage Conditional Write (S3 / GCS / Azure Blob)

Use cloud object storage as a lock backend. Modern object storage APIs support conditional writes (`PUT` with `If-None-Match: *`), which provide the same `putIfAbsent` semantic needed for lock acquisition. Lock keys map to object paths. A successful conditional PUT means the lock is acquired; a failed one means it is already held.

This is conceptually clean and requires no additional infrastructure for teams already running on a cloud provider. The path-based key maps naturally to the metadata hierarchy, and the implementation would be straightforward.

**Why we are not choosing this:**

Object storage has no TTL on individual objects (or maybe some providers introduced them lately as I write this), Gravitino would have to build its own expiry and stale lock recovery mechanism from scratch.

Introduces a hard dependency on a cloud provider, with I/O calls traversing beyond the local network of Gravitino cluster.
#### Option 4 : Implement Consensus-Based Coordination (Paxos / Raft)

Implement a consensus protocol directly within the Gravitino server cluster. Nodes elect a leader or use a quorum-based protocol to coordinate lock grants without any external dependency. This is the approach taken by systems like CockroachDB, etcd, and Kafka's internal coordinator.

**Why we are not choosing this:**

Easier said than done. Consensus protocols are extraordinarily difficult to implement correctly. Paxos and Raft have subtle edge cases around leader election, log compaction, split-brain detection etc. Introducing a homegrown consensus implementation into Gravitino's locking path would create a system that is harder to reason about, harder to test, and almost certainly less correct than any of the other options.

This option exists for the sake of completeness.
## Implementation Details

Details below apply to **Option 1** (distributed lock table) only.

### Claim primitive and JDBC dialects

Use an atomic **claim** as the acquisition primitive. PostgreSQL: `INSERT ... ON CONFLICT DO NOTHING` (`putIfAbsent`). Other backends: equivalent idempotent insert/upsert (e.g. MySQL `INSERT IGNORE` / `ON DUPLICATE KEY`), or a short transaction using `SELECT ... FOR UPDATE` plus insert, per dialect.

**Caveat:** claim logic cannot be one portable SQL string. Conflict detection, row-count / returning semantics, and unique-key behavior differ by engine, so this layer must branch per supported backend (same idea as existing dialect-specific `*_meta` providers), or fall back to a heavier transactional pattern that is safe on every target.

**Pseudocode**

```text
interface DistributedLockDialect {
  // Returns true if this session became the holder for scopePath.
  boolean tryClaim(SqlSession session, String scopePath, HolderId holder,
                   Instant now, Duration leaseTtl);

  // Optional: renew lease if still holder.
  boolean heartbeat(SqlSession session, String scopePath, HolderId holder, Instant now, Duration leaseTtl);
}

JdbcDistributedLockStore.acquire(scopePath):
  dialect = dialectFor(JDBCBackendType.fromUrl(jdbcUrl))
  begin txn
    if hierarchicalConflictExists(scopePath): rollback; return ABSENT   // see "Hierarchical conflict"
    ok = dialect.tryClaim(session, scopePath, holderId, now, ttl)
  commit or rollback
  return ok ? HELD : CONTENDED
```

**New / changed types (conceptual)**

- `DistributedLockDialect` : one implementation per supported JDBC backend (e.g. `PostgreSqlLockDialect`, `MySqlLockDialect`, `ForUpdateFallbackDialect`).
- `DistributedLockStore` / `JdbcDistributedLockStore` :  orchestrates txn boundary + dialect + hierarchy check.
- `HolderId` : stable per process (UUID in config or generated at startup) + optional member id for logging.
- Config keys, e.g. `gravitino.lock.distributed.enabled`, lease/heartbeat intervals (similar to existing `Configs` style).

**Existing code touchpoints**

- New DDL under `scripts/{postgresql,mysql,h2}/` for `gravitino_distributed_locks`.
- New MyBatis mappers + provider pattern parallel to `mapper/provider/postgresql/*` for lock SQL.
- `SQLExceptionConverterFactory` / JDBC backend type detection already exists

### Lock row and lease

Each structural write acquires a row keyed by scope path (e.g., `/metalake/catalog/db`) before mutating metadata. Columns include at least: `path` (unique), `holder_id`, `expires_at`, and a **monotonic `fence_token`**. The holder renews `expires_at` on a heartbeat (e.g. every 10s, TTL ~30s). 

Steal/recovery uses conditional `UPDATE`/`DELETE` only when the row is expired or the same `holder_id` releases.

**Pseudocode**

```text
CREATE TABLE gravitino_distributed_locks (
  path TEXT PRIMARY KEY,
  holder_id TEXT NOT NULL,
  fence_token BIGINT NOT NULL,
  expires_at BIGINT NOT NULL  -- epoch millis
);

onAcquireSuccess():
  schedulePeriodic(heartbeatEvery=10s):
    UPDATE gravitino_distributed_locks
      SET expires_at = now + ttl
      WHERE path = :scope AND holder_id = :me AND expires_at > now   -- still valid

onRelease():
  DELETE FROM gravitino_distributed_locks
    WHERE path = :scope AND holder_id = :me
```

**New / changed types**

- `DistributedLockRow` PO + mapper.
- `DistributedLockHeartbeat` : `ScheduledExecutorService` daemon (similar to `LockManager`'s cleaners), registered when distributed locks enabled.

**Existing code touchpoints**

- No change to `TreeLockNode`; heartbeat is separate from in-process `LockManager` timers unless you consolidate metrics later.

### Fencing

After lease expiry, another node may legitimately take the lock and bump `fence_token`. Any metadata commit done under the distributed lock must record the current fence. That prevents a slow former holder from applying stale multi-row updates after its lease is gone, closing the failure mode that **TTL-only** leases leave open without fencing.

**Pseudocode**

```text
-- At steal after expiry:
UPDATE gravitino_distributed_locks
  SET holder_id = :newHolder, fence_token = fence_token + 1, expires_at = :newExpiry
  WHERE path = :scope AND expires_at < :now

-- Every mutating statement touching subtree under lock carries :expectedFence (value read when lock acquired):
UPDATE table_meta SET ..., last_committed_fence = :expectedFence
  WHERE table_id = :id AND last_committed_fence < :expectedFence
-- if rowCount == 0 → abort: lost fence / stale writer
```


**New / changed types**

- `FenceToken` / `DistributedLockHandle`:  `long fence`, `String scope`, returned from `DistributedLockStore.acquire` and threaded through the operation.
- Column(s) `last_committed_fence` (or per-metalake generation) on affected `*_meta` tables, **or** a sidecar table mapping `(entity_type, id) → last_fence`.

**Existing code touchpoints**

- All **structural** mutators in `*MetaService` / mappers used during rename/drop/cascade: extend UPDATEs with fence predicate.
- Dispatchers pass `DistributedLockHandle` into `store.put` / batch paths (API may use a `ThreadLocal` or explicit parameter object to avoid signature explosion — design choice).

### Hierarchical conflict detection

Two scopes conflict if one path is a **prefix** of the other (parent/ancestor vs child/descendant). Before inserting a claim, the session must assert no **active** row exists whose `path` equals, strictly prefixes, or is strictly prefixed by the requested scope, e.g. via indexed prefix queries (`path = ?`, `path LIKE ? || '/%'`, and the symmetric checks), all in the same transaction as the insert so two acquirers cannot both pass the scan. Indexes (e.g. `text_pattern_ops` on PostgreSQL) matter so prefix checks stay cheap.

**Pseudocode**

```text
-- "Active" = not expired: expires_at > now (use DB clock or app clock consistently)

SELECT 1 FROM gravitino_distributed_locks
 WHERE expires_at > :now
   AND (
         path = :scope
      OR path LIKE :scope || '/%'           -- descendant held
      OR :scope LIKE path || '/%'           -- ancestor held
       )
LIMIT 1;

IF found → abort claim
ELSE → run dialect.tryClaim(...)
```


**New / changed types**

- `LockPathCodec.encode(NameIdentifier scope)` : shared with tests asserting parent/child boundaries.

**Existing code touchpoints**

- `DistributedLockStore.acquire` calls mapper `existsConflictingLock(path, now)` inside the same SQL txn as insert.

### Reads vs writes (relational store)

- **Structural writes:** take the distributed lock (plus existing `TreeLock` in-process during rollout) for the chosen scope, perform store updates, then release.
- **Hierarchical reads:** take no distributed lock, but **must** run the full resolve-ancestors-and-load sequence inside **one** JDBC transaction at **`REPEATABLE READ`** (or stricter if needed) so all statements share one snapshot, eliminating torn views inside Gravitino's DB. 

That implies refactoring today's store access so those reads do not open/close a new session per mapper hop.

**Pseudocode**

```text
// Structural write (HA on)
doWithDistributedLock(scope, () ->
  TreeLockUtils.doWithTreeLock(identForLocalExclusion, WRITE, () ->
    withFence(handle, () ->
      store.runStructuralMutation(..., handle.fence())
    )
  )
)

// Hierarchical read (relational only)
SessionUtils.withRepeatableReadTransaction(() -> {
  MetalakeEntity m = metalakeService.getInTxn(ident);
  CatalogEntity c = catalogService.getInTxn(...);
  ...
  return tableService.getInTxn(...);
});
```

**New / changed types**

- `SessionUtils.withRepeatableReadTransaction(Runnable/Supplier)` - opens session, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` (dialect-specific), executes nested mapper work, single commit/rollback.
- `*MetaService` variants or internal methods `*InTxn(SqlSession, ...)` that do not call `commitAndCloseSqlSession` per hop.
- `DistributedLockIntegration` facade used by dispatchers: `void runStructural(NameIdentifier lockLeaf, LockType treeLock, Runnable op)`.

**Existing code touchpoints**

- **`TreeLockUtils.doWithTreeLock`** : unchanged signature
- **`TableOperationDispatcher`**, **`SchemaOperationDispatcher`**, etc. : replace multi-hop `store.get` sequences on read paths with txn-scoped reads
- **`SessionUtils`**, **`SqlSessions`** : extend nesting so RR txn can span multiple mapper calls without intermediate close.

### Catalog backends (Hive, Iceberg, …)

Anything that talks to an external catalog outside that DB transaction is not covered by RR: we can still see skew between Gravitino store state and engine state under races. Option 1 fixes cross-node coordination and snapshot consistency for the metadata DB; end-to-end semantics with the engine may need explicit product guarantees (weaker consistency, or stricter ordering rules) beyond this table.

**Pseudocode**

```text
loadTable(ident):
  Table tEngine = catalogOps.loadFromEngine(ident)   // outside DB txn

  TableEntity tStore = SessionUtils.withRR(() ->
      store.get(ident, TABLE, TableEntity.class)
  )

  mergePolicy(tEngine, tStore)  // document: e.g. engine wins for columns, store wins for Gravitino-only fields
```

**New / changed types**

- Optional `LoadConsistencyMode` in config per catalog type (document-only for v1; enum e.g. `STORE_SNAPSHOT_THEN_ENGINE`, `BEST_EFFORT`).

**Existing code touchpoints**

- **`TableOperationDispatcher.internalLoadTable`** and peers : explicitly order “engine fetch” vs “RR store read”; avoid assuming a single atomic view across both without documenting it.
- No change to external catalog connectors; scope is API/contract documentation + tests that simulate interleaving.

## Rollout Plan

- **Feature flag:** e.g. `gravitino.lock.distributed.enabled` (default off in the first release that ships the code). Structural path + RR-read path consult the flag; TreeLock always remains for in-process ordering when the flag is on or off.

- **Rollback without redeploy:** turning the flag off reverts to **TreeLock-only** cross-node behavior. 

- **Operational note:** with flag **on**, nodes must agree on behavior: a long-lived mix (some on, some off) restores the original HA race for structural ops

- **Observability:** metrics/log line per op: `distributed_lock=on|off`, acquire latency, steal/conflict count, fence rejections.

