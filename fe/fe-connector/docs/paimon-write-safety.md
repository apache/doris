# Paimon snapshot publication protection

A Paimon write creates data files before publishing a snapshot. Concurrent writers
must publish snapshots without overwriting an existing snapshot version. Otherwise,
a write can return success and subsequently disappear from the latest snapshot.
This also applies to independent compaction jobs and writers outside Doris.

Doris checks the resolved table's commit mechanism during write planning, before
creating the BE sink. The check covers INSERT, INSERT OVERWRITE, UPDATE, DELETE,
and MERGE. Reading, catalog creation, and metadata replay do not require write
protection.

## Default admission

| Actual publication mechanism | Default behavior |
| --- | --- |
| Catalog environment with a catalog loader and snapshot version management | Allow; the catalog server must implement the atomic snapshot commit contract. |
| `hdfs://` table with the standard Paimon HadoopFileIO | Allow; HDFS publishes through atomic rename. |
| Enabled Paimon Hive catalog lock | Allow; all writers must use the same shared metastore lock. |
| Paimon JDBC lease lock without an independent supported mechanism | Reject; expiration does not fence the previous owner. |
| Filesystem catalog on S3/S3A, local files, or an unrecognized implementation without a supported lock | Reject. |
| OSS without another supported mechanism | Reject; the scheme alone cannot establish conditional-create semantics and bucket versioning prerequisites. |

The configured catalog type or `paimon.lock.enabled=true` alone is insufficient.
A filesystem catalog can have that setting without providing a lock factory. A
REST catalog must actually advertise snapshot version management. A `viewfs://`
mount is not assumed to have HDFS semantics.

These checks concern snapshot publication. They do not remove Paimon's other
restrictions on concurrent compaction, overwrite, bucket modes, or row changes.
All writers must use compatible publication and locking configurations. Doris
cannot inspect the configuration of a Spark or Flink job.

## Existing single-writer deployments

The catalog property `paimon.allow-unsafe-writes` defaults to `false`. An operator
can explicitly allow a table's unprotected writes after arranging that all
writers to that table run sequentially:

```sql
ALTER CATALOG paimon_catalog SET PROPERTIES (
    'paimon.allow-unsafe-writes' = 'true'
);
```

The property is also accepted by CREATE CATALOG. It affects subsequent planned
writes without restarting FE. Set it back to `false` to restore default admission;
already planned or running writes are not cancelled.

This opt-in does not create a lock or serialize queries. A Doris session, another
Doris cluster, Spark, Flink, and a standalone compaction job all count as writers.
Do not enable the property to make concurrent writes safe. Doris logs a warning
for each planned write admitted through the opt-in, including the table and the
unsupported mechanism.

Before upgrading, inventory catalogs that write through a filesystem or JDBC
backend. Configure a supported publication mechanism, or explicitly opt in for
externally serialized workloads. During a rolling upgrade, older FEs can still
plan writes without this check; complete the FE upgrade before relying on the
new default.

## Validation and follow-up

`PaimonWriteSafetyTest` covers admission and all five DML operations before sink
creation. `PaimonPublicationContractTest` contains two SDK contract probes:

- Two independent JVMs both observe an absent local snapshot. They publish in a
  controlled order through the real SDK rename implementation. The test checks
  that the second publication can hide an already acknowledged row.
- A JDBC lease is explicitly aged, then acquired by another writer. The first
  writer resumes and releases the second writer's lock, allowing a third writer
  to acquire it while the second writer is still inside its callback.

The boundary regression suite checks rejection, enabling the opt-in, disabling it
again, and continued reads. Existing isolated filesystem write suites explicitly
opt in. The JDBC regression's bounded concurrency stress cases deliberately
bypass admission to exercise the SDK; they do not qualify JDBC lease locks as a
supported default mechanism. The expiry probe remains separate.

These are added tests, not a claim that they have passed on every platform. Run
them on the Fedora build/test host before relying on their results. HDFS, HMS,
REST, and mixed-engine behavior also need their respective integration environments.

S3 conditional snapshot creation (`If-None-Match: *`), JDBC ownership/fencing, and
lock metrics are follow-up work. This change does not replace Paimon's snapshot
commit implementation or provide FE-failover recovery for external transactions.

References: [Paimon concurrency control](https://paimon.apache.org/docs/2.1/concepts/concurrency-control/),
[SDK catalog environment](https://github.com/apache/paimon/blob/release-2.1.0/paimon-core/src/main/java/org/apache/paimon/table/CatalogEnvironment.java),
[SDK JDBC lock](https://github.com/apache/paimon/blob/release-2.1.0/paimon-core/src/main/java/org/apache/paimon/jdbc/JdbcCatalogLock.java).
