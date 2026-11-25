# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Spark references for test_deltalake_time_as_of on the shared local filesystem.

Invoked by uc-managed.py --time-travel-edges NEW_DIR. The fixture creates a unique UC
managed table and fresh filesystem tables. Only its own short-retention table is vacuumed.
"""

import json
import os
import shutil
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

from pyspark.sql import functions as F


def actions(log, version):
    return [json.loads(line) for line in (log / f"{version:020d}.json").read_text().splitlines()]


def history(spark, table):
    entries = spark.sql(f"DESCRIBE HISTORY {table}").select("version", F.unix_millis("timestamp").alias("ms"))
    return {r.version: r.ms for r in entries.collect()}


def sql_quote(value):
    return "'" + value.replace("'", "''") + "'"


@contextmanager
def spark_zone(spark, zone):
    # Align the local Spark driver's clock and the SQL session when comparing unqualified literals.
    tz = spark._jvm.java.util.TimeZone
    previous_driver = tz.getDefault()
    previous_session = spark.conf.get("spark.sql.session.timeZone")
    try:
        tz.setDefault(tz.getTimeZone(zone))
        spark.conf.set("spark.sql.session.timeZone", zone)
        yield
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_session)
        tz.setDefault(previous_driver)


def read_frame(spark, sql):
    frame = spark.sql(sql)
    return {"schema": frame.dtypes, "rows": [list(row) for row in frame.collect()]}


def spark_error(spark, sql, *fragments):
    try:
        spark.sql(sql).collect()
    except Exception as error:
        message = str(error)
        assert all(text.lower() in message.lower() for text in fragments), (sql, message)
        return
    raise AssertionError(f"Spark unexpectedly succeeded: {sql}")


def pin(kind, value):
    if kind == "version":
        return f"VERSION AS OF {value}"
    if kind == "millis":
        return f"TIMESTAMP AS OF timestamp_millis({value})"
    assert kind == "literal", kind
    return f"TIMESTAMP AS OF {sql_quote(value)}"


def snapshot_case(
    spark, cases, source, table, name, version, kind, value, zone="UTC", *, nested=False, reference=None
):
    projection = "id, payload.renamed AS val" if nested else "*"
    with spark_zone(spark, zone):
        # Explicitly cast local literals in Spark SQL so the SQL session supplies the time zone.
        at = f"TIMESTAMP AS OF CAST({sql_quote(value)} AS TIMESTAMP)" if kind == "literal" else pin(kind, value)
        actual = read_frame(spark, f"SELECT {projection} FROM {table} {at} ORDER BY id")
        if source == "dst" and kind == "literal":
            raw = read_frame(spark, f"SELECT {projection} FROM {table} {pin(kind, value)} ORDER BY id")
            assert raw == actual, (name, raw, actual)
        if kind != "version" and reference is None:
            reference = read_frame(spark, f"SELECT {projection} FROM {table} VERSION AS OF {version} ORDER BY id")
        if reference is not None:
            assert actual == reference, (name, actual, reference)
    cases.append(
        {
            "source": source,
            "name": name,
            "version": version,
            "kind": kind,
            "value": value,
            "zone": zone,
            "nested": nested,
            **actual,
        }
    )
    return actual


def time_matrix(spark, cases, source, table, older, newer, older_ms, newer_ms):
    assert newer_ms > older_ms + 1, (older_ms, newer_ms)
    by_version = {
        v: read_frame(spark, f"SELECT * FROM {table} VERSION AS OF {v} ORDER BY id") for v in (older, newer)
    }
    for label, version, ms in (
        ("first", older, older_ms),
        ("between", older, (older_ms + newer_ms) // 2),
        ("before_next", older, newer_ms - 1),
        ("next", newer, newer_ms),
    ):
        moment = datetime.fromtimestamp(ms / 1000, timezone.utc)
        utc = moment.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        shanghai = (moment + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        for variant, kind, literal, zone in (
            ("epoch", "millis", ms, "UTC"),
            ("iso", "literal", utc.replace(" ", "T") + "Z", "UTC"),
            ("utc", "literal", utc, "UTC"),
            ("shanghai", "literal", shanghai, "Asia/Shanghai"),
            ("explicit_east", "literal", shanghai.replace(" ", "T") + "+08:00", "UTC"),
        ):
            snapshot_case(
                spark,
                cases,
                source,
                table,
                label + "_" + variant,
                version,
                kind,
                literal,
                zone,
                reference=by_version[version],
            )


def rename_managed_column(spark, table_name):
    # The OSS Spark UC connector rejects ALTER TABLE RENAME COLUMN. Use Kernel's public schema
    # update transaction with the UC committer; reusing the field metadata preserves its column ID.
    origin = os.environ["UC_TEST_SERVER"].rstrip("/")
    with urlopen(f"{origin}/api/2.1/unity-catalog/tables/doris_delta.m1.{table_name}", timeout=15) as response:
        registered = json.load(response)
    assert registered["table_type"] == "MANAGED" and registered["data_source_format"] == "DELTA"
    jvm = spark._jvm
    kernel = jvm.io.delta.kernel
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    options = jvm.java.util.HashMap()
    for key, value in (("uri", origin), ("auth.type", "static"), ("auth.token", "local-test")):
        options.put(key, value)
    supplier = jvm.com.google.common.base.Suppliers.ofInstance(conf)
    rest = jvm.io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient(options, supplier)
    try:
        client = kernel.unitycatalog.UCCatalogManagedClient(rest)
        ident = kernel.unitycatalog.UCTableIdentifier("doris_delta", "m1", table_name)
        empty = jvm.java.util.Optional.empty()
        engine = kernel.defaults.engine.DefaultEngine.create(conf)
        snapshot = client.loadSnapshot(
            engine, registered["table_id"], registered["storage_location"], ident, empty, empty
        )
        assert snapshot.getVersion() == 3
        schema = snapshot.getSchema()
        field = schema.get("label")
        renamed = (
            kernel.types.StructType()
            .add(schema.get("id"))
            .add("renamed_label", field.getDataType(), field.isNullable(), field.getMetadata())
        )
        txn = snapshot.buildUpdateTableTransaction("doris-uc-regression", kernel.Operation.MANUAL_UPDATE)
        result = (
            txn.withUpdatedSchema(renamed)
            .build(engine)
            .commit(engine, kernel.utils.CloseableIterable.emptyIterable())
        )
        assert result.getVersion() == 4
        # Kernel lets the caller run post-commit publication separately. Leave this commit staged.
        after = client.loadSnapshot(
            engine, registered["table_id"], registered["storage_location"], ident, empty, empty
        )
        assert after.getVersion() == 4
        new_meta = after.getSchema().get("renamed_label").getMetadata()
        old_meta = field.getMetadata()
        assert new_meta.getLong("delta.columnMapping.id") == old_meta.getLong("delta.columnMapping.id")
        assert new_meta.getString("delta.columnMapping.physicalName") == old_meta.getString(
            "delta.columnMapping.physicalName"
        )
        return old_meta.getLong("delta.columnMapping.id")
    finally:
        rest.close()


def managed_schema(spark, manifest, hold, table_name):
    table = f"doris_delta.m1.{table_name}"
    spark.sql(f"""CREATE TABLE {table} (id BIGINT, label STRING) USING DELTA
        TBLPROPERTIES ('delta.columnMapping.mode'='name')""")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'before'), (2, 'removed')")
    spark.sql(f"DELETE FROM {table} WHERE id = 2")
    spark.sql(f"INSERT INTO {table} VALUES (3, 'after')")
    hold(table_name, [3], wait_for_writer=True)
    field_id = rename_managed_column(spark, table_name)
    log = hold(table_name, [3, 4], wait_for_writer=False)
    api = os.environ["UC_TEST_SERVER"].rstrip("/") + "/api/2.1/unity-catalog"
    with urlopen(f"{api}/delta/v1/catalogs/doris_delta/schemas/m1/tables/{table_name}", timeout=15) as response:
        state = json.load(response)
    assert state["latest-table-version"] == 4
    commits = {c["version"]: c for c in state["commits"]}
    assert {3, 4} <= commits.keys()
    for version in (3, 4):
        assert (log / "_staged_commits" / commits[version]["file-name"]).is_file()
        assert not (log / f"{version:020d}.json").exists() and not (log / f"{version:020d}.crc").exists()
    assert not (log / "_last_checkpoint").exists()
    t3, t4 = (commits[v]["timestamp"] for v in (3, 4))
    time_matrix(spark, manifest["cases"], "uc", table, 3, 4, t3, t4)
    old = snapshot_case(spark, manifest["cases"], "uc", table, "old_version", 3, "version", 3)
    new = snapshot_case(spark, manifest["cases"], "uc", table, "new_version", 4, "version", 4)
    assert old["schema"] == [("id", "bigint"), ("label", "string")]
    assert new["schema"] == [("id", "bigint"), ("renamed_label", "string")]
    spark_error(spark, f"SELECT renamed_label FROM {table} VERSION AS OF 3", "UNRESOLVED_COLUMN")
    spark_error(spark, f"SELECT label FROM {table} VERSION AS OF 4", "UNRESOLVED_COLUMN")
    mid = (t3 + t4) // 2
    mixed = {}
    for tag, old, new in (
        ("time_old", pin("millis", mid), pin("version", 4)),
        ("time_new", pin("version", 3), pin("millis", t4)),
    ):
        query = (
            f"SELECT o.id,o.label,n.renamed_label FROM {table} {old} o "
            f"JOIN {table} {new} n ON n.id=o.id ORDER BY o.id"
        )
        mixed[tag] = read_frame(spark, query)["rows"]
        assert mixed[tag] == [[1, "before", "before"], [3, "after", "after"]], mixed[tag]
    manifest["uc"] = {
        "table": table_name,
        "mid": mid,
        "new_time": t4,
        "mixed": mixed,
        "field_id": field_id,
        "staged_files": {str(v): commits[v]["file-name"] for v in (3, 4)},
    }


def commit_time_transition(spark, manifest, path, table):
    spark.sql(f"""CREATE TABLE {table}(id BIGINT, label STRING) USING DELTA LOCATION {sql_quote(path.as_uri())}
        TBLPROPERTIES ('delta.enableInCommitTimestamps'='false','delta.enableDeletionVectors'='false')""")
    spark.sql(f"INSERT INTO {table} VALUES(1,'legacy-one')")
    spark.sql(f"INSERT INTO {table} VALUES(2,'legacy-two')")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.enableInCommitTimestamps'='true')")
    spark.sql(f"INSERT INTO {table} VALUES(3,'after-enablement')")
    log = path / "_delta_log"
    infos = {v: next(a["commitInfo"] for a in actions(log, v) if "commitInfo" in a) for v in range(5)}
    assert all(infos[v].get("inCommitTimestamp") is None for v in (0, 1, 2))
    t3, t4 = (infos[v]["inCommitTimestamp"] for v in (3, 4))
    assert t4 > t3 + 1, (t3, t4)
    conf = next(a["metaData"]["configuration"] for a in actions(log, 3) if "metaData" in a)
    assert int(conf["delta.inCommitTimestampEnablementVersion"]) == 3
    assert int(conf["delta.inCommitTimestampEnablementTimestamp"]) == t3
    # Assign distinct filesystem times on this fresh local fixture. Versions 0–2 use these times;
    # files 3–4 appear later than their in-commit timestamps. The data at v2 and v3 is identical.
    mtimes = {0: t3 - 120000, 1: t3 - 90000, 2: t3 - 30000, 3: t4 + 30000, 4: t4 + 60000}
    for v, ms in mtimes.items():
        os.utime(log / f"{v:020d}.json", ns=(ms * 1000000, ms * 1000000))
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    times = history(spark, table)
    assert times == {0: mtimes[0], 1: mtimes[1], 2: mtimes[2], 3: t3, 4: t4}, times
    for name, v, ms in (
        ("legacy_exact", 1, mtimes[1]),
        ("legacy_between", 1, mtimes[2] - 1),
        ("last_legacy", 2, mtimes[2]),
        ("before_switch", 2, t3 - 1),
        ("at_switch", 3, t3),
        ("after_switch", 3, t3 + 1),
        ("before_next", 3, t4 - 1),
        ("after_next", 4, t4),
    ):
        snapshot_case(spark, manifest["cases"], "transition", table, name, v, "millis", ms)
    time_matrix(spark, manifest["cases"], "transition", table, 1, 2, mtimes[1], mtimes[2])
    for ms, error in (
        (mtimes[0] - 1, "DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION"),
        (t4 + 1, "DELTA_TIMESTAMP_GREATER_THAN_COMMIT"),
    ):
        iso = datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="milliseconds")
        with spark_zone(spark, "UTC"):
            for at in (pin("millis", ms), pin("literal", iso)):
                spark_error(spark, f"SELECT * FROM {table} {at}", error)
    manifest["transition"] = {
        "enabled_version": 3,
        "in_commit_ms": {3: t3, 4: t4},
        "file_mtime_ms": mtimes,
        "before_earliest": mtimes[0] - 1,
        "after_latest": t4 + 1,
    }


def dst_inputs(spark, manifest, path, table):
    spark.sql(f"""CREATE TABLE {table}(id BIGINT, label STRING) USING DELTA LOCATION {sql_quote(path.as_uri())}
        TBLPROPERTIES ('delta.enableInCommitTimestamps'='false','delta.enableDeletionVectors'='false',
                       'delta.deletedFileRetentionDuration'='interval 1000 days',
                       'delta.logRetentionDuration'='interval 1000 days')""")
    for v in range(1, 8):
        spark.sql(f"INSERT INTO {table} VALUES({v},'v{v}')")
    # Both US clock transitions from the last complete calendar year are within this fixture's retention.
    year = datetime.now(timezone.utc).year - 1

    def sunday(d):
        return d + timedelta(days=(6 - d.weekday()) % 7)

    spring = sunday(date(year, 3, 8))
    autumn = sunday(date(year, 11, 1))
    utc_times = [
        (spring, "06:00"),
        (spring, "06:55"),
        (spring, "07:15"),
        (spring, "07:45"),
        (autumn, "05:15"),
        (autumn, "05:45"),
        (autumn, "06:15"),
        (autumn, "06:45"),
    ]
    log = path / "_delta_log"
    ms = {}
    for v, (day, clock) in enumerate(utc_times):
        ms[v] = int(datetime.fromisoformat(f"{day}T{clock}:00+00:00").timestamp() * 1000)
        assert next(a["commitInfo"] for a in actions(log, v) if "commitInfo" in a).get("inCommitTimestamp") is None
        os.utime(log / f"{v:020d}.json", ns=(ms[v] * 1000000, ms[v] * 1000000))
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    assert history(spark, table) == ms
    zone = "America/New_York"
    for name, v, literal in (
        ("spring_before", 1, f"{spring} 01:59:59.999"),
        ("spring_gap", 2, f"{spring} 02:30:00"),
        ("spring_shifted", 2, f"{spring} 03:30:00"),
        ("spring_absolute", 2, f"{spring}T07:30:00Z"),
        ("spring_explicit_east", 2, f"{spring}T15:30:00+08:00"),
        ("autumn_fold", 4, f"{autumn} 01:30:00"),
        ("autumn_first_offset", 4, f"{autumn}T01:30:00-04:00"),
    ):
        snapshot_case(spark, manifest["cases"], "dst", table, name, v, "literal", literal, zone)
    # An explicit -05:00 identifies the second occurrence. Delta Spark 4.4.1 can select the
    # first occurrence in a New York session even though Spark SQL parses the correct epoch.
    # Keep both observations; use the UTC instant as the reference for Doris's offset contract.
    literal = f"{autumn}T01:30:00-05:00"
    with spark_zone(spark, zone):
        parsed = spark.sql(f"SELECT unix_millis(CAST({sql_quote(literal)} AS TIMESTAMP))").first()[0]
        ny = read_frame(spark, f"SELECT * FROM {table} {pin('literal', literal)} ORDER BY id")
    assert ms[6] < parsed < ms[7]
    with spark_zone(spark, "UTC"):
        absolute = read_frame(spark, f"SELECT * FROM {table} {pin('millis', parsed)} ORDER BY id")
        assert absolute == read_frame(spark, f"SELECT * FROM {table} VERSION AS OF 6 ORDER BY id")
    manifest["dst"] = {
        "zone": zone,
        "spring": str(spring),
        "autumn": str(autumn),
        "commit_millis": ms,
        "second_occurrence_epoch": parsed,
        "second_occurrence_literal": literal,
        "spark_new_york": ny["rows"],
        "spark_utc": absolute["rows"],
    }


def retention_and_vacuum(spark, manifest, path, table):
    spark.sql(f"""CREATE TABLE {table}(id BIGINT,label STRING) USING DELTA LOCATION {sql_quote(path.as_uri())}
        TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 2 seconds',
                       'delta.logRetentionDuration'='interval 30 days','delta.enableDeletionVectors'='false',
                       'delta.enableInCommitTimestamps'='false')""")
    spark.sql(f"INSERT INTO {table} VALUES(1,'retired')")
    spark.sql(f"INSERT OVERWRITE {table} VALUES(2,'retained')")
    spark.sql(f"INSERT INTO {table} VALUES(3,'latest')")
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    assert Path(unquote(urlparse(detail.location).path)).resolve() == path
    log = path / "_delta_log"
    a1 = next(a["add"] for a in actions(log, 1) if "add" in a)
    a2 = next(a["add"] for a in actions(log, 2) if "add" in a)
    rem = next(a["remove"] for a in actions(log, 2) if "remove" in a and a["remove"]["path"] == a1["path"])
    retired = (path / unquote(a1["path"])).resolve()
    retained = (path / unquote(a2["path"])).resolve()
    assert retired.parent == retained.parent == path and retired.is_file() and retained.is_file()
    assert spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled").lower() == "true"
    age_guard = "spark.databricks.delta.vacuum.enforceTimeTravelWithinDeletedFileRetentionDuration"
    assert spark.conf.get(age_guard).lower() == "true"
    times = history(spark, table)
    # Wait for the table's own two-second data retention to elapse, leaving all log versions in place.
    cutoff = max(rem["deletionTimestamp"], times[3], int(retired.stat().st_mtime * 1000)) + 2100
    remaining = (cutoff - time.time() * 1000) / 1000
    if remaining > 0:
        time.sleep(remaining)
    assert spark.read.parquet(retired.as_uri()).select("id", "label").first().asDict() == {
        "id": 1,
        "label": "retired",
    }
    retained_rows = [list(r) for r in spark.read.parquet(retained.as_uri()).select("id", "label").collect()]
    assert retained_rows == [[2, "retained"]]

    def probe(v, selector):
        return f"SELECT * FROM {table} {pin(selector, v if selector == 'version' else times[v])} ORDER BY id"

    # The age check and physical deletion are different boundaries. Record Spark's default first.
    check = "DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION"
    for v in (1, 2):
        for kind in ("version", "millis"):
            spark_error(spark, probe(v, kind), check)
    spark.conf.set(age_guard, "false")
    try:
        assert read_frame(spark, probe(1, "version"))["rows"] == [[1, "retired"]]
        assert read_frame(spark, probe(2, "version"))["rows"] == [[2, "retained"]]
    finally:
        spark.conf.set(age_guard, "true")
    # Only this invocation's new table is eligible. DRY RUN must identify the obsolete data file.
    candidates = [r[0] for r in spark.sql(f"VACUUM {table} DRY RUN").collect()]
    named = {Path(unquote(urlparse(x).path)).resolve() for x in candidates}
    assert retired in named and retained not in named and all(p.is_relative_to(path) for p in named), candidates
    shutil.copy2(retired, path.parents[2] / "retired-before-vacuum.parquet")
    vacuum_result = [r[0] for r in spark.sql(f"VACUUM {table}").collect()]
    assert not retired.exists() and retained.is_file() and (log / f"{1:020d}.json").is_file()
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    assert read_frame(spark, f"SELECT * FROM {table} ORDER BY id")["rows"] == [[2, "retained"], [3, "latest"]]
    for v in (1, 2):
        for kind in ("version", "millis"):
            spark_error(spark, probe(v, kind), check)
    spark.conf.set(age_guard, "false")
    try:
        for kind in ("version", "millis"):
            assert read_frame(spark, probe(2, kind))["rows"] == retained_rows
            spark_error(spark, probe(1, kind), retired.name, "FILE_NOT_EXIST")
    finally:
        spark.conf.set(age_guard, "true")
    manifest["retention"] = {
        "retention_seconds": 2,
        "v1_ms": times[1],
        "v2_ms": times[2],
        "retired": retired.name,
        "retained": retained.name,
        "retained_rows": retained_rows,
        "dry_run": candidates,
        "vacuum": vacuum_result,
        "spark_default_rejected_old_versions": True,
        "obsolete_file_exists": retired.exists(),
        "v2_file_exists": retained.exists(),
        "latest": read_frame(spark, f"SELECT * FROM {table} ORDER BY id"),
    }


def nested_checkpoint(spark, manifest, path, table):
    original = path.parents[2] / "nested-source"
    src = table + "_source"
    spark.sql(f"""CREATE TABLE {src}(id BIGINT, payload STRUCT<original:BIGINT, flag:STRING>)
        USING DELTA LOCATION {sql_quote(original.as_uri())}
        TBLPROPERTIES ('delta.columnMapping.mode'='name','delta.checkpointInterval'='2')""")
    spark.sql(f"INSERT INTO {src} VALUES(1, named_struct('original', cast(10 as bigint), 'flag', 'a'))")
    spark.sql(f"ALTER TABLE {src} RENAME COLUMN payload.original TO renamed")
    spark.sql(f"INSERT INTO {src} VALUES(2, named_struct('renamed', cast(20 as bigint), 'flag', 'b'))")
    spark.sql(f"ALTER TABLE {src} DROP COLUMN payload.renamed")
    spark.sql(f"ALTER TABLE {src} ADD COLUMNS (payload.renamed STRING)")
    spark.sql(f"INSERT INTO {src} VALUES(3, named_struct('flag', 'c', 'renamed', 'fresh'))")

    # The source stays complete. Only the copy starts at the v2 checkpoint.
    def omit_early_logs(directory, names):
        if Path(directory) != original / "_delta_log":
            return []
        return [name for name in names if name.lstrip(".").startswith((f"{0:020d}.", f"{1:020d}."))]

    shutil.copytree(original, path, ignore=omit_early_logs)
    log = path / "_delta_log"
    assert (log / f"{2:020d}.checkpoint.parquet").is_file() and not (log / f"{1:020d}.json").exists()
    spark.sql(f"CREATE TABLE {table} USING DELTA LOCATION {sql_quote(path.as_uri())}")
    times = history(spark, src)
    for name, version in [("at_checkpoint", 2), ("replay_one_commit", 3), ("readded_field", 6)]:
        snapshot_case(spark, manifest["cases"], "checkpoint", table, name, version, "version", version, nested=True)
    t3 = (times[3] + times[4]) // 2
    snapshot_case(
        spark,
        manifest["cases"],
        "checkpoint",
        table,
        "time_after_checkpoint",
        3,
        "millis",
        t3,
        nested=True,
    )
    assert read_frame(spark, f"SELECT id,payload.renamed FROM {table} VERSION AS OF 6 ORDER BY id")["rows"] == [
        [1, None],
        [2, None],
        [3, "fresh"],
    ]
    mixed = read_frame(
        spark,
        f"""SELECT o.id, o.payload.renamed AS old_val, n.payload.renamed AS new_val
        FROM {table} {pin("millis", t3)} o LEFT JOIN {table} VERSION AS OF 6 n ON n.id=o.id ORDER BY o.id""",
    )
    assert mixed["rows"] == [[1, 10, None], [2, 20, None]]
    manifest["checkpoint"] = {"t3": t3, "mixed": mixed}
    spark_error(spark, f"SELECT id FROM {table} VERSION AS OF 1", "DELTA_VERSION_NOT_FOUND")


def prepare(spark, destination, hold):
    # Creating a fresh fixture root keeps VACUUM and timestamp edits away from existing warehouses.
    root = Path(destination).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=False)
    spark.sparkContext.setLogLevel("WARN")
    warehouse = root / "warehouse" / "edges"
    warehouse.mkdir(parents=True)
    token = uuid.uuid4().hex[:12]
    manifest = {"cases": []}
    managed_schema(spark, manifest, hold, "uc_staged_schema_" + token)
    for name, task in (
        ("transition", commit_time_transition),
        ("dst", dst_inputs),
        ("checkpoint", nested_checkpoint),
        ("retention", retention_and_vacuum),
    ):
        task(spark, manifest, warehouse / name, "spark_catalog.default.m2_" + name + "_" + token)
    result = root / "expected.json"
    result.with_suffix(".tmp").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    result.with_suffix(".tmp").replace(result)
    print(f"Prepared {len(manifest['cases'])} Spark comparisons at {result}")
