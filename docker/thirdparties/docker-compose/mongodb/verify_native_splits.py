#!/usr/bin/env python3
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

"""Qualify frozen native range plans against disposable snapshot fixtures and an exact-T source oracle."""

import argparse
import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json
import os
from pathlib import Path
import re
import secrets
import threading
import time
from urllib.request import ProxyHandler, Request, build_opener
import uuid

from bson import Binary, Decimal128, Int64, ObjectId, Timestamp
import pymongo
from pymongo.write_concern import WriteConcern

import verify_native_snapshot as snapshot_checks

DATABASE = "doris_mongodb_splits"
ROWS = 601
APPLICATION = "doris-native-splits"
POOL_ROWS = 32769
COLLECTIONS = ["integer_records", "date_records", "oid_records", "duplicate_records", "empty_records",
               "decimal_records", "string_records", "double_records", "no_index_records", "transaction_records"]


def field(name, target, tags, path=None):
    return dict(name=name, path=path or [name], doris_type=target, bson_types=tags)


def schema():
    columns = [field("seq", "BIGINT", ["long"]), field("epoch", "BIGINT", ["long"])]
    for precision in range(7):
        columns.append(field("time_p" + str(precision), "DATETIME(" + str(precision) + ")", ["date"], ["observed"]))
    for precision, scale in [(1, 0), (9, 0), (9, 9), (18, 0), (18, 6), (18, 18), (38, 0), (38, 4), (38, 38)]:
        columns.append(field("decimal_p" + str(precision) + "s" + str(scale),
                             f"DECIMAL({precision},{scale})", ["decimal"], ["amount"]))
    manifest = {DATABASE + "." + name: {"columns": columns}
                for name in COLLECTIONS + ["record_view", "measurements"]}
    for name in ["compound_wildcard_records", "wildcard_only_records"]:
        manifest[DATABASE + "." + name] = {"columns": [field("seq", "BIGINT", ["long"])]}
    manifest[DATABASE + ".pool_records"] = {"columns": [field("seq", "BIGINT", ["long"])]}
    manifest[DATABASE + ".predicate_records"] = {"columns": [
        field("seq", "BIGINT", ["long"]), field("text", "STRING", ["string"])]}
    return json.dumps(manifest)


def initialize(source, shards=None):
    source.drop_database(DATABASE)
    database = source[DATABASE]
    integer_keys = [Int64(-(2 ** 63)), Int64(2 ** 63 - 1), Int64(2 ** 53 + 1), -1, 0, 1,
                    None, [1, 2], "5", Decimal128("5.000000000000000000000000000000001"),
                    float("nan"), float("inf"), float("-inf"), True, Binary(b"split"), {"key": 1}]
    dates = [datetime(1969, 12, 31, 23, 59, 59, 999000, timezone.utc),
             datetime(1970, 1, 1, tzinfo=timezone.utc), datetime(2024, 3, 10, 9, 59, 59, 999000, timezone.utc),
             datetime(2024, 3, 10, 10, 0, 0, 123000, timezone.utc),
             datetime(2024, 11, 3, 8, 59, 59, 999000, timezone.utc),
             datetime(2024, 11, 3, 9, 0, 0, 1_000, timezone.utc)]
    amounts = ["0", "1", "-1", "0.1234567891234567891234567891234567", "9999999999999999999999999999999999",
               "999999999999999999999999999999.9999", "1E-38", "1E-6176", "1E+6144"]
    if shards:
        source.admin.command("enableSharding", DATABASE, primaryShard=shards[0])
    for name in COLLECTIONS:
        database.create_collection(name)
        if name == "empty_records":
            database[name].create_index("split_key")
            continue
        records = []
        for index in range(ROWS):
            document = dict(_id=Int64(index), seq=Int64(index), epoch=Int64(0),
                            observed=dates[index % len(dates)], amount=Decimal128(amounts[index % len(amounts)]))
            if name == "integer_records":
                if index % 19:
                    document["split_key"] = integer_keys[index % len(integer_keys)] if index % 5 == 0 else index % 23
            elif name == "date_records":
                if index % 17:
                    document["split_key"] = dates[index % len(dates)] if index % 5 else integer_keys[index % len(integer_keys)]
            elif name == "oid_records":
                document["_id"] = ObjectId(index.to_bytes(12, "big")) if index % 5 else "key-" + str(index)
            elif name == "duplicate_records":
                document["split_key"] = Int64(7)
            elif name == "decimal_records":
                document["split_key"] = Decimal128(amounts[index % len(amounts)])
            elif name == "string_records":
                document["split_key"] = "key-" + str(index)
            elif name == "double_records":
                document["split_key"] = index + 0.5
            else:
                document["split_key"] = Int64(index % 23)
            records.append(document)
        database[name].with_options(write_concern=WriteConcern("majority")).insert_many(records)
        if name != "no_index_records":
            database[name].create_index("split_key")
        if shards and name in ["integer_records", "date_records", "transaction_records"]:
            namespace = DATABASE + "." + name
            source.admin.command("shardCollection", namespace, key={"_id": 1})
            source.admin.command("split", namespace, middle={"_id": Int64(300)})
            source.admin.command("moveChunk", namespace, find={"_id": Int64(ROWS - 1)}, to=shards[1],
                                 _waitForDelete=True)
    for name in ["compound_wildcard_records", "wildcard_only_records"]:
        database[name].with_options(write_concern=WriteConcern("majority")).insert_many([
            {"_id": Int64(index), "seq": Int64(index), "split_key": Int64(index), "payload": {"value": index}}
            for index in range(ROWS)])
        # The invalid sampling hint must appear before the ordinary B-tree in listIndexes.
        database[name].create_index([("split_key", 1), ("payload.$**", 1)], name="compound_wildcard")
        if name == "compound_wildcard_records":
            database[name].create_index([("split_key", 1), ("suffix", 1)], name="regular_split")
    database.pool_records.insert_many([{"_id": Int64(index), "seq": Int64(index)}
                                       for index in range(POOL_ROWS)])
    database.predicate_records.insert_many([
        {"_id": Int64(1), "seq": Int64(1), "text": "v" * 60000},
        {"_id": Int64(2), "seq": Int64(2), "text": "v" * 60000},
        {"_id": Int64(3), "seq": Int64(3), "text": "other"}])
    database.command("create", "record_view", viewOn="integer_records", pipeline=[])
    database.create_collection("measurements", timeseries={"timeField": "observed"})
    database.measurements.insert_many([dict(seq=Int64(index), epoch=Int64(0), observed=dates[index % len(dates)],
                                            amount=Decimal128("1")) for index in range(20)])


def install_reader(source, sharded):
    user = "split_reader"
    password = secrets.token_hex(16)
    if any(value["user"] == user for value in source.admin.command("usersInfo", 1)["users"]):
        source.admin.command("dropUser", user)
    if not source.admin.command("rolesInfo", {"role": "split_metadata", "db": "admin"})["roles"]:
        privilege = ({"resource": {"db": "config", "collection": "version"}, "actions": ["find"]} if sharded
                     else {"resource": {"cluster": True}, "actions": ["replSetGetConfig"]})
        source.admin.command("createRole", "split_metadata", privileges=[privilege], roles=[])
    roles = [{"role": "read", "db": DATABASE}, {"role": "split_metadata", "db": "admin"}]
    source.admin.command("createUser", user, pwd=password, roles=roles)
    return user, password


def create_catalog(name, uri, pool=8, policy="null"):
    properties = {"type": "mongodb", "mongodb.uri": uri, "mongodb.databases": DATABASE,
                  "mongodb.schema.mode": "explicit", "mongodb.schema": schema(),
                  "mongodb.type_mismatch": policy, "mongodb.pool_max_clients": str(pool),
                  "mongodb.socket_timeout_ms": "15000"}
    snapshot_checks.sql("DROP CATALOG IF EXISTS " + name)
    snapshot_checks.sql("CREATE CATALOG " + name + " PROPERTIES (" + ",".join(
        snapshot_checks.sql_quote(key) + "=" + snapshot_checks.sql_quote(value) for key, value in properties.items()) + ")")


def query(catalog, name, strategy="range", split_field="split_key", count=8, columns="*", condition="", connection=None):
    hints = "mongodb_split_strategy='" + strategy + "',file_split_size=1,max_file_split_num=" + str(count)
    hints += ",mongodb_split_field='" + split_field + "'"
    return snapshot_checks.sql("SELECT /*+ SET_VAR(" + hints + ") */ " + columns + " FROM " + catalog + "."
                               + DATABASE + "." + name + " " + condition + " ORDER BY seq", connection)


def profile(token):
    base = "http://" + os.environ["DORIS_FE_HTTP_ADDRESS"] + "/rest/v1/query_profile"
    credentials = os.environ["DORIS_FE_USER"] + ":" + os.environ.get("DORIS_FE_PASSWORD", "")
    headers = {"Authorization": "Basic " + base64.b64encode(credentials.encode()).decode()}
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        def get(url):
            with build_opener(ProxyHandler({})).open(Request(url, headers=headers), timeout=5) as response:
                return json.load(response)["data"]
        for item in get(base)["rows"]:
            if token in item["Sql Statement"] and item.get("Profile Completion State") == "COMPLETE":
                text = get(base + "/" + item["Profile ID"]).replace("&nbsp;", " ")
                pins = re.findall(r"MongoSnapshotTimestamp:\s*(\d+):(\d+)", text)
                if pins:
                    assert len(set(pins)) == 1, "Range partitions used different snapshots"
                    assert "MongoSplitActual" in text and "MongoSplitReason" in text
                    return Timestamp(*map(int, pins[0])), text
        time.sleep(0.1)
    raise AssertionError("Completed native range profile was not published")


def source_at(source, name, timestamp):
    documents = []
    with source.start_session(causal_consistency=False) as session:
        reply = source[DATABASE].command({"find": name, "filter": {}, "batchSize": 64,
                                         "readConcern": {"level": "snapshot", "atClusterTime": timestamp}},
                                        session=session)
        try:
            documents.extend(reply["cursor"]["firstBatch"])
            while reply["cursor"]["id"]:
                reply = source[DATABASE].command({"getMore": reply["cursor"]["id"], "collection": name,
                                                 "batchSize": 64}, session=session)
                documents.extend(reply["cursor"]["nextBatch"])
        finally:
            if reply["cursor"]["id"]:
                source[DATABASE].command({"killCursors": name, "cursors": [reply["cursor"]["id"]]}, session=session)
    return documents


def coverage_commands(source, catalog):
    database = source[DATABASE]
    database.command("profile", 0)
    database["system.profile"].drop()
    database.command("profile", 2)
    token = "mongodb_range_coverage_" + uuid.uuid4().hex
    try:
        statement = ("SELECT /*+ SET_VAR(mongodb_split_strategy='range',file_split_size=1,max_file_split_num=8,"
                     "mongodb_split_field='split_key',batch_size=16,enable_profile=true,profile_level=2,"
                         "use_consistent_hash_for_external_scan=true,external_scan_consistent_hash_spread_num=0) */ "
                     "/* " + token + " */ seq FROM " + catalog + "." + DATABASE + ".integer_records ORDER BY seq")
        assert snapshot_checks.sql(statement) == tuple((index,) for index in range(ROWS))
        timestamp, _ = profile(token)
        commands = {}
        sampled_at_pin = False
        for entry in database["system.profile"].find({"appName": APPLICATION}):
            command = entry.get("originatingCommand", entry.get("command", {}))
            partition = command.get("filter", {})
            if command.get("find") == "integer_records" and "value" in command.get("projection", {}):
                assert command["readConcern"]["atClusterTime"] == timestamp
                assert command.get("singleBatch") is True, "A snapshot sample must leave no remote cursor"
                sampled_at_pin = True
            if command.get("find") == "integer_records" and partition:
                key = repr(partition)
                commands[key] = command
        assert sampled_at_pin, "FE sampling did not use the native partitions' pinned timestamp"
        assert len(commands) == 8, "The source did not observe all eight unique frozen partition predicates"
        occurrences = Counter()
        for command in commands.values():
            assert command["readConcern"]["atClusterTime"] == timestamp
            assert "split_key" not in command.get("projection", {}), "An unused split key need not cross the wire"
            with source.start_session(causal_consistency=False) as session:
                reply = database.command({"find": "integer_records", "filter": command["filter"],
                                          "readConcern": {"level": "snapshot", "atClusterTime": timestamp},
                                          "batchSize": 1024}, session=session)
                assert reply["cursor"]["id"] == 0
                occurrences.update(document["seq"] for document in reply["cursor"]["firstBatch"])
        assert occurrences == Counter(range(ROWS)), "A source document matched zero or multiple partitions"
    finally:
        database.command("profile", 0)
    print("PASS: source commands use one T and every source document matches exactly one frozen partition", flush=True)


def matrix(catalog):
    def compare_table(item):
        name, split_field = item
        comparisons = 0
        with snapshot_checks.connect() as connection:
            for zone in ["UTC", "+08:00", "America/Los_Angeles", "Asia/Kathmandu"]:
                snapshot_checks.sql("SET time_zone=" + snapshot_checks.sql_quote(zone), connection)
                baseline = query(catalog, name, "single", connection=connection)
                for count in [1, 2, 3, 8, 32]:
                    for pushdown in [False, True]:
                        snapshot_checks.sql("SET enable_mongodb_predicate_pushdown=" + str(pushdown).lower(), connection)
                        snapshot_checks.sql("SET enable_mongodb_projection_pushdown=" + str(pushdown).lower(), connection)
                        assert baseline == query(catalog, name, split_field=split_field, count=count,
                                                 connection=connection), (name, zone, count, pushdown)
                        comparisons += 1
                for condition in ["WHERE seq > 0 AND seq < 300", "WHERE time_p3 >= '2024-03-10 01:59:59'",
                                  "WHERE decimal_p38s4 >= 1", "WHERE CAST(seq AS STRING) LIKE '%7%'"]:
                    baseline_filter = query(catalog, name, "single", condition=condition, connection=connection)
                    assert baseline_filter == query(catalog, name, split_field=split_field, condition=condition,
                                                    connection=connection)
                    comparisons += 1
        return comparisons

    # These fixtures are immutable until every comparison joins. Independent SQL sessions also
    # exercise bounded shared-pool contention without overlapping failpoints or source mutation.
    with ThreadPoolExecutor(max_workers=5) as executor:
        comparisons = sum(executor.map(compare_table, [
            ("integer_records", "split_key"), ("date_records", "split_key"), ("oid_records", "_id"),
            ("duplicate_records", "split_key"), ("empty_records", "split_key")]))
    print(f"PASS: {comparisons} complete row-multiset comparisons including DATETIME(0..6), Decimal precision/scale, "
          "timezones, missing/null/arrays/other families, duplicate boundaries and residual filters", flush=True)


def precision_oracle(source, catalog):
    specifications = [(1, 0), (9, 0), (9, 9), (18, 0), (18, 6), (18, 18), (38, 0), (38, 4), (38, 38)]
    cells = 0
    with snapshot_checks.connect() as connection:
        for zone in ["UTC", "+08:00", "America/Los_Angeles", "Asia/Kathmandu"]:
            snapshot_checks.sql("SET time_zone=" + snapshot_checks.sql_quote(zone), connection)
            token = "mongodb_range_precision_" + uuid.uuid4().hex
            statement = ("SELECT /*+ SET_VAR(mongodb_split_strategy='range',file_split_size=1,max_file_split_num=8,"
                         "mongodb_split_field='split_key',enable_profile=true,profile_level=2) */ /* " + token
                         + " */ * FROM " + catalog + "." + DATABASE + ".integer_records ORDER BY seq")
            observed = snapshot_checks.sql(statement, connection)
            timestamp, _ = profile(token)
            expected = []
            for document in sorted(source_at(source, "integer_records", timestamp), key=lambda value: value["seq"]):
                row = [document["seq"], document["epoch"]]
                date = document["observed"].replace(tzinfo=None)
                for scale in range(7):
                    # BSON Date is a UTC millisecond value. Lower SQL precision must be exact;
                    # the native mapping reports NULL rather than rounding a nonzero remainder.
                    factor = 10 ** (6 - scale)
                    row.append(date if date.microsecond % factor == 0 else None)
                amount = document["amount"].to_decimal()
                with localcontext() as context:
                    context.prec = 80
                    for precision, scale in specifications:
                        exact = (amount.is_finite() and abs(amount) < Decimal(10) ** (precision - scale)
                                 and amount.scaleb(scale) == amount.scaleb(scale).to_integral_value())
                        row.append(amount if exact else None)
                expected.append(tuple(row))
            normalized = []
            for row in observed:
                values = list(row)
                for index in range(2, 9):
                    if isinstance(values[index], str):
                        values[index] = datetime.fromisoformat(values[index])
                normalized.append(tuple(values))
            assert tuple(normalized) == tuple(expected), "Range precision cells disagree with the independent BSON oracle"
            cells += len(expected) * len(expected[0])
    print(f"PASS: {cells} exact BSON-source precision cells across four timezones, independent of single-read results", flush=True)


def rejection_cases(catalog):
    for name in ["decimal_records", "string_records", "double_records", "no_index_records"]:
        try:
            query(catalog, name, columns="seq")
            raise AssertionError("An unsupported range request succeeded: " + name)
        except Exception as error:
            assert "range planning unavailable" in str(error).lower(), str(error)
        assert query(catalog, name, "single") == query(catalog, name, "auto")
    for name in ["record_view", "measurements"]:
        try:
            query(catalog, name, columns="seq")
            raise AssertionError("Unqualified object range request succeeded")
        except Exception as error:
            assert "standard views" in str(error).lower() or "unsupported_object" in str(error).lower()
    print("PASS: explicit unqualified modes reject; auto keeps declared single-cursor paths", flush=True)


def concurrent_oracle(source, catalog, iterations, require_cross_backend=False, other_catalog=None):
    stopped = threading.Event()
    failures = []

    def writer():
        epoch = 0
        try:
            while not stopped.is_set():
                epoch += 1
                with source.start_session() as session:
                    with session.start_transaction(write_concern=WriteConcern("majority")):
                        source[DATABASE].transaction_records.update_many({}, [{"$set": {"epoch": Int64(epoch),
                            "split_key": {"$mod": [{"$add": ["$seq", Int64(epoch)]}, 23]}}}], session=session)
        except Exception as error:
            failures.append(error)

    thread = threading.Thread(target=writer)
    thread.start()
    crossed_backends = False
    try:
        for iteration in range(iterations):
            token = "mongodb_ranges_" + uuid.uuid4().hex
            joined = iteration % 2 == 1
            hints = ("mongodb_split_strategy='range',file_split_size=1,max_file_split_num=8,batch_size=16,"
                     "enable_profile=true,profile_level=2,use_consistent_hash_for_external_scan=true,"
                     "external_scan_consistent_hash_spread_num=0")
            if joined:
                statement = ("SELECT /*+ SET_VAR(" + hints + ") */ /* " + token + " */ l.seq,l.epoch,r.epoch FROM "
                             + catalog + "." + DATABASE + ".transaction_records l JOIN "
                             + (other_catalog or catalog) + "." + DATABASE
                             + ".transaction_records r ON l.seq=r.seq ORDER BY l.seq")
            else:
                statement = ("SELECT /*+ SET_VAR(" + hints + ",mongodb_split_field='split_key') */ /* " + token
                             + " */ seq,epoch FROM " + catalog + "." + DATABASE + ".transaction_records ORDER BY seq")
            observed = snapshot_checks.sql(statement)
            timestamp, text = profile(token)
            location = None
            locations = set()
            for line in text.replace("</br>", "\n").splitlines():
                host = re.search(r"hostname:([^,]+),\s+port:(\d+)", line)
                if host:
                    location = host.groups()
                if "MongoSnapshotTimestamp" in line and location:
                    locations.add(location)
            crossed_backends |= len(locations) >= 2
            expected = tuple(sorted((document["seq"], document["epoch"], document["epoch"]) if joined
                                    else (document["seq"], document["epoch"])
                                    for document in source_at(source, "transaction_records", timestamp)))
            assert observed == expected, "Parallel read disagreed with the independent single-cursor exact-T oracle"
            assert len(observed) == ROWS and len({row[1] for row in observed}) == 1
    finally:
        stopped.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), "Transaction writer did not stop"
    assert not failures, failures
    if require_cross_backend:
        assert crossed_backends, "No range plan actually executed native partitions on multiple BEs"
    print(f"PASS: {iterations} exact-T parallel multiset comparisons during atomic split-key updates", flush=True)


def cursors_closed(sources):
    for source in sources:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            count = len(list(source.admin.aggregate([
                {"$currentOp": {"allUsers": True, "idleCursors": True}},
                {"$match": {"ns": {"$regex": "^" + DATABASE + r"\."}}}])))
            if count == 0:
                break
            time.sleep(0.1)
        else:
            raise AssertionError("A controlled native range cursor remains open: " + str(count))


def range_fault_cases(source, catalog):
    # The bounded FE sample uses singleBatch, so every getMore belongs to a native partition.
    statement = ("SELECT /*+ SET_VAR(mongodb_split_strategy='range',file_split_size=1,max_file_split_num=8,"
                 "batch_size=8) */ sum(seq) FROM " + catalog + "."
                 + DATABASE + ".integer_records")
    expected = ((ROWS * (ROWS - 1) // 2,),)
    database = source[DATABASE]
    for code in [2, 239]:
        database.command("profile", 0)
        database["system.profile"].drop()
        database.command("profile", 2)
        snapshot_checks.failpoint(source, {"failCommands": ["getMore"], "errorCode": code,
                                            "appName": APPLICATION})
        try:
            snapshot_checks.expect_error(statement, ("code=" + str(code),))
        finally:
            snapshot_checks.off(source)
            database.command("profile", 0)
        full_reads = [entry for entry in database["system.profile"].find({"appName": APPLICATION})
                      if entry.get("command", {}).get("find") == "integer_records"
                      and not entry["command"].get("filter")]
        assert len(full_reads) == 2, "A failed published range plan reselected T or performed a full rescan"
        cursors_closed([source])
        assert snapshot_checks.sql(statement) == expected
    print("PASS: published range getMore command/history failures abort without full rescan and recover cleanly", flush=True)

    for cancel in [True, False]:
        entered = snapshot_checks.failpoint(source, {"failCommands": ["getMore"], "blockConnection": True,
                                                     "blockTimeMS": 4000, "appName": APPLICATION})
        try:
            with snapshot_checks.connect() as connection, ThreadPoolExecutor(max_workers=1) as executor:
                connection_id = snapshot_checks.sql("SELECT connection_id()", connection)[0][0]
                query = statement if cancel else statement.replace("batch_size=8", "batch_size=8,query_timeout=2")
                future = executor.submit(snapshot_checks.expect_error, query, ("cancel", "timeout", "mongodb"), connection)
                snapshot_checks.wait_entered(source, entered)
                if cancel:
                    snapshot_checks.sql("KILL QUERY " + str(connection_id))
                future.result(timeout=20)
        finally:
            snapshot_checks.off(source)
        cursors_closed([source])
        assert snapshot_checks.sql(statement) == expected
    print("PASS: observed native range cancellation/deadline releases cursors and pool leases", flush=True)

    entered = snapshot_checks.failpoint(source, {"failCommands": ["find"], "blockConnection": True,
                                                 "blockTimeMS": 4000, "appName": APPLICATION}, {"skip": 2})
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            query = statement.replace("sum(seq)", "seq,epoch") + " ORDER BY seq"
            future = executor.submit(snapshot_checks.sql, query)
            snapshot_checks.wait_entered(source, entered)
            with source.start_session() as session:
                with session.start_transaction(write_concern=WriteConcern("majority")):
                    database.integer_records.update_many({}, {"$set": {"epoch": Int64(1), "split_key": Int64(-999)}},
                                                         session=session)
            observed = future.result(timeout=25)
            assert observed == tuple((index, 0) for index in range(ROWS))
    finally:
        snapshot_checks.off(source)
    cursors_closed([source])
    print("PASS: split-key changes committed after plan publication remain invisible to every native partition", flush=True)

    previous = source.admin.command("getParameter", 1, minSnapshotHistoryWindowInSeconds=1)[
        "minSnapshotHistoryWindowInSeconds"]
    source.admin.command("setParameter", 1, minSnapshotHistoryWindowInSeconds=0)
    entered = snapshot_checks.failpoint(source, {"failCommands": ["find"], "blockConnection": True,
                                                 "blockTimeMS": 12000, "appName": APPLICATION}, {"skip": 2})
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(snapshot_checks.expect_error, statement, ("code=239",))
            snapshot_checks.wait_entered(source, entered)
            # Advance the source's oldest available timestamp after the entire range plan is frozen.
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                database.history_clock.with_options(write_concern=WriteConcern("majority")).insert_one(
                    {"clock": time.monotonic_ns()})
                source.admin.command("fsync")
                time.sleep(0.15)
            future.result(timeout=20)
    finally:
        snapshot_checks.off(source)
        source.admin.command("setParameter", 1, minSnapshotHistoryWindowInSeconds=previous)
    cursors_closed([source])
    assert snapshot_checks.sql(statement) == expected
    print("PASS: genuine history expiration after range publication returns SnapshotTooOld without reselection", flush=True)


def planning_regressions(source, catalog):
    namespace = catalog + "." + DATABASE
    literal = snapshot_checks.sql_quote("v" * 60000)
    for streaming in [False, True]:
        statement = ("SELECT /*+ SET_VAR(mongodb_split_strategy='range',file_split_size=1,"
                     "max_file_split_num=8,enable_external_table_batch_mode=" + str(streaming).lower()
                     + ") */ count(*),sum(seq) FROM " + namespace + ".predicate_records WHERE text=" + literal)
        assert snapshot_checks.sql(statement) == ((2, 3),)
    print("PASS: normal large predicates are admitted after early properties in sync and streaming plans", flush=True)

    # Cancel the single-batch sample after timestamp selection. The synchronous request has no
    # registered ConnectorScanContext, so it must observe the query task-group lifetime too.
    for streaming in [False, True]:
        database = source[DATABASE]
        database.command("profile", 0)
        database["system.profile"].drop()
        database.command("profile", 2)
        # Stay below the probe's lifetime deadline so this isolates query cancellation
        # from a socket timeout while the artificial server delay is active.
        entered = snapshot_checks.failpoint(source, {"failCommands": ["find"], "blockConnection": True,
                                                     "blockTimeMS": 400, "appName": APPLICATION}, {"skip": 1})
        try:
            with snapshot_checks.connect() as connection, ThreadPoolExecutor(max_workers=1) as executor:
                connection_id = snapshot_checks.sql("SELECT connection_id()", connection)[0][0]
                statement = ("SELECT /*+ SET_VAR(mongodb_split_strategy='range',file_split_size=1,"
                             "max_file_split_num=8,enable_external_table_batch_mode=" + str(streaming).lower()
                             + ") */ sum(seq) FROM " + namespace + ".integer_records")
                future = executor.submit(snapshot_checks.expect_error, statement,
                                         ("cancel", "killed", "stopped", "interrupted", "task groups are closed"), connection)
                snapshot_checks.wait_entered(source, entered)
                snapshot_checks.sql("KILL QUERY " + str(connection_id))
                future.result(timeout=15)
            # Streaming cancellation can return before its producer finishes the blocked command.
            # Wait for that command's profile record before asserting its wire/cleanup contract.
            deadline = time.monotonic() + 10
            sample_filter = {"appName": APPLICATION, "command.find": "integer_records",
                             "command.projection.value": {"$exists": True}}
            while not database["system.profile"].find_one(sample_filter):
                assert time.monotonic() < deadline, "The cancelled sample did not finish"
                time.sleep(0.05)
        finally:
            snapshot_checks.off(source)
            database.command("profile", 0)
        sampled_getmores = [entry for entry in database["system.profile"].find({"appName": APPLICATION})
                           if entry.get("command", {}).get("getMore")
                           and entry.get("originatingCommand", {}).get("projection", {}).get("value")]
        samples = list(database["system.profile"].find(sample_filter))
        assert len(samples) == 1 and samples[0]["command"].get("singleBatch") is True
        assert not sampled_getmores, "The cancelled FE probe advanced to another sample batch"
        cursors_closed([source])
    print("PASS: KILL during the FE sample stops sync/streaming probes without getMore or retained cursors", flush=True)

    branches = " UNION ALL ".join("SELECT count(*) n FROM " + namespace + ".empty_records" for _ in range(257))
    statement = ("SELECT /*+ SET_VAR(mongodb_split_strategy='range',file_split_size=1,max_file_split_num=8,"
                 "enable_external_scan_task_reuse=false) */ sum(n) FROM (" + branches + ") u")
    snapshot_checks.expect_error(statement, ("statement/source split budget exhausted",))
    cursors_closed([source])
    print("PASS: 257 empty fallback scans cannot bypass the pinned-source partition budget", flush=True)


def worker_pool_case(uri, catalog):
    create_catalog(catalog, uri, pool=1)
    group = catalog + "_workers"
    snapshot_checks.sql("DROP WORKLOAD GROUP IF EXISTS " + group)
    snapshot_checks.sql("CREATE WORKLOAD GROUP " + group + " PROPERTIES "
                        "('min_remote_scan_thread_num'='1','max_remote_scan_thread_num'='1')")
    expected = ((POOL_ROWS, POOL_ROWS * (POOL_ROWS - 1) // 2),)
    with snapshot_checks.connect() as connection:
        snapshot_checks.sql("SET workload_group=" + snapshot_checks.sql_quote(group), connection)
        for streaming in [False, True]:
            token = "mongodb_pool_worker_" + uuid.uuid4().hex
            # More than one output batch per partition is essential: otherwise a reader can
            # reach EOF and return its lease before the sole worker switches to its sibling.
            statement = ("SELECT /*+ SET_VAR(mongodb_split_strategy='range',file_split_size=1,"
                         "max_file_split_num=8,batch_size=8,max_file_scanners_concurrency=2,"
                         "min_file_scanners_concurrency=2,enable_adaptive_scan=false,"
                         "parallel_pipeline_task_num=1,query_timeout=30,enable_profile=true,profile_level=2,"
                         "enable_external_table_batch_mode=" + str(streaming).lower()
                         + ") */ /* " + token + " */ count(*),sum(seq) FROM " + catalog + "." + DATABASE + ".pool_records")
            assert snapshot_checks.sql(statement, connection) == expected
            _, text = profile(token)
            counts = re.findall(r"NumScanners:\s*(\d+)", text)
            assert counts and max(map(int, counts)) >= 2, "The fixture did not run competing file scanners"
    print("PASS: a one-worker group and one-client pool finish multi-batch range scans in both planning modes", flush=True)


def pool_case(source, uri, catalog):
    create_catalog(catalog, uri, pool=1)
    statement = "SELECT /*+ SET_VAR(batch_size=16) */ sum(seq) FROM " + catalog + "." + DATABASE + ".integer_records"
    assert snapshot_checks.sql(statement) == ((ROWS * (ROWS - 1) // 2,),)
    entered = snapshot_checks.failpoint(source, {"failCommands": ["getMore"], "blockConnection": True,
                                                "blockTimeMS": 6000, "appName": APPLICATION})
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(snapshot_checks.sql, statement)
            snapshot_checks.wait_entered(source, entered)
            snapshot_checks.expect_error(statement.replace("batch_size=16", "batch_size=16,query_timeout=2"),
                                         ("timeout", "mongodb"))
            assert future.result(timeout=20) == ((ROWS * (ROWS - 1) // 2,),)
    finally:
        snapshot_checks.off(source)
    assert snapshot_checks.sql(statement) == ((ROWS * (ROWS - 1) // 2,),)
    cursors_closed([source])
    print("PASS: query remaining time below the default 10000 ms transport caps cannot bypass pool_max_clients=1", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture-state", required=True)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--catalog-prefix", default="mongodb_splits",
                        help="SQL identifier prefix; use distinct prefixes for independent fixture runs")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--oracle-only", action="store_true", help="skip per-BE fault/pool contention cases")
    parser.add_argument("--require-cross-backend", action="store_true",
                        help="require actual native partitions on multiple BEs in each deployment oracle")
    parser.add_argument("--allow-failpoints", action="store_true", required=True,
                        help="the fixture must be disposable and otherwise idle")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.catalog_prefix):
        parser.error("--catalog-prefix must be an unquoted SQL identifier")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    path = Path(args.fixture_state)
    state = json.loads(path.read_text())
    sources = [pymongo.MongoClient(uri, appname="split-fixture-controller", retryReads=False)
               for uri in [state["replica_uri"], state["router_uris"][0]]]
    try:
        readers = []
        for index, source in enumerate(sources):
            initialize(source, state["shard_names"] if index else None)
            user, password = install_reader(source, bool(index))
            uri = snapshot_checks.query_uri(state["replica_uri"] if index == 0 else state["router_uris"][0],
                                            user, password)
            readers.append(uri.replace("appName=" + snapshot_checks.APPLICATION, "appName=" + APPLICATION))
        alternate = snapshot_checks.query_uri(state["router_uris"][1], user, password)
        readers.append(alternate.replace("appName=" + snapshot_checks.APPLICATION, "appName=" + APPLICATION))
        state["split_reader_uris"] = readers
        path.write_text(json.dumps(state, indent=2))
        if args.prepare_only:
            print("Prepared deterministic mixed-type range fixtures and read-only identities", flush=True)
            return
        for index, source in enumerate(sources):
            catalog = args.catalog_prefix + ("_rs" if index == 0 else "_router")
            create_catalog(catalog, readers[index])
            alternate_catalog = catalog
            if index == 1:
                alternate_catalog = catalog + "_alt"
                create_catalog(alternate_catalog, readers[2])
            if index == 0:
                coverage_commands(source, catalog)
            matrix(catalog)
            rejection_cases(catalog)
            precision_oracle(source, catalog)
            concurrent_oracle(source, catalog, args.iterations, args.require_cross_backend, alternate_catalog)
        if not args.oracle_only:
            planning_regressions(sources[0], args.catalog_prefix + "_rs")
            worker_pool_case(readers[0], args.catalog_prefix + "_worker")
            range_fault_cases(sources[0], args.catalog_prefix + "_rs")
            pool_case(sources[0], readers[0], args.catalog_prefix + "_pool")
        cursors_closed(sources)
        print("PASS: native split qualification completed", flush=True)
    finally:
        for source in sources:
            snapshot_checks.off(source)
            source.close()


if __name__ == "__main__":
    main()
