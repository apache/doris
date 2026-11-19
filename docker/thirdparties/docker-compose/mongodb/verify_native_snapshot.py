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

"""Validate common snapshots against snapshot_fixture.py's disposable services."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import threading
import time
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import ProxyHandler, Request, build_opener
import uuid

from bson import Decimal128, Int64, Timestamp
import pymongo
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
import pymysql

DATABASE = "doris_mongodb_snapshot"
ROWS = 2081
APPLICATION = "doris-native-snapshot"
CATALOGS = ["mongodb_snapshot_rs", "mongodb_snapshot_router", "mongodb_snapshot_router_alt"]


def sql_quote(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def connect():
    connection = pymysql.connect(host=os.environ["DORIS_FE_HOST"], port=int(os.environ["DORIS_FE_PORT"]),
                                 user=os.environ["DORIS_FE_USER"], password=os.environ.get("DORIS_FE_PASSWORD", ""),
                                 autocommit=True, read_timeout=60)
    with connection.cursor() as cursor:
        for setting in ["batch_size=1024", "enable_profile=true", "query_timeout=30",
                        "mongodb_read_concern='snapshot'"]:
            cursor.execute("SET " + setting)
    return connection


def sql(statement, connection=None):
    if connection is None:
        with connect() as owned:
            return sql(statement, owned)
    with connection.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()


def query_uri(uri, username=None, password=None):
    parsed = urlsplit(uri)
    options = [(key, value) for key, value in parse_qsl(parsed.query) if key.lower() != "appname"]
    options.append(("appName", APPLICATION))
    hosts = parsed.netloc.rsplit("@", 1)[-1]
    credentials = parsed.netloc.rsplit("@", 1)[0] if "@" in parsed.netloc else ""
    if username is not None:
        credentials = quote(username, safe="") + ":" + quote(password, safe="")
    return urlunsplit(parsed._replace(netloc=(credentials + "@" if credentials else "") + hosts,
                                     query=urlencode(options)))


def schema():
    def field(name, target="BIGINT", tags=None):
        result = {"name": name, "path": [name], "doris_type": target, "bson_types": tags or ["long"]}
        return result
    bank = {"columns": [field("seq"), field("amount"), field("epoch")]}
    return json.dumps({DATABASE + "." + name: definition for name, definition in {
        "left_records": bank, "right_records": bank, "ddl_records": bank, "record_view": bank,
        "measurements": {"columns": [field("seq"), field("amount"),
                                      field("observed", "DATETIME(3)", ["date"])]},
        "precision_records": {"columns": [field("seq"), field("observed", "DATETIME(3)", ["date"]),
                                          field("precise_time", "DATETIME(6)", ["date"]),
                                          field("amount", "DECIMAL(38,4)", ["decimal"])]}
    }.items()})


def create_catalog(name, uri, **overrides):
    properties = {"type": "mongodb", "mongodb.uri": query_uri(uri), "mongodb.databases": DATABASE,
                  "mongodb.schema.mode": "explicit", "mongodb.schema": schema(),
                  "mongodb.connect_timeout_ms": "1000", "mongodb.server_selection_timeout_ms": "1000",
                  "mongodb.socket_timeout_ms": "15000", "mongodb.pool_max_clients": "4"}
    properties.update(overrides)
    sql("DROP CATALOG IF EXISTS " + name)
    sql("CREATE CATALOG " + name + " PROPERTIES (" + ",".join(
        sql_quote(key) + "=" + sql_quote(value) for key, value in properties.items()) + ")")


def initialize(source, sharded=False, shards=None):
    database = source[DATABASE]
    source.drop_database(DATABASE)
    if sharded:
        source.admin.command("enableSharding", DATABASE, primaryShard=shards[0])
    for name in ["left_records", "right_records", "ddl_records"]:
        documents = [{"_id": index, "seq": Int64(index), "amount": Int64(0), "epoch": Int64(0)}
                     for index in range(ROWS)]
        documents[0 if name != "right_records" else ROWS - 1]["amount"] = Int64(
            200 if name == "right_records" else 100)
        database[name].with_options(write_concern=WriteConcern("majority")).insert_many(documents)
        if sharded and name != "ddl_records":
            namespace = DATABASE + "." + name
            source.admin.command("shardCollection", namespace, key={"_id": 1})
            source.admin.command("split", namespace, middle={"_id": 1040})
            source.admin.command("moveChunk", namespace, find={"_id": ROWS - 1}, to=shards[1],
                                 _waitForDelete=True)
    database.create_collection("measurements", timeseries={"timeField": "observed", "metaField": "device"})
    database.measurements.insert_many([
        {"observed": datetime(2024, 3, 10, 10, tzinfo=timezone.utc), "device": "fixture",
         "seq": Int64(index), "amount": Int64(index % 7)} for index in range(ROWS)])
    database.create_collection("record_view", viewOn="left_records", pipeline=[])
    database.precision_records.insert_many([
        {"seq": Int64(1), "observed": datetime.fromtimestamp(-0.001, timezone.utc),
         "precise_time": datetime.fromtimestamp(-0.001, timezone.utc),
         "amount": Decimal128("123456789012345678901234567890.1234")},
        {"seq": Int64(2), "observed": datetime(2024, 3, 10, 10, 0, 0, 123000, timezone.utc),
         "precise_time": datetime(2024, 3, 10, 10, 0, 0, 123000, timezone.utc), "amount": Decimal128("-0.0001")}])


def install_reader(source, sharded):
    role = "snapshot_query_identity"
    privileges = ([{"resource": {"db": "config", "collection": "version"}, "actions": ["find"]}]
                  if sharded else [{"resource": {"cluster": True}, "actions": ["replSetGetConfig"]}])
    try:
        source.admin.command("dropUser", "snapshot_reader")
    except pymongo.errors.OperationFailure as error:
        if error.code != 11:
            raise
    try:
        source.admin.command("dropRole", role)
    except pymongo.errors.OperationFailure as error:
        if error.code != 31:
            raise
    source.admin.command("createRole", role, privileges=privileges, roles=[])
    password = uuid.uuid4().hex
    source.admin.command("createUser", "snapshot_reader", pwd=password,
                         roles=[{"role": "read", "db": DATABASE}, {"role": role, "db": "admin"}])
    return password


def bank_query(left, right=None):
    right = right or left
    return ("SELECT 1 AS side,sum(amount),sum(epoch),count(*) FROM " + left + "." + DATABASE + ".left_records "
            "UNION ALL SELECT 2,sum(amount),sum(epoch),count(*) FROM " + right + "." + DATABASE
            + ".right_records ORDER BY side")


def update_bank(source, epoch):
    with source.start_session(causal_consistency=False) as session:
        with session.start_transaction(read_concern=ReadConcern("snapshot"), write_concern=WriteConcern("majority")):
            source[DATABASE].left_records.update_one({"_id": 0}, {"$set": {
                "amount": Int64(50 if epoch % 2 else 100), "epoch": Int64(epoch)}}, session=session)
            source[DATABASE].right_records.update_one({"_id": ROWS - 1}, {"$set": {
                "amount": Int64(250 if epoch % 2 else 200), "epoch": Int64(epoch)}}, session=session)


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
                text = get(base + "/" + item["Profile ID"]).replace("&nbsp;", " ").replace("\u00a0", " ")
                pins = re.findall(r"MongoSnapshotTimestamp:\s*(\d+):(\d+)", text)
                if len(pins) >= 2:
                    assert len(set(pins)) == 1, "Scan nodes used different snapshot timestamps"
                    location = None
                    backends = set()
                    for line in text.replace("</br>", "\n").splitlines():
                        host = re.search(r"hostname:([^,]+),\s+port:(\d+)", line)
                        if host:
                            location = host.groups()
                        if "MongoSnapshotTimestamp" in line and location:
                            backends.add(location)
                    return Timestamp(*map(int, pins[0])), backends
        time.sleep(0.2)
    raise AssertionError("A completed profile with both native snapshot pins was not published")


def source_bank(source, timestamp, aggregate=True):
    results = []
    with source.start_session(causal_consistency=False) as session:
        for side, name in enumerate(["left_records", "right_records"], 1):
            reply = source[DATABASE].command({"find": name, "filter": {}, "batchSize": 500,
                                             "readConcern": {"level": "snapshot", "atClusterTime": timestamp}},
                                            session=session)
            documents = reply["cursor"]["firstBatch"]
            while reply["cursor"]["id"]:
                reply = source[DATABASE].command({"getMore": reply["cursor"]["id"], "collection": name,
                                                 "batchSize": 500}, session=session)
                documents += reply["cursor"]["nextBatch"]
            if aggregate:
                results.append((side, sum(row["amount"] for row in documents),
                                sum(row["epoch"] for row in documents), len(documents)))
            else:
                results.extend((side, row["seq"], row["amount"], row["epoch"])
                               for row in sorted(documents, key=lambda row: row["seq"]))
    return tuple(results)


def concurrent_oracle(source, catalog, alternate, iterations, require_cross_backend=False):
    stopped = threading.Event()
    cross_backend_seen = False
    def writer():
        epoch = 1
        while not stopped.is_set():
            update_bank(source, epoch)
            epoch += 1
            stopped.wait(0.03)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(writer)
        try:
            for iteration in range(iterations):
                token = "snapshot_oracle_" + uuid.uuid4().hex
                aggregate = iteration % 2 == 0
                statement = bank_query(catalog, alternate) if aggregate else (
                    "SELECT 1 AS side,seq,amount,epoch FROM " + catalog + "." + DATABASE + ".left_records "
                    "UNION ALL SELECT 2,seq,amount,epoch FROM " + alternate + "." + DATABASE
                    + ".right_records ORDER BY side,seq,amount,epoch")
                if require_cross_backend:
                    # A stable single-split placement can choose one BE for both collections.
                    # Admit all eligible BEs, then prove actual scanner placement from the Profile.
                    statement = statement.replace("SELECT", "SELECT /*+ SET_VAR("
                                                  "use_consistent_hash_for_external_scan=true,"
                                                  "external_scan_consistent_hash_spread_num=0) */", 1)
                observed = sql("/* " + token + " */ " + statement)
                timestamp, backends = profile(token)
                cross_backend_seen |= len(backends) >= 2
                expected = source_bank(source, timestamp, aggregate)
                assert observed == expected, (observed, expected)
                if aggregate:
                    assert observed[0][1] + observed[1][1] == 300, observed
                    assert observed[0][2] == observed[1][2], observed
        finally:
            stopped.set()
            future.result(timeout=30)
    assert not require_cross_backend or cross_backend_seen, "No query ran MongoDB scans on two actual BEs"
    if require_cross_backend:
        print("PASS: actual MongoDB scans on multiple BE processes share T and the exact source-at-T results", flush=True)
    print("PASS: transactional writer, two native scans/getMore, shared profile T and exact source-at-T oracle", flush=True)


def expect_error(statement, fragments=("mongodb",), connection=None):
    try:
        sql(statement, connection)
    except pymysql.MySQLError as error:
        message = str(error).lower()
        assert any(fragment.lower() in message for fragment in fragments), message
        assert "mongodb://" not in message and "snapshot_reader" not in message, "Sensitive diagnostics"
        return
    raise AssertionError("The rejected or injected snapshot query succeeded")


def off(source):
    source.admin.command({"configureFailPoint": "failCommand", "mode": "off"})


def failpoint(source, data, mode=None):
    return source.admin.command({"configureFailPoint": "failCommand", "mode": mode or {"times": 1},
                                 "data": {"appName": APPLICATION, **data}})["count"]


def wait_entered(source, count):
    source.admin.command({"waitForFailPoint": "failCommand", "timesEntered": count + 1, "maxTimeMS": 15000})


def cursors_closed(sources):
    for source in sources:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            # Replica-set primaries also retain replication cursors; inspect only the controlled database.
            count = len(list(source.admin.aggregate([
                {"$currentOp": {"allUsers": True, "idleCursors": True}},
                {"$match": {"ns": {"$regex": "^" + DATABASE + r"\."}}}])))
            if count == 0:
                break
            time.sleep(0.1)
        else:
            raise AssertionError("A native source cursor remains open: " + str(count))


def ordinary_cases(catalog, other_cluster):
    with connect() as connection:
        baseline = sql("SELECT /*+ SET_VAR(mongodb_read_concern='majority') */ * FROM " + catalog + "."
                       + DATABASE + ".precision_records ORDER BY seq", connection)
        assert baseline[0][3] == Decimal("123456789012345678901234567890.1234"), baseline
        for zone in ["UTC", "+08:00", "America/Los_Angeles", "Asia/Kathmandu"]:
            sql("SET time_zone=" + sql_quote(zone), connection)
            assert baseline == sql("SELECT * FROM " + catalog + "." + DATABASE
                                   + ".precision_records ORDER BY seq", connection)
        for mode in ["primary", "secondary", "secondaryPreferred", "nearest"]:
            sql("SET mongodb_read_preference=" + sql_quote(mode), connection)
            assert sql("SELECT count(*) FROM " + catalog + "." + DATABASE + ".left_records", connection) == ((ROWS,),)
        sql("SET mongodb_read_preference='primary'", connection)
        expect_error("SELECT count(*) FROM " + catalog + "." + DATABASE + ".record_view", ("standard views",), connection)
        expect_error(bank_query(catalog, other_cluster), ("source identity",), connection)
        sql("SET mongodb_read_concern='majority'", connection)
        sql("SELECT /*+ SET_VAR(mongodb_read_concern='snapshot') */ count(*) FROM " + catalog + "."
            + DATABASE + ".left_records", connection)
        assert sql("SELECT @@mongodb_read_concern", connection) == (("majority",),)
    print("PASS: explicit snapshot routing, view/cross-cluster rejection, hint restoration, exact decimal/date/timezones", flush=True)


def same_session_case(source, catalog):
    with connect() as connection:
        update_bank(source, 0)
        before = sql(bank_query(catalog), connection)
        update_bank(source, 1)
        after = sql(bank_query(catalog), connection)
        assert before == ((1, 100, 0, ROWS), (2, 200, 0, ROWS)), before
        assert after == ((1, 50, 1, ROWS), (2, 250, 1, ROWS)), after
    print("PASS: consecutive statements on one connection select fresh snapshots", flush=True)


def permission_case(source, uri, catalog):
    password = uuid.uuid4().hex
    try:
        source.admin.command("dropUser", "snapshot_data_only")
    except pymongo.errors.OperationFailure as error:
        if error.code != 11:
            raise
    source.admin.command("createUser", "snapshot_data_only", pwd=password,
                         roles=[{"role": "read", "db": DATABASE}])
    create_catalog(catalog, query_uri(uri, "snapshot_data_only", password))
    statement = "SELECT count(*) FROM " + catalog + "." + DATABASE + ".left_records"
    assert sql(statement.replace("SELECT", "SELECT /*+ SET_VAR(mongodb_read_concern='majority') */", 1)) == ((ROWS,),)
    expect_error(statement, ("code 13",))
    print("PASS: data-only reader keeps normal reads, missing snapshot identity permission fails closed", flush=True)


def source_command_checks(source):
    rows = list(source[DATABASE].system.profile.find({"appName": APPLICATION}))
    finds = [row["command"] for row in rows if "atClusterTime" in row.get("command", {}).get("readConcern", {})]
    assert finds and all(isinstance(command["readConcern"]["atClusterTime"], Timestamp) for command in finds)
    continuations = [row for row in rows if "getMore" in row.get("command", {})]
    # The capped profiler can evict an earlier find; each getMore retains its own originating command.
    assert continuations and all(row["command"]["lsid"] == row["originatingCommand"]["lsid"]
                                 and isinstance(row["originatingCommand"]["readConcern"]["atClusterTime"], Timestamp)
                                 for row in continuations)
    probes = [row["command"] for row in rows
              if "__doris_snapshot_probe__" in row.get("command", {}).get("projection", {})]
    # The Java driver can request one extra batch slot to exhaust a limit-one cursor.
    assert probes and all(command.get("limit") == 1 and command.get("batchSize") in [1, 2]
                          and command.get("readConcern", {}).get("level") == "snapshot" for command in probes)
    print("PASS: actual source commands carry typed atClusterTime and matching getMore sessions", flush=True)


def history_case(source, catalog):
    statement = "SELECT sum(seq) FROM " + catalog + "." + DATABASE + ".left_records"
    previous = source.admin.command("getParameter", 1, minSnapshotHistoryWindowInSeconds=1)[
        "minSnapshotHistoryWindowInSeconds"]
    source.admin.command("setParameter", 1, minSnapshotHistoryWindowInSeconds=0)
    count = failpoint(source, {"failCommands": ["find"], "blockConnection": True, "blockTimeMS": 12000}, {"skip": 1})
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(expect_error, statement, ("code=239",))
            wait_entered(source, count)
            # Advance the oldest available timestamp before the first native find executes at T.
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                source[DATABASE].history_clock.with_options(write_concern=WriteConcern("majority")).insert_one(
                    {"clock": time.monotonic_ns()})
                source.admin.command("fsync")
                time.sleep(0.15)
            future.result(timeout=20)
    finally:
        off(source)
        source.admin.command("setParameter", 1, minSnapshotHistoryWindowInSeconds=previous)
    cursors_closed([source])
    assert sql(statement) == ((2164240,),)
    print("PASS: genuinely expired server-selected T fails SnapshotTooOld without reselection, next statement recovers", flush=True)


def probe_case(source, catalog):
    statement = "SELECT count(*) FROM " + catalog + "." + DATABASE + ".left_records"
    failpoint(source, {"failCommands": ["find"], "errorCode": 2})
    try:
        expect_error(statement, ("code 2",))
    finally:
        off(source)
    assert sql(statement) == ((ROWS,),)
    count = failpoint(source, {"failCommands": ["find"], "blockConnection": True, "blockTimeMS": 4000})
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(expect_error,
                                 statement.replace("SELECT", "SELECT /*+ SET_VAR(query_timeout=2) */", 1))
        try:
            wait_entered(source, count)
            future.result(timeout=15)
        finally:
            off(source)
    cursors_closed([source])
    assert sql(statement) == ((ROWS,),)
    print("PASS: FE snapshot-probe failure/deadline closes resources, no timestamp fallback and recovery", flush=True)


def fault_cases(source, catalog):
    statement = "SELECT sum(seq) FROM " + catalog + "." + DATABASE + ".left_records"
    for code in [2, 239]:
        failpoint(source, {"failCommands": ["getMore"], "errorCode": code})
        try:
            expect_error(statement)
        finally:
            off(source)
        cursors_closed([source])
        assert sql(statement) == ((2164240,),)
    print("PASS: getMore command/history failures never select a new T, cleanup and recovery", flush=True)

    for cancel in [False, True]:
        count = failpoint(source, {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 4000})
        with connect() as connection, ThreadPoolExecutor(max_workers=1) as executor:
            connection_id = sql("SELECT connection_id()", connection)[0][0]
            query = statement if cancel else statement.replace("SELECT", "SELECT /*+ SET_VAR(query_timeout=2) */", 1)
            future = executor.submit(expect_error, query, ("cancel", "timeout", "mongodb"), connection)
            try:
                wait_entered(source, count)
                if cancel:
                    sql("KILL QUERY " + str(connection_id))
                future.result(timeout=15)
            finally:
                off(source)
        cursors_closed([source])
        assert sql(statement) == ((2164240,),)
    print("PASS: observed getMore cancellation/deadline, cursor/session release and recovery", flush=True)

    # FailCommand's skip mode first permits the FE's one-row snapshot probe.
    count = failpoint(source, {"failCommands": ["find"], "blockConnection": True, "blockTimeMS": 6000}, {"skip": 1})
    update_bank(source, 0)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(sql, bank_query(catalog))
        try:
            wait_entered(source, count)
            update_bank(source, 1)
            observed = future.result(timeout=25)
            assert observed[0][1:3] == (100, 0) and observed[1][1:3] == (200, 0), observed
        finally:
            off(source)
    cursors_closed([source])
    print("PASS: transaction committed after T selection is invisible in both native scans", flush=True)

    count = failpoint(source, {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 4000})
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(expect_error, statement, ("topology changed",))
        try:
            wait_entered(source, count)
            config = source.admin.command("replSetGetConfig")["config"]
            config["version"] += 1
            config["members"][0].setdefault("tags", {})["snapshot_validation"] = "updated"
            source.admin.command("replSetReconfig", config)
            future.result(timeout=20)
        finally:
            off(source)
    assert sql(statement) == ((2164240,),)
    print("PASS: replica-set configuration changes abort the pinned scan, next statement recovers", flush=True)

    ddl_statement = statement.replace("left_records", "ddl_records")
    count = failpoint(source, {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 4000})
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(expect_error, ddl_statement)
        try:
            wait_entered(source, count)
            source[DATABASE].ddl_records.drop()
            source[DATABASE].ddl_records.insert_one({"seq": Int64(7), "amount": Int64(7), "epoch": Int64(0)})
            future.result(timeout=20)
        finally:
            off(source)
    cursors_closed([source])
    sql("REFRESH CATALOG " + catalog)
    assert sql(ddl_statement) == ((7,),)
    print("PASS: DROP/CREATE namespace replacement fails, refresh permits the new identity", flush=True)


def pool_case(source, uri):
    catalog = "mongodb_snapshot_pool"
    create_catalog(catalog, uri, **{"mongodb.pool_max_clients": "1"})
    statement = "SELECT sum(seq) FROM " + catalog + "." + DATABASE + ".left_records"
    assert sql(statement) == ((2164240,),)
    count = failpoint(source, {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 6000})
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(sql, statement)
        try:
            wait_entered(source, count)
            expect_error(statement.replace("SELECT", "SELECT /*+ SET_VAR(query_timeout=2) */", 1), ("timeout", "mongodb"))
            assert future.result(timeout=15) == ((2164240,),)
        finally:
            off(source)
    cursors_closed([source])
    assert sql(statement) == ((2164240,),)
    print("PASS: different remaining query deadlines share the configured one-client native pool", flush=True)


def timeseries_case(source, catalog):
    statement = "SELECT count(*),sum(seq),sum(amount) FROM " + catalog + "." + DATABASE + ".measurements"
    baseline = sql(statement)
    assert baseline == ((ROWS, 2164240, sum(index % 7 for index in range(ROWS))),), baseline
    count = failpoint(source, {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 4000})
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(sql, statement)
        try:
            wait_entered(source, count)
            source[DATABASE].measurements.insert_one({"observed": datetime.now(timezone.utc), "device": "fixture",
                                                      "seq": Int64(9999), "amount": Int64(9999)})
            assert future.result(timeout=15) == baseline
        finally:
            off(source)
    assert sql(statement)[0][0] == ROWS + 1
    count = failpoint(source, {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 4000})
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(expect_error, statement, ("identity changed",))
        try:
            wait_entered(source, count)
            source[DATABASE].command("collMod", "measurements", expireAfterSeconds=315360000)
            future.result(timeout=15)
        finally:
            off(source)
    sql("REFRESH CATALOG " + catalog)
    assert sql(statement)[0][0] == ROWS + 1
    cursors_closed([source])
    print("PASS: logical time-series duplicate timestamps, concurrent insertion, definition-change rejection", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture-state", required=True)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--oracle-only", action="store_true", help="run data/consistency checks without fault injection")
    parser.add_argument("--require-cross-backend", action="store_true",
                        help="require actual native scans on multiple BEs in each deployment oracle")
    parser.add_argument("--allow-failpoints", required=True, action="store_true",
                        help="the fixture must be disposable and otherwise idle")
    args = parser.parse_args()
    path = Path(args.fixture_state)
    state = json.loads(path.read_text())
    sources = [pymongo.MongoClient(uri, appname="snapshot-fixture-controller", retryReads=False)
               for uri in [state["replica_uri"], *state["router_uris"]]]
    shard_clients = [pymongo.MongoClient(uri, appname="snapshot-fixture-controller") for uri in state["shard_uris"]]
    try:
        readers = []
        for index in [0, 1]:
            initialize(sources[index], index == 1, state["shard_names"])
            password = install_reader(sources[index], index == 1)
            readers.append(query_uri(state["replica_uri"] if index == 0 else state["router_uris"][0],
                                     "snapshot_reader", password))
        readers.append(query_uri(state["router_uris"][1], "snapshot_reader", password))
        state["reader_uris"] = readers
        path.write_text(json.dumps(state, indent=2))
        path.chmod(0o600)
        if args.prepare_only:
            print("Prepared deterministic data and read-only query identities", flush=True)
            return
        for catalog, uri in zip(CATALOGS, readers):
            create_catalog(catalog, uri)
        ordinary_cases(CATALOGS[0], CATALOGS[1])
        same_session_case(sources[0], CATALOGS[0])
        for index in [0, 1]:
            permission_case(sources[index], readers[index], "mongodb_snapshot_permissions_" + str(index))
        sources[0][DATABASE].command("profile", 2)
        try:
            for index, alternate in [(0, 0), (1, 2)]:
                concurrent_oracle(sources[index], CATALOGS[index], CATALOGS[alternate], args.iterations,
                                  args.require_cross_backend)
            source_command_checks(sources[0])
        finally:
            sources[0][DATABASE].command("profile", 0)
        if args.oracle_only:
            cursors_closed([sources[0], *shard_clients])
            print("PASS: snapshot oracle checks for " + state["image"], flush=True)
            return
        probe_case(sources[0], CATALOGS[0])
        fault_cases(sources[0], CATALOGS[0])
        history_case(sources[0], CATALOGS[0])
        pool_case(sources[0], readers[0])
        timeseries_case(sources[0], CATALOGS[0])
        for index in [1, 2]:
            statement = "SELECT count(*),sum(seq) FROM " + CATALOGS[index] + "." + DATABASE + ".measurements"
            assert sql(statement) == ((ROWS, 2164240),)
        cursors_closed([sources[0], *shard_clients])
        print("PASS: all common-snapshot integration checks for " + state["image"], flush=True)
    finally:
        for source in [sources[0], *shard_clients]:
            off(source)
        for source in sources + shard_clients:
            source.close()


if __name__ == "__main__":
    main()
