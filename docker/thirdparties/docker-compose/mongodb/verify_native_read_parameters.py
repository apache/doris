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

"""Qualify native read settings on real replica members and replicated mongos shards."""

import argparse
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import secrets
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, unquote
import uuid

import pymongo
from bson import Int64
from pymysql import MySQLError

import verify_native_splits as splits
import verify_native_snapshot as snapshot

DATABASE = splits.DATABASE
PREFERENCES = ["primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"]
CONCERNS = ["local", "majority", "available", "snapshot"]


def uri_options(uri, **settings):
    parsed = urlsplit(uri)
    replaced = {key.lower() for key in settings}
    options = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
               if key.lower() not in replaced]
    options += [(key, str(value)) for key, value in settings.items() if value is not None]
    return urlunsplit(parsed._replace(query=urlencode(options)))


@contextmanager
def connection():
    with snapshot.connect() as result:
        # The snapshot verifier's helper forces snapshot; inheritance tests must undo that.
        snapshot.sql("SET mongodb_read_concern='inherit'", result)
        snapshot.sql("SET mongodb_read_preference='inherit'", result)
        yield result


def sql(statement, current=None):
    if current is not None:
        return snapshot.sql(statement, current)
    with connection() as owned:
        return snapshot.sql(statement, owned)


def hints(settings):
    def value(v):
        return str(v).lower() if isinstance(v, (bool, int)) else snapshot.sql_quote(v)
    return ",".join(key + "=" + value(v) for key, v in settings.items())


def catalog(name, uri, overrides=None):
    properties = {"type": "mongodb", "mongodb.uri": uri, "mongodb.databases": DATABASE,
                  "mongodb.schema.mode": "explicit", "mongodb.schema": splits.schema(),
                  "mongodb.type_mismatch": "null", "mongodb.read_concern": "majority",
                  "mongodb.read_preference": "primary", "mongodb.connect_timeout_ms": "5000",
                  "mongodb.server_selection_timeout_ms": "5000", "mongodb.socket_timeout_ms": "5000",
                  "mongodb.pool_max_clients": "8"}
    properties.update(overrides or {})
    sql("DROP CATALOG IF EXISTS " + name)
    sql("CREATE CATALOG " + name + " PROPERTIES (" + ",".join(
        snapshot.sql_quote(key) + "=" + snapshot.sql_quote(value) for key, value in properties.items()) + ")")


def rejected(statement, fragments):
    with connection() as current:
        snapshot.expect_error(statement, fragments, current)
        assert sql("SELECT @@mongodb_read_preference,@@mongodb_read_concern", current) == (("inherit", "inherit"),)


class Deployment:
    def __init__(self, state, index, prefix):
        self.state, self.index = state, index
        self.label = "replica" if index == 0 else "mongos"
        self.name = prefix + "_" + self.label
        self.reader = state["split_reader_uris"][index]
        self.source = pymongo.MongoClient(state["replica_uri"] if index == 0 else state["router_uris"][0])
        self.members = []
        self.results = []
        groups = [state["replica_ports"]] if index == 0 else state["shard_ports"]
        for group, ports in enumerate(groups):
            assert len(ports) == 3, "Routing proof requires actual secondaries in every replica set/shard"
            for member, port in enumerate(ports):
                client = pymongo.MongoClient("127.0.0.1", port, username=state["username"],
                                             password=state["password"], authSource="admin", directConnection=True,
                                             serverSelectionTimeoutMS=5000)
                previous = client[DATABASE].command("profile", 2)
                self.members.append((group, member, client, previous))
        self.roles()

    def roles(self):
        roles = {}
        for group, member, client, _ in self.members:
            hello = client.admin.command("hello")
            assert hello.get("isWritablePrimary") or hello.get("secondary"), "Fixture member is not ready"
            roles[group, member] = "primary" if hello.get("isWritablePrimary") else "secondary"
        return roles

    def commands(self, application, collection="integer_records"):
        result = []
        for group, member, client, _ in self.members:
            for entry in client[DATABASE]["system.profile"].find({"appName": application,
                                                                "command.find": collection}):
                command = entry["command"]
                # Exclude FE timestamp selection and its fixed-width boundary sample.
                projection = command.get("projection", {}).get("value")
                if command.get("limit") == 1 or isinstance(projection, dict) and "$cond" in projection:
                    continue
                result.append((group, member, command))
        return result

    def run(self, preference="primary", concern="snapshot", strategy="range", streaming=False,
            uri=None, overrides=None, options=None, tag=None, collection="integer_records",
            columns="seq", expected=None, query_where="", source_preference=None):
        application = "doris-read-parameters-" + uuid.uuid4().hex
        selected_uri = uri_options(uri or self.reader, appName=application)
        catalog(self.name, selected_uri, overrides)
        settings = {"mongodb_read_preference": preference, "mongodb_read_concern": concern,
                    "mongodb_split_strategy": strategy, "file_split_size": 1, "max_file_split_num": 8,
                    "batch_size": 16, "enable_external_table_batch_mode": streaming}
        settings.update(options or {})
        before = self.roles()
        statement = ("SELECT /*+ SET_VAR(" + hints(settings) + ") */ " + columns + " FROM "
                     + self.name + "." + DATABASE + "." + collection + " " + query_where + " ORDER BY 1")
        expected = expected if expected is not None else tuple((i,) for i in range(splits.ROWS))
        assert sql(statement) == expected, "Read-parameter result mismatch"
        commands = self.commands(application, collection)
        assert commands, "No real native source find was recorded"
        assert self.roles() == before, "Fixture role changed during the routing assertion"
        effective = source_preference or preference
        for group, member, command in commands:
            role = before[group, member]
            if effective in ["primary", "primaryPreferred"]:
                assert role == "primary", "A primary policy read a secondary"
            elif effective in ["secondary", "secondaryPreferred"]:
                assert role == "secondary", "Eligible secondaries were bypassed"
            if tag is not None:
                assert str(member) == tag, "URI routing criteria were lost during the mode override"
            assert command.get("readConcern", {}).get("level") == concern, "Effective readConcern was lost"
        if concern == "snapshot":
            times = {command["readConcern"]["atClusterTime"] for _, _, command in commands}
            assert len(times) == 1, "Native reads did not share one source timestamp"
        self.results.append({"topology": self.label, "preference": preference, "concern": concern,
                             "strategy": strategy, "streaming": streaming, "native_commands": len(commands)})
        sql("DROP CATALOG " + self.name)

    def matrix(self):
        for streaming in [False, True]:
            for strategy in ["single", "auto", "range"]:
                for concern in CONCERNS:
                    for preference in PREFERENCES:
                        if strategy == "range" and concern != "snapshot":
                            continue
                        self.run(preference, concern, strategy, streaming)
        print("PASS:", self.label, "90 read-policy/split/planning combinations with actual source-node proof", flush=True)

    def routing(self):
        catalog_options = {"mongodb.read_preference": "secondary", "mongodb.read_concern": "snapshot"}
        # Explicit session values above cover all modes. These cases exercise true inheritance.
        self.inherited(self.reader, catalog_options, "secondary", "snapshot")
        overridden = uri_options(self.reader, readPreference="secondary", readConcernLevel="available")
        self.inherited(overridden, {"mongodb.read_preference": "primary", "mongodb.read_concern": "majority"},
                       "secondary", "available")
        roles = self.roles()
        eligible = set.intersection(*[{member for (g, member), role in roles.items()
                                      if g == group and role == "secondary"}
                                     for group in {g for g, _ in roles}])
        assert eligible, "Replicated shards need a common tagged secondary"
        tag = str(min(eligible))
        tagged = uri_options(self.reader, readPreference="secondary", readPreferenceTags="fixture_member:" + tag,
                             maxStalenessSeconds=120)
        for preference in ["secondary", "nearest", "secondaryPreferred"]:
            self.run(preference, uri=tagged, tag=tag)
        catalog(self.name, tagged)
        rejected("SELECT /*+ SET_VAR(mongodb_read_preference='primary') */ seq FROM "
                 + self.name + "." + DATABASE + ".integer_records", ("incompatible with URI",))
        missing = uri_options(self.reader, readPreference="secondary", readPreferenceTags="fixture_member:missing")
        catalog(self.name, missing, {"mongodb.server_selection_timeout_ms": "500"})
        rejected("SELECT seq FROM " + self.name + "." + DATABASE + ".integer_records", ("mongodb",))
        for mechanism in ["SCRAM-SHA-1", "SCRAM-SHA-256"]:
            self.run(uri=uri_options(self.reader, authMechanism=mechanism))
        if self.index == 1:
            parsed = urlsplit(self.reader)
            credentials = parsed.netloc.rsplit("@", 1)[0]
            ports = self.state["router_ports"]
            hosts = ",".join("127.0.0.1:" + str(port) for port in ports)
            both = urlunsplit(parsed._replace(netloc=credentials + "@" + hosts))
            self.run(uri=uri_options(both, directConnection="false"))
            alternate = self.state["router_uris"][1]
            alternate = urlunsplit(urlsplit(alternate)._replace(netloc=credentials + "@"
                                  + urlsplit(alternate).netloc.rsplit("@", 1)[-1]))
            self.run(uri=alternate)
        else:
            primary = next(member for (group, member), role in roles.items() if role == "primary")
            parsed = urlsplit(self.reader)
            credentials = parsed.netloc.rsplit("@", 1)[0]
            direct = urlunsplit(parsed._replace(netloc=credentials + "@127.0.0.1:"
                                                + str(self.state["replica_ports"][primary])))
            self.run(uri=uri_options(direct, directConnection="true", replicaSet=None))
        print("PASS:", self.label, "catalog/URI/session precedence, tag/staleness constraints, seed modes and SCRAM", flush=True)

    def inherited(self, uri, overrides, preference, concern):
        application = "doris-read-parameters-" + uuid.uuid4().hex
        catalog(self.name, uri_options(uri, appName=application), overrides)
        assert sql("SELECT seq FROM " + self.name + "." + DATABASE + ".integer_records ORDER BY seq") == tuple(
            (i,) for i in range(splits.ROWS))
        roles = self.roles()
        commands = self.commands(application)
        assert commands and all(roles[g, m] == preference for g, m, _ in commands)
        assert all(command.get("readConcern", {}).get("level") == concern for _, _, command in commands)
        sql("DROP CATALOG " + self.name)

    def resources(self):
        for batch in [1, 16, 1024, 65535]:
            for streaming in [False, True]:
                self.run(options={"batch_size": batch}, streaming=streaming,
                         query_where="WHERE seq < 16", expected=tuple((i,) for i in range(16)))
        for size in [1, 2, 16, 1024]:
            self.run(overrides={"mongodb.pool_max_clients": str(size)})
        for limit in [1, 2, 32, 0, -1]:
            self.run(options={"max_file_split_num": limit})
        self.run(options={"file_split_size": 2147483647})
        self.run(options={"file_split_size": 0, "max_initial_file_split_size": 1,
                          "max_file_split_size": 2, "max_initial_file_split_num": 1})
        for enabled in [False, True]:
            self.run(options={"enable_mongodb_predicate_pushdown": enabled,
                              "enable_mongodb_like_pushdown": enabled,
                              "enable_mongodb_projection_pushdown": enabled})
        self.run(options={"max_file_scanners_concurrency": 2, "min_file_scanners_concurrency": 1,
                          "parallel_pipeline_task_num": 1})
        transport = uri_options(self.reader, connectTimeoutMS=1, serverSelectionTimeoutMS=1,
                                socketTimeoutMS=1, maxPoolSize=1)
        self.run(uri=transport)
        print("PASS:", self.label, "batch/pool bounds, shared split/scanner limits and URI transport overrides", flush=True)

    def schemas_and_auth(self):
        database = self.source[DATABASE]
        database.drop_collection("parameter_validated")
        database.create_collection("parameter_validated", validator={"$jsonSchema": {
            "bsonType": "object", "properties": {"_id": {"bsonType": "objectId"},
                                                  "seq": {"bsonType": "long"}}}})
        database.parameter_validated.insert_many([{"seq": Int64(i)} for i in range(32)])
        expected = tuple((i,) for i in range(32))
        for mode in ["auto", "validator"]:
            self.run(strategy="single", collection="parameter_validated", expected=expected,
                     overrides={"mongodb.schema.mode": mode, "mongodb.schema": "{}"})
        manifest = {DATABASE + ".parameter_validated": {"columns": [
            splits.field("source_seq", "BIGINT", ["long"], ["seq"])]}}
        for mode in ["auto", "explicit"]:
            self.run(strategy="single", collection="parameter_validated", columns="source_seq", expected=expected,
                     overrides={"mongodb.schema.mode": mode, "mongodb.schema": json.dumps(manifest)})
        self.run(strategy="single", collection="parameter_validated", columns="count(*)", expected=((32,),),
                 overrides={"mongodb.schema.mode": "document", "mongodb.schema": "{}"})
        database.drop_collection("parameter_mismatch")
        database.parameter_mismatch.insert_many([{"seq": Int64(0), "value": Int64(1)},
                                                 {"seq": Int64(1), "value": "bad"}])
        manifest = {DATABASE + ".parameter_mismatch": {"columns": [splits.field("seq", "BIGINT", ["long"]),
                                                                       splits.field("value", "BIGINT", ["long"])]}}
        self.run(strategy="single", collection="parameter_mismatch", columns="seq,value",
                 expected=((0, 1), (1, None)), overrides={"mongodb.schema": json.dumps(manifest)})
        catalog(self.name, self.reader, {"mongodb.schema": json.dumps(manifest), "mongodb.type_mismatch": "error"})
        rejected("SELECT value FROM " + self.name + "." + DATABASE + ".parameter_mismatch", ("bson",))
        for mode in ["validator", "explicit"]:
            catalog(self.name, self.reader, {"mongodb.schema.mode": mode, "mongodb.schema": "{}"})
            rejected("SELECT seq FROM " + self.name + "." + DATABASE + ".integer_records", ("schema", "validator"))

        catalog(self.name, self.reader, {"mongodb.databases": " " + DATABASE + " , parameter_other "})
        databases = {row[0] for row in sql("SHOW DATABASES FROM " + self.name)}
        assert DATABASE in databases and "parameter_other" in databases and "admin" not in databases

        user, password = "parameter_reader", secrets.token_hex(16)
        if database.command("usersInfo", {"user": user, "db": DATABASE})["users"]:
            database.command("dropUser", user)
        database.command("createUser", user, pwd=password, roles=[{"role": "read", "db": DATABASE},
                          {"role": "split_metadata", "db": "admin"}])
        parsed = urlsplit(self.reader)
        bare = urlunsplit(parsed._replace(netloc=parsed.netloc.rsplit("@", 1)[-1]))
        self.run(uri=bare, overrides={"mongodb.username": user, "mongodb.password": password,
                                     "mongodb.auth_database": DATABASE})
        database_uri = snapshot.query_uri(self.reader, user, password)
        self.run(uri=uri_options(database_uri, authSource=DATABASE))
        catalog(self.name, bare, {"mongodb.username": user, "mongodb.password": "wrong-fixture-password",
                                 "mongodb.auth_database": DATABASE})
        rejected("SELECT seq FROM " + self.name + "." + DATABASE + ".integer_records", ("mongodb",))
        sql("ALTER CATALOG " + self.name + " SET PROPERTIES ('mongodb.password'=" + snapshot.sql_quote(password) + ")")
        assert sql("SELECT seq FROM " + self.name + "." + DATABASE + ".integer_records ORDER BY seq") == tuple(
            (i,) for i in range(splits.ROWS))
        print("PASS:", self.label, "all schema modes, mismatch policies, database allowlist and credential sources/recovery", flush=True)

    def transport_faults(self):
        application = "doris-read-parameters-" + uuid.uuid4().hex
        selected = uri_options(self.reader, appName=application)
        statement = ("SELECT /*+ SET_VAR(mongodb_read_concern='majority',mongodb_split_strategy='single',"
                     "batch_size=1,enable_external_table_batch_mode=false,use_consistent_hash_for_external_scan=true,"
                     "external_scan_consistent_hash_spread_num=0) */ seq FROM "
                     + self.name + "." + DATABASE + ".integer_records WHERE seq < 32 ORDER BY seq")
        expected = tuple((i,) for i in range(32))
        # The URI's short socket cap must lose to the explicit Catalog cap on both FE and BE.
        catalog(self.name, uri_options(selected, socketTimeoutMS=1), {"mongodb.socket_timeout_ms": "5000"})
        assert sql(statement) == expected
        entered = snapshot.failpoint(self.source, {"failCommands": ["getMore"], "blockConnection": True,
                                                   "blockTimeMS": 300, "appName": application})
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(sql, statement)
                snapshot.wait_entered(self.source, entered)
                snapshot.off(self.source)
                assert future.result(timeout=15) == expected
        finally:
            snapshot.off(self.source)
        catalog(self.name, selected, {"mongodb.socket_timeout_ms": "100"})
        assert sql(statement) == expected
        entered = snapshot.failpoint(self.source, {"failCommands": ["getMore"], "blockConnection": True,
                                                   "blockTimeMS": 500, "appName": application})
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(sql, statement)
                snapshot.wait_entered(self.source, entered)
                snapshot.off(self.source)
                try:
                    future.result(timeout=15)
                except MySQLError as error:
                    assert "mongodb" in str(error).lower() or "timeout" in str(error).lower()
                else:
                    raise AssertionError("The configured socket timeout was ignored")
        finally:
            snapshot.off(self.source)
        splits.cursors_closed([self.source, *[item[2] for item in self.members]])
        assert sql(statement) == expected

        catalog(self.name, selected, {"mongodb.pool_max_clients": "1"})
        entered = snapshot.failpoint(self.source, {"failCommands": ["getMore"], "blockConnection": True,
                                                   "blockTimeMS": 4000, "appName": application})
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                holder = executor.submit(sql, statement)
                snapshot.wait_entered(self.source, entered)
                before = len(self.commands(application))
                assert before > 0 and not holder.done()
                # Disabling a failpoint waits for active users and would release the holder
                # before the competitor starts. Keep it active and prove the waiter sends no
                # find; a second blocked getMore must not masquerade as pool admission.
                rejected(statement.replace("batch_size=1", "batch_size=1,query_timeout=2"), ("mongodb", "timeout"))
                assert not holder.done(), "The holder did not overlap the competing deadline"
                assert len(self.commands(application)) == before, "The waiter bypassed the shared client bound"
                assert holder.result(timeout=15) == expected
        finally:
            snapshot.off(self.source)
        assert sql(statement) == expected
        splits.cursors_closed([self.source, *[item[2] for item in self.members]])
        print("PASS:", self.label, "actual socket override/timeout, unequal-deadline pool admission and recovery", flush=True)

    def validation(self):
        invalid = {"mongodb.read_preference": ["invalid"], "mongodb.read_concern": ["linearizable", "inherit"],
                   "mongodb.schema.mode": ["inferred"], "mongodb.type_mismatch": ["skip"],
                   "mongodb.datetime_timezone": ["Asia/Shanghai"], "mongodb.pool_max_clients": ["0", "-1", "1025"]}
        for name in ["mongodb.connect_timeout_ms", "mongodb.server_selection_timeout_ms", "mongodb.socket_timeout_ms"]:
            invalid[name] = ["0", "-1", "300001"]
        for name, values in invalid.items():
            for value in values:
                try:
                    catalog(self.name, self.reader, {name: value})
                except Exception as error:
                    assert "mongodb" in str(error).lower()
                else:
                    raise AssertionError("Invalid catalog setting succeeded: " + name)
        catalog(self.name, self.reader)
        for concern in ["local", "majority", "available"]:
            rejected("SELECT /*+ SET_VAR(mongodb_split_strategy='range',mongodb_read_concern="
                     + snapshot.sql_quote(concern) + ") */ seq FROM " + self.name + "." + DATABASE
                     + ".integer_records", ("snapshot_required",))
        for setting in ["mongodb_split_strategy='invalid'", "mongodb_read_concern='linearizable'",
                        "mongodb_read_preference='invalid'", "mongodb_split_field='a.b'"]:
            rejected("SELECT /*+ SET_VAR(" + setting + ") */ seq FROM " + self.name + "." + DATABASE
                     + ".integer_records", ("mongodb",))
        for batch in [0, 65536]:
            rejected("SELECT /*+ SET_VAR(batch_size=" + str(batch) + ") */ seq FROM " + self.name
                     + "." + DATABASE + ".integer_records", ("batch_size",))
        print("PASS:", self.label, "invalid catalog/session bounds and unqualified range rejection", flush=True)

    def close(self):
        sql("DROP CATALOG IF EXISTS " + self.name)
        for _, _, client, previous in self.members:
            client[DATABASE].command("profile", previous["was"], slowms=previous["slowms"])
            client.close()
        self.source.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture-state", required=True)
    parser.add_argument("--catalog-prefix", default="mongodb_read_parameters")
    parser.add_argument("--allow-failpoints", action="store_true", required=True,
                        help="the source must be an owned disposable fixture")
    parser.add_argument("--report")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.catalog_prefix):
        parser.error("catalog-prefix must be an unquoted SQL identifier")
    # Pool limits are BE-local; this fault oracle requires both queries on the same BE.
    backends = sql("SHOW BACKENDS")
    assert sum(str(row[9]).lower() == "true" for row in backends) == 1, "Use exactly one live BE for this verifier"
    state = json.loads(Path(args.fixture_state).read_text())
    results = []
    for index in [0, 1]:
        deployment = Deployment(state, index, args.catalog_prefix)
        try:
            deployment.matrix()
            deployment.routing()
            deployment.resources()
            deployment.schemas_and_auth()
            deployment.transport_faults()
            deployment.validation()
            splits.cursors_closed([member[2] for member in deployment.members])
            results += deployment.results
        finally:
            deployment.close()
    if args.report:
        Path(args.report).write_text(json.dumps(results, indent=2))
    print("PASS: native read-parameter qualification completed", flush=True)


if __name__ == "__main__":
    main()
