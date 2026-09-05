// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// A catalog stating the lake settings its fluss cluster cannot send it.
//
// Every catalog in this directory already relies on the mechanism: fluss removes
// every lake option whose name holds key, secret or password before it hands a
// table's properties to a client, so the credentials for the object store the
// lake sits in reach Doris only as `fluss.lake.paimon.*` properties of its own.
// What the other suites do not show is what happens when a value is WRONG, which
// is where an override that never arrived and an override that arrived look
// different for the first time.
//
// The keys are spelled the way paimon spells them, so a user can copy the tail
// of a `datalake.paimon.*` line out of a fluss cluster's configuration. They
// override the cluster's values one key at a time rather than replacing the set.
suite("test_fluss_lake_override", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String minioPort = context.config.otherConfigs.get("fluss_minio_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String endpoint = "http://${externalEnvIp}:${minioPort}"
    // Where the fluss cluster puts the lake; docker-compose/fluss/fluss.env.tpl.
    String warehouse = "s3://fluss-lake/wh"

    String controlCatalog = "test_fluss_lake_override"
    String sameCatalog = "test_fluss_lake_override_same"
    String flavorCatalog = "test_fluss_lake_override_flavor"
    String wrongCatalog = "test_fluss_lake_override_wrong"
    String noCredCatalog = "test_fluss_lake_override_nocred"

    def dropAll = {
        for (String name : [controlCatalog, sameCatalog, flavorCatalog, wrongCatalog, noCredCatalog]) {
            sql """drop catalog if exists ${name}"""
        }
    }
    dropAll()

    // The control: the credentials, and nothing else overridden. Everything below is read
    // against this, so that a difference is attributable to the override under test.
    sql """
        create catalog ${controlCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "${endpoint}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin"
        );
    """
    sql """switch ${controlCatalog}"""
    sql """use fluss_test"""
    sql """set enable_file_scanner_v2 = true"""

    def rowsOf = { String query -> sql(query).collect { row -> row.collect { it.toString() } } }

    order_qt_control_lake """select id, name, price from lake_log\$lake"""

    // --- an override that repeats the cluster changes nothing -----------------
    // The path itself has to be inert when it carries the same value: a catalog that
    // states the warehouse it already has must read exactly what the control reads.
    // Otherwise every case below would be measuring the override machinery rather than
    // the value it carried.
    sql """
        create catalog ${sameCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "${endpoint}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.lake.paimon.warehouse" = "${warehouse}"
        );
    """
    assertEquals(rowsOf("select id, name from lake_log\$lake order by id"),
            rowsOf("select id, name from ${sameCatalog}.fluss_test.lake_log\$lake order by id"),
            "restating the cluster's own warehouse changed what the lake reads")

    // --- one key at a time, not the whole set ---------------------------------
    // Stating the flavor and nothing else has to leave the warehouse alone. An override
    // implemented as "use these settings instead of the cluster's" would lose it here,
    // and the failure would be about a warehouse nobody mentioned.
    sql """
        create catalog ${flavorCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "${endpoint}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.lake.paimon.metastore" = "filesystem"
        );
    """
    order_qt_flavor_only_lake """select count(*) from ${flavorCatalog}.fluss_test.lake_log\$lake"""

    // --- an override that is wrong has to be wrong on both paths --------------
    // A warehouse that holds no such table, which is a location nothing could have
    // written -- so if either read still succeeds, that read never saw the override.
    //
    // The two reads are the two places the overrides have to be handed over, and they are
    // reached separately: resolving `tbl$lake` goes through the connector's metadata,
    // planning the base table's union read goes through its scan planner. Each builds the
    // sibling connector for itself. One of them left out would build a SECOND sibling
    // configured from the cluster alone -- which would work, and quietly read the wrong
    // lake -- so both are asked, and they fail with different messages because they fail
    // in different places.
    sql """
        create catalog ${wrongCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "${endpoint}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.lake.paimon.warehouse" = "s3://fluss-lake/no-such-warehouse"
        );
    """
    test {
        sql """select * from ${wrongCatalog}.fluss_test.lake_log\$lake"""
        exception "nothing has been tiered"
    }
    test {
        sql """set fluss_union_read_mode = 'required'"""
        sql """select * from ${wrongCatalog}.fluss_test.lake_log"""
        exception "warehouse and the fluss cluster disagree"
    }
    sql """set fluss_union_read_mode = ''"""

    // --- a wrong override can be taken back -----------------------------------
    // ALTER CATALOG can set a property but not remove one, so a blank value is the only
    // way back from a typo. It therefore has to mean "not stated" rather than "stated as
    // empty": the latter would leave the catalog reading a warehouse called "" forever.
    sql """alter catalog ${wrongCatalog} set properties ("fluss.lake.paimon.warehouse" = "")"""
    assertEquals(rowsOf("select id, name from lake_log\$lake order by id"),
            rowsOf("select id, name from ${wrongCatalog}.fluss_test.lake_log\$lake order by id"),
            "blanking the override did not hand the warehouse back to the cluster")

    // --- a flavor the connector cannot serve is refused by name ---------------
    // Not by falling back to the default: a catalog configured for one metastore and
    // reading another would answer from whatever the default happens to find.
    sql """
        create catalog ${flavorCatalog}_jdbc properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "${endpoint}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.lake.paimon.metastore" = "jdbc"
        );
    """
    test {
        sql """select * from ${flavorCatalog}_jdbc.fluss_test.lake_log\$lake"""
        exception "'jdbc'"
    }
    sql """drop catalog if exists ${flavorCatalog}_jdbc"""

    // --- the lake keys are Doris's own, not the fluss client's ----------------
    // They share the catalog's property namespace with the settings that DO go to fluss,
    // and they are told apart by prefix. Told apart wrongly, they would reach the fluss
    // client stripped of `fluss.` -- as `lake.paimon.*` options it has never heard of --
    // and the first thing to fail would be a table that has nothing to do with any lake.
    order_qt_non_lake_table_reads """select count(*) from ${sameCatalog}.fluss_test.log_basic"""

    // --- the credentials are what the lake cannot be read without -------------
    // The closing case of the whole arrangement. This catalog names the object store and
    // omits the credentials, which is precisely what a user gets by configuring nothing:
    // fluss sends the endpoint (its name holds no secret) and strips the keys. So the
    // read must fail. The control at the top of this suite is the same catalog with the
    // credentials added, and it reads the data -- not just the metadata, which is planned
    // on the FE; the rows come off parquet opened by the BE, so both halves of the engine
    // were configured from these two properties.
    sql """
        create catalog ${noCredCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "${endpoint}"
        );
    """
    test {
        sql """select * from ${noCredCatalog}.fluss_test.lake_log\$lake"""
        exception "s3"
    }
    // The same catalog reads what needs no object store at all, so the failure above is
    // about reaching the lake and not about the catalog being unusable.
    order_qt_no_cred_reads_fluss """select count(*) from ${noCredCatalog}.fluss_test.log_basic"""

    // --- a storage setting that is not a credential ---------------------------
    // How a bucket is addressed is storage too, and it is the one storage key a fluss
    // cluster does send -- its name holds no secret. Carried to the lake catalog instead
    // of to the engine's storage layer it would configure the FE and not the BE. This
    // environment addresses minio by IP, where the addressing rules fall back to path
    // style on their own, so what a read proves here is that stating the key is accepted
    // and harmless -- not that it took effect. Nothing end to end can show the latter
    // without an object store named by a hostname.
    sql """
        create catalog ${controlCatalog}_ps properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "${endpoint}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.lake.paimon.s3.path.style.access" = "true"
        );
    """
    assertEquals(rowsOf("select id, name from lake_log\$lake order by id"),
            rowsOf("select id, name from ${controlCatalog}_ps.fluss_test.lake_log\$lake order by id"),
            "stating how the bucket is addressed changed what the lake reads")
    sql """drop catalog if exists ${controlCatalog}_ps"""

    sql """switch internal"""
    dropAll()
}
