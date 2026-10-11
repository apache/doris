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

import java.nio.channels.Channels

// The Flight SQL JDBC driver on the classpath shades Arrow Flight; its FlightSqlClient is the
// one a test can drive directly (see test_session_options).
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CloseSessionRequest
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.pojo.Schema

// What a Flight SQL client is told a column is. The table_schema column of GetTables(include_schema)
// is the one statement FE makes about the Arrow type of a column, and a client that types its columns
// from it (the ADBC drivers do) reads the batches BE emits as that type: a wrong cell is not a
// degraded answer but a failed read. This pins the schema for a column of every type, nested types
// down to the leaves, exactly as it is served today -- including the cells known to disagree with
// what BE emits, which stay as they are on purpose until the BE Arrow type layer is reworked and the
// whole mapping is corrected in one step (#67577): TIMESTAMPTZ carries the literal zone "UTC" where
// BE stamps the session time zone, and AGG_STATE is described as Null where BE emits binary.
// A change to any line below is a change every GetTables client sees; make it deliberately, and make
// it in DorisArrowTypeMapping, the one place FE maps a Doris type to an Arrow type.
//
// Not in the 'arrow_flight_sql' group on purpose: `sql` stays the MySQL control connection that
// creates the tables, and GetTables is asked through a raw Flight SQL client.
suite("test_get_tables_schema") {
    String host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    int port = context.config.otherConfigs.get("extArrowFlightSqlPort") as int
    String user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String password = context.config.otherConfigs.get("extArrowFlightSqlPassword")

    def db = context.dbName
    def allTypes = "get_tables_schema_all_types"
    def aggTypes = "get_tables_schema_agg_types"
    def dec256 = "get_tables_schema_dec256"

    sql "DROP TABLE IF EXISTS ${allTypes}"
    sql """
        CREATE TABLE ${allTypes} (
            k_int INT NOT NULL,
            c_bool BOOLEAN,
            c_tinyint TINYINT,
            c_smallint SMALLINT,
            c_bigint BIGINT,
            c_largeint LARGEINT,
            c_float FLOAT,
            c_double DOUBLE,
            c_decimal_9_2 DECIMAL(9, 2),
            c_decimal_18_4 DECIMAL(18, 4),
            c_decimal_38_10 DECIMAL(38, 10),
            c_date DATE,
            c_datetime_0 DATETIME(0),
            c_datetime_3 DATETIME(3),
            c_datetime_6 DATETIME(6),
            c_timestamp_ns TIMESTAMP_NS,
            c_timestamptz_0 TIMESTAMPTZ(0),
            c_timestamptz_3 TIMESTAMPTZ(3),
            c_timestamptz_6 TIMESTAMPTZ(6),
            c_char CHAR(10),
            c_varchar VARCHAR(100),
            c_string STRING,
            c_json JSON,
            c_variant VARIANT,
            c_ipv4 IPV4,
            c_ipv6 IPV6,
            c_array_int ARRAY<INT>,
            c_array_datetime ARRAY<DATETIME(6)>,
            c_map MAP<VARCHAR(20), BIGINT>,
            c_struct STRUCT<f1: INT, f2: STRING, f3: ARRAY<DATE>>,
            c_nested ARRAY<MAP<STRING, ARRAY<DECIMAL(10, 3)>>>
        )
        DUPLICATE KEY(k_int)
        DISTRIBUTED BY HASH(k_int) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "SET enable_agg_state = true"
    sql "DROP TABLE IF EXISTS ${aggTypes}"
    sql """
        CREATE TABLE ${aggTypes} (
            k_int INT NOT NULL,
            c_bitmap BITMAP BITMAP_UNION,
            c_hll HLL HLL_UNION,
            c_quantile_state QUANTILE_STATE QUANTILE_UNION,
            c_agg_state AGG_STATE<max_by(INT, INT)> GENERIC
        )
        AGGREGATE KEY(k_int)
        DISTRIBUTED BY HASH(k_int) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "SET enable_decimal256 = true"
    sql "DROP TABLE IF EXISTS ${dec256}"
    sql """
        CREATE TABLE ${dec256} (
            k_int INT NOT NULL,
            c_decimal_76_20 DECIMAL(76, 20)
        )
        DUPLICATE KEY(k_int)
        DISTRIBUTED BY HASH(k_int) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // One line per field, children indented under their parent: the Arrow type as Arrow Java spells
    // it, "not null" where the schema says so, and in braces the Flight SQL column metadata a client's
    // ResultSetMetaData reads -- the Doris type name, the precision and the scale, when present.
    def describe
    describe = { Field field, int depth, List<String> out ->
        def meta = field.getMetadata()
        def tags = []
        if (meta.containsKey("ARROW:FLIGHT:SQL:TYPE_NAME")) {
            tags << meta.get("ARROW:FLIGHT:SQL:TYPE_NAME")
        }
        if (meta.containsKey("ARROW:FLIGHT:SQL:PRECISION")) {
            tags << "precision=" + meta.get("ARROW:FLIGHT:SQL:PRECISION")
        }
        if (meta.containsKey("ARROW:FLIGHT:SQL:SCALE")) {
            tags << "scale=" + meta.get("ARROW:FLIGHT:SQL:SCALE")
        }
        def line = ("  " * depth) + field.getName() + ": " + field.getType()
        line += field.isNullable() ? "" : " not null"
        line += tags.isEmpty() ? "" : " {" + tags.join(", ") + "}"
        out << line
        field.getChildren().each { describe(it, depth + 1, out) }
    }

    def allocator = new RootAllocator()
    def client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build()
    try {
        def cred = client.authenticateBasicToken(user, password).get()
        def flight = new FlightSqlClient(client)

        // GetTables(include_schema) for one table, its table_schema decoded the way a client decodes it.
        def tableSchema = { String table ->
            def info = flight.getTables("internal", db, table, null, true, cred)
            def schemas = []
            info.getEndpoints().each { endpoint ->
                flight.getStream(endpoint.getTicket(), cred).withCloseable { stream ->
                    while (stream.next()) {
                        def root = stream.getRoot()
                        def names = root.getVector("table_name")
                        def bytes = (VarBinaryVector) root.getVector("table_schema")
                        for (int i = 0; i < root.getRowCount(); i++) {
                            assertEquals(table, names.getObject(i).toString())
                            schemas << MessageSerializer.deserializeSchema(new ReadChannel(
                                    Channels.newChannel(new ByteArrayInputStream(bytes.get(i)))))
                        }
                    }
                }
            }
            assertEquals(1, schemas.size(), "GetTables should describe ${db}.${table} exactly once")
            return (Schema) schemas[0]
        }
        // The expected block is written indented for readability; the least-indented line is depth 0.
        def dedent = { String text ->
            def lines = text.readLines().findAll { !it.trim().isEmpty() }
            int indent = lines.collect { it.length() - it.stripLeading().length() }.min()
            return lines.collect { it.substring(indent) }.join("\n")
        }
        def check = { String table, String expected ->
            Schema schema = tableSchema(table)
            def lines = []
            schema.getFields().each { describe(it, 0, lines) }
            assertEquals(dedent(expected), lines.join("\n"),
                    "the GetTables schema of ${db}.${table} changed; see DorisArrowTypeMapping")
            // Every column names the table it belongs to and is read-only, as JDBC clients expect.
            schema.getFields().each { field ->
                def meta = field.getMetadata()
                assertEquals(db, meta.get("ARROW:FLIGHT:SQL:SCHEMA_NAME"), field.getName())
                assertEquals(table, meta.get("ARROW:FLIGHT:SQL:TABLE_NAME"), field.getName())
                assertEquals("1", meta.get("ARROW:FLIGHT:SQL:IS_READ_ONLY"), field.getName())
                assertEquals("1", meta.get("ARROW:FLIGHT:SQL:IS_SEARCHABLE"), field.getName())
                assertEquals("0", meta.get("ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT"), field.getName())
                assertEquals("0", meta.get("ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE"), field.getName())
            }
        }

        // DATETIME is a wall-clock value: a timezone-naive timestamp whose unit follows the scale.
        // TIMESTAMPTZ takes the same units but the literal zone "UTC" (kept as is, see the header).
        // LARGEINT is text, as BE writes it; DATE is a day number (date32), not date64.
        // A map is list<entries: struct<key, value>> with the entries struct and the key not null,
        // whatever the column declares, because an Arrow map with a nullable key is not a schema.
        check(allTypes, """
            k_int: Int(32, true) not null {INT, precision=10, scale=0}
            c_bool: Bool {BOOLEAN, scale=0}
            c_tinyint: Int(8, true) {TINYINT, precision=3, scale=0}
            c_smallint: Int(16, true) {SMALLINT, precision=5, scale=0}
            c_bigint: Int(64, true) {BIGINT, precision=19, scale=0}
            c_largeint: Utf8 {LARGEINT, precision=39}
            c_float: FloatingPoint(SINGLE) {FLOAT, precision=7, scale=7}
            c_double: FloatingPoint(DOUBLE) {DOUBLE, precision=15, scale=15}
            c_decimal_9_2: Decimal(9, 2, 128) {DECIMAL32, precision=9, scale=2}
            c_decimal_18_4: Decimal(18, 4, 128) {DECIMAL64, precision=18, scale=4}
            c_decimal_38_10: Decimal(38, 10, 128) {DECIMAL128, precision=38, scale=10}
            c_date: Date(DAY) {DATEV2}
            c_datetime_0: Timestamp(SECOND, null) {DATETIMEV2, precision=18, scale=0}
            c_datetime_3: Timestamp(MILLISECOND, null) {DATETIMEV2, precision=18, scale=3}
            c_datetime_6: Timestamp(MICROSECOND, null) {DATETIMEV2, precision=18, scale=6}
            c_timestamp_ns: Timestamp(NANOSECOND, null) {TIMESTAMP_NS, precision=29, scale=9}
            c_timestamptz_0: Timestamp(SECOND, UTC) {TIMESTAMPTZ, precision=18, scale=0}
            c_timestamptz_3: Timestamp(MILLISECOND, UTC) {TIMESTAMPTZ, precision=18, scale=3}
            c_timestamptz_6: Timestamp(MICROSECOND, UTC) {TIMESTAMPTZ, precision=18, scale=6}
            c_char: Utf8 {CHAR}
            c_varchar: Utf8 {VARCHAR}
            c_string: Utf8 {STRING}
            c_json: Utf8 {JSON}
            c_variant: Utf8 {VARIANT}
            c_ipv4: Int(32, true) {IPV4}
            c_ipv6: Utf8 {IPV6}
            c_array_int: List {ARRAY}
              item: Int(32, true) {INT, precision=10, scale=0}
            c_array_datetime: List {ARRAY}
              item: Timestamp(MICROSECOND, null) {DATETIMEV2, precision=18, scale=6}
            c_map: Map(false) {MAP}
              entries: Struct not null
                key: Utf8 not null
                value: Int(64, true) {BIGINT, precision=19, scale=0}
            c_struct: Struct {STRUCT}
              f1: Int(32, true) {INT, precision=10, scale=0}
              f2: Utf8 {STRING}
              f3: List {ARRAY}
                item: Date(DAY) {DATEV2}
            c_nested: List {ARRAY}
              item: Map(false) {MAP}
                entries: Struct not null
                  key: Utf8 not null
                  value: List {ARRAY}
                    item: Decimal(10, 3, 128) {DECIMAL64, precision=10, scale=3}
        """)

        // BITMAP / HLL / QUANTILE_STATE are opaque bytes. AGG_STATE is described as Null (kept as is,
        // see the header).
        check(aggTypes, """
            k_int: Int(32, true) not null {INT, precision=10, scale=0}
            c_bitmap: Binary not null {BITMAP}
            c_hll: Binary not null {HLL}
            c_quantile_state: Binary not null {QUANTILE_STATE}
            c_agg_state: Null not null {AGG_STATE}
        """)

        check(dec256, """
            k_int: Int(32, true) not null {INT, precision=10, scale=0}
            c_decimal_76_20: Decimal(76, 20, 256) {DECIMAL256, precision=76, scale=20}
        """)

        // End the session the way the drivers do rather than leaving it to wait_timeout.
        flight.closeSession(new CloseSessionRequest(), cred)
    } finally {
        client.close()
        allocator.close()
    }
}
