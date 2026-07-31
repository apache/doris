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

package org.apache.doris.connector.adbc;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What a scan range puts on the wire.
 *
 * <p>Every assertion here is one half of a contract whose other half is C++ in
 * {@code be/src/format_v2/table/adbc_reader.cpp}. Nothing checks the two agree at build time, so a key
 * renamed on one side surfaces as a scan that fails at run time complaining about a missing parameter --
 * which is why the names are asserted literally rather than through the constants that produced them.
 */
class AdbcScanRangeTest {

    private static AdbcScanRange.Builder minimal() {
        return new AdbcScanRange.Builder()
                .driverPath("/opt/doris/plugins/adbc_drivers/libadbc_driver_sqlite.so")
                .uri("file:/tmp/x.db")
                .querySql("SELECT \"a\" FROM \"main\".\"t1\"");
    }

    private static Map<String, String> adbcParams(AdbcScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());
        return formatDesc.getAdbcParams();
    }

    @Test
    void routesToTheAdbcReaderThroughTheArrowPath() {
        // Both are required: BE enters its Arrow scanner on the format and picks the ADBC reader on the
        // table format. Either one alone lands the scan in a reader with no ADBC branch.
        Assertions.assertEquals("arrow", minimal().build().getFileFormat());
        Assertions.assertEquals("adbc", minimal().build().getTableFormatType());
    }

    @Test
    void carriesAPlaceholderPathWithNoScheme() {
        // There is no file. A scheme-bearing placeholder risks being resolved as a filesystem, so the
        // shape follows the remote_doris scan node, which reads Arrow the same way.
        String path = minimal().build().getPath().orElse(null);
        Assertions.assertNotNull(path);
        Assertions.assertFalse(path.contains("://"), path);
    }

    @Test
    void writesTheParametersIntoTheAdbcSlotAndNotTheJdbcOne() {
        // The inherited implementation writes jdbc_params, which BE's ADBC reader never reads: the scan
        // would arrive with no driver path at all.
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        minimal().build().populateRangeParams(formatDesc, new TFileRangeDesc());

        Assertions.assertTrue(formatDesc.isSetAdbcParams());
        Assertions.assertFalse(formatDesc.isSetJdbcParams());
    }

    @Test
    void usesTheParameterNamesBeLooksUp() {
        Map<String, String> params = adbcParams(minimal()
                .driverEntrypoint("AdbcDriverInit")
                .username("alice")
                .password("secret")
                .build());

        Assertions.assertEquals("/opt/doris/plugins/adbc_drivers/libadbc_driver_sqlite.so",
                params.get("driver_path"));
        Assertions.assertEquals("AdbcDriverInit", params.get("driver_entrypoint"));
        Assertions.assertEquals("file:/tmp/x.db", params.get("uri"));
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"", params.get("query_sql"));
    }

    @Test
    void carriesAPartitionDescriptorInsteadOfAStatement() {
        Map<String, String> params = adbcParams(new AdbcScanRange.Builder()
                .driverPath("/opt/doris/plugins/adbc_drivers/libadbc_driver_flightsql.so")
                .uri("grpc://remote:9090")
                .partitionDescriptor("Zm9vYmFy")
                .build());

        Assertions.assertEquals("Zm9vYmFy", params.get("partition_descriptor"));
        // No statement travels with a partition: the source already ran it, and a BE that found both
        // would have to guess which one the plan meant.
        Assertions.assertFalse(params.containsKey("query_sql"));
    }

    @Test
    void refusesToCarryBothKindsOfWorkOrNeither() {
        // The two are alternatives, and BE rejects a range that says both or neither. Failing while
        // planning names the bug; failing on BE reports it as one backend's problem, mid-query.
        IllegalStateException both = Assertions.assertThrows(IllegalStateException.class,
                () -> minimal().partitionDescriptor("Zm9vYmFy").build());
        Assertions.assertTrue(both.getMessage().contains("both"), both.getMessage());

        IllegalStateException neither = Assertions.assertThrows(IllegalStateException.class,
                () -> new AdbcScanRange.Builder()
                        .driverPath("/opt/doris/plugins/adbc_drivers/libadbc_driver_sqlite.so")
                        .uri("file:/tmp/x.db")
                        .build());
        Assertions.assertTrue(neither.getMessage().contains("neither"), neither.getMessage());
    }

    @Test
    void sendsTheUserPropertyUnderAdbcsNameForIt() {
        // The catalog property is 'user'; the ADBC option is 'username'. Sending the property name would
        // leave the source unauthenticated with no complaint from either side.
        Map<String, String> params = adbcParams(minimal().username("alice").password("secret").build());
        Assertions.assertEquals("alice", params.get("username"));
        Assertions.assertEquals("secret", params.get("password"));
        Assertions.assertFalse(params.containsKey("user"));
    }

    @Test
    void omitsCredentialsAndEntrypointWhenThereAreNone() {
        Map<String, String> params = adbcParams(minimal()
                .driverEntrypoint(null).username(null).password("").build());
        Assertions.assertFalse(params.containsKey("username"));
        Assertions.assertFalse(params.containsKey("password"));
        // An empty entrypoint is not "use the default": BE hands whatever is present to dlsym.
        Assertions.assertFalse(params.containsKey("driver_entrypoint"));
    }

    @Test
    void passesDriverOptionsThroughWithTheirPrefixIntact() {
        // The "adbc." prefix is part of the ADBC option name, not a namespace this connector added, so
        // stripping it would name an option no driver knows.
        Map<String, String> options = new LinkedHashMap<>();
        options.put("adbc.snowflake.sql.db", "MYDB");
        Map<String, String> params = adbcParams(minimal().driverOptions(options).build());

        Assertions.assertEquals("MYDB", params.get("adbc.snowflake.sql.db"));
        Assertions.assertFalse(params.containsKey("snowflake.sql.db"));
    }
}
