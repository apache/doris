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

package org.apache.doris.connector.fluss;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Covers the namespace listing surface of the fluss metadata.
 */
public class FlussConnectorMetadataTest {

    @Test
    public void listingIsPassedThroughToTheClusterUnchanged() {
        // Fluss already has database/table names in Doris's own shape, so nothing here may invent,
        // filter or re-case a name: what the cluster reports is what SHOW DATABASES / SHOW TABLES show.
        RecordingFlussAdminOps adminOps = new RecordingFlussAdminOps();
        adminOps.databases = Arrays.asList("fluss_db", "MixedCase");
        adminOps.tablesByDatabase.put("fluss_db", Arrays.asList("log_table", "pk_table"));
        adminOps.tablesByDatabase.put("empty_db", Collections.emptyList());

        FlussConnectorMetadata metadata = new FlussConnectorMetadata(adminOps);

        Assertions.assertEquals(Arrays.asList("fluss_db", "MixedCase"), metadata.listDatabaseNames(null));
        Assertions.assertEquals(Arrays.asList("log_table", "pk_table"), metadata.listTableNames(null, "fluss_db"));
        Assertions.assertEquals(Collections.emptyList(), metadata.listTableNames(null, "empty_db"));
        Assertions.assertTrue(metadata.databaseExists(null, "fluss_db"));
        Assertions.assertFalse(metadata.databaseExists(null, "absent_db"));

        Assertions.assertEquals(
                Arrays.asList("listDatabases()", "listTables(fluss_db)", "listTables(empty_db)",
                        "databaseExists(fluss_db)", "databaseExists(absent_db)"),
                adminOps.calls);
    }
}
