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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Pins that paimon CREATE TABLE rejects a {@code DISTRIBUTE BY} clause, moved off fe-core {@code CreateTableInfo}.
 *
 * <p><b>WHY this matters (Rule 9):</b> {@code PaimonSchemaBuilder} deliberately ignores {@code bucketSpec}, so
 * without a connector-side reject a {@code CREATE TABLE ... ENGINE=paimon DISTRIBUTE BY HASH(x)} would silently
 * succeed and drop the user's distribution intent. This test locks the reject in.</p>
 *
 * <p>{@code rejectDistribution} is package-private (reached only via {@code createTable} in production); the test
 * constructs the metadata offline with null catalog/context (the validator touches only the request), mirroring
 * {@code MaxComputeValidateColumnsTest}.</p>
 */
public class PaimonCreateTableValidationTest {

    private PaimonConnectorMetadata metadata() {
        // Non-null (empty) properties: the ctor derives type-mapping options from them; catalog/context stay null
        // (rejectDistribution touches only the request).
        return new PaimonConnectorMetadata(null, Collections.emptyMap(), null);
    }

    @Test
    public void distributeByIsRejected() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Collections.singletonList(
                        new ConnectorColumn("id", ConnectorType.of("INT"), "", true, null)))
                .bucketSpec(new ConnectorBucketSpec(Collections.singletonList("id"), 4, "doris_default"))
                .build();

        // WHY: paimon has no DISTRIBUTE BY (buckets go in PARTITIONED BY via bucket(n, col)); the schema builder
        // ignores bucketSpec, so silent acceptance would drop the clause. MUTATION: dropping the reject go red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().rejectDistribution(request));
        Assertions.assertTrue(ex.getMessage().contains("Paimon doesn't support 'DISTRIBUTE BY'"),
                "must reproduce the former fe-core DISTRIBUTE BY message");
    }

    @Test
    public void noDistributeByPasses() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Collections.singletonList(
                        new ConnectorColumn("id", ConnectorType.of("INT"), "", true, null)))
                .build();
        // WHY: guards against over-rejection -- a create with no DISTRIBUTE BY must pass.
        Assertions.assertDoesNotThrow(() -> metadata().rejectDistribution(request));
    }
}
