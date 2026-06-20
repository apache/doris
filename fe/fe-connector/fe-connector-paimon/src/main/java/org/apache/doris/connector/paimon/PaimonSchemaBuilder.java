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
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Builds a Paimon {@link Schema} from a connector-SPI
 * {@link ConnectorCreateTableRequest}.
 *
 * <p>Functional port of the legacy fe-core
 * {@code PaimonMetadataOps.toPaimonSchema}: primary keys come from
 * {@code properties["primary-key"]}, partition keys come from the
 * {@link ConnectorPartitionSpec} (identity transforms only), {@code "primary-key"} and
 * {@code "comment"} are stripped from the option map, and {@code "location"} is re-keyed
 * to {@link CoreOptions#PATH}. Bucket / distribution info is intentionally NOT consumed —
 * legacy paimon ignored bucketSpec and let any {@code bucket} option ride through
 * unchanged as a passthrough option.</p>
 *
 * <p>Two deliberate, safer divergences from the legacy bytes (each documented + tested at
 * its call site): the table comment falls back to {@link ConnectorCreateTableRequest#getComment()}
 * (the {@code COMMENT} clause) when {@code properties["comment"]} is absent — legacy read only
 * the property and silently dropped the clause; and blank primary-key tokens are filtered out —
 * legacy would have forwarded an empty name that Paimon rejects downstream.</p>
 */
public final class PaimonSchemaBuilder {

    private static final String PRIMARY_KEY_IDENTIFIER = "primary-key";
    private static final String PROP_COMMENT = "comment";
    private static final String PROP_LOCATION = "location";
    private static final String IDENTITY_TRANSFORM = "identity";

    private PaimonSchemaBuilder() {
    }

    /**
     * Convert a CREATE TABLE request into a Paimon {@link Schema}.
     *
     * @throws DorisConnectorException if a partition field uses a non-identity transform
     *         or a column type cannot be represented in Paimon
     */
    public static Schema build(ConnectorCreateTableRequest request) {
        Map<String, String> properties = request.getProperties();

        // primary keys: from properties["primary-key"] only (no dedicated request field),
        // split on comma, trimmed, blanks dropped. Mirrors legacy toPaimonSchema.
        String pkAsString = properties.get(PRIMARY_KEY_IDENTIFIER);
        List<String> primaryKeys = pkAsString == null
                ? Collections.emptyList()
                : Arrays.stream(pkAsString.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());

        List<String> partitionKeys = partitionKeys(request.getPartitionSpec());

        // options normalization: drop primary-key/comment, re-key location -> CoreOptions.PATH.
        Map<String, String> normalizedOptions = new HashMap<>(properties);
        normalizedOptions.remove(PRIMARY_KEY_IDENTIFIER);
        normalizedOptions.remove(PROP_COMMENT);
        if (normalizedOptions.containsKey(PROP_LOCATION)) {
            String path = normalizedOptions.remove(PROP_LOCATION);
            normalizedOptions.put(CoreOptions.PATH.key(), path);
        }

        // comment resolution: legacy toPaimonSchema read ONLY properties["comment"] (the nereids
        // PROPERTIES("comment"=...) map); the dedicated COMMENT clause never reached it. The SPI
        // converter (CreateTableInfoToConnectorRequestConverter.convert) sets request.getComment()
        // from CreateTableInfo.getComment() (the COMMENT clause) and request.getProperties() from
        // CreateTableInfo.getProperties() (the PROPERTIES map) — two distinct nereids fields.
        // Resolution: properties["comment"] wins (preserves legacy persisted-comment behavior),
        // else fall back to request.getComment() so a user's COMMENT clause is not silently dropped.
        String comment = properties.containsKey(PROP_COMMENT)
                ? properties.get(PROP_COMMENT)
                : request.getComment();

        Schema.Builder builder = Schema.newBuilder()
                .options(normalizedOptions)
                .primaryKey(primaryKeys)
                .partitionKeys(partitionKeys)
                .comment(comment);
        for (ConnectorColumn col : request.getColumns()) {
            // Column-level nullability applied here via copy(nullable), mirroring legacy
            // toPaimonSchema's toPaimontype(type).copy(field.getContainsNull()).
            builder.column(col.getName(),
                    PaimonTypeMapping.toPaimonType(col.getType()).copy(col.isNullable()),
                    col.getComment());
        }
        return builder.build();
    }

    private static List<String> partitionKeys(ConnectorPartitionSpec spec) {
        if (spec == null) {
            return Collections.emptyList();
        }
        List<String> keys = new ArrayList<>(spec.getFields().size());
        for (ConnectorPartitionField field : spec.getFields()) {
            String transform = field.getTransform();
            // Paimon legacy only supported plain (identity) partition columns. Guard mirrors
            // MaxComputeConnectorMetadata.identityPartitionColumns. transform is @NonNull on
            // ConnectorPartitionField, so only the value matters.
            if (transform != null && !IDENTITY_TRANSFORM.equalsIgnoreCase(transform)) {
                throw new DorisConnectorException(
                        "Paimon only supports identity partition columns, got transform: " + transform);
            }
            keys.add(field.getColumnName());
        }
        return keys;
    }
}
