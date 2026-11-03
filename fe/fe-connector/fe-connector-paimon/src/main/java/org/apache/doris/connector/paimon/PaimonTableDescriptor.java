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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TPaimonCatalogContext;
import org.apache.doris.thrift.TPaimonTableDesc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Portable table metadata pinned by FE for Paimon readers and writers. */
public final class PaimonTableDescriptor {
    private PaimonTableDescriptor() {
    }

    public static TPaimonTableDesc describeForScan(FileStoreTable table, String database, String name,
            Map<String, String> storageOptions) {
        return describe(table.location().toString(), PaimonScanParams.withoutTimeTravelSelectors(table.schema()),
                database, name, storageOptions);
    }

    public static TPaimonTableDesc describe(String location, TableSchema schema, String database, String name,
            Map<String, String> storageOptions) {
        TPaimonTableDesc result = new TPaimonTableDesc();
        result.setTablePath(location);
        try {
            result.setSchemaJson(JsonSerdeUtil.toJson(schema));
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to serialize Paimon table schema", e);
        }
        result.setDatabaseName(database);
        result.setTableName(name);
        result.setBranch(CoreOptions.branch(schema.options()));
        result.setStorageOptions(normalizeStorage(location, storageOptions));
        return result;
    }

    public static TPaimonTableDesc describe(FileStoreTable table, TableSchema schema, String database, String name,
            Map<String, String> storageOptions, boolean supportsPartitionModification) {
        CatalogEnvironment environment = table.catalogEnvironment();
        Identifier identifier = environment.identifier();
        // Doris names may be aliases or case mappings; catalog services need the pinned remote identity.
        TPaimonTableDesc result = describe(table.location().toString(), schema,
                identifier == null ? database : identifier.getDatabaseName(),
                identifier == null ? name : identifier.getTableName(), storageOptions);
        TPaimonCatalogContext context = new TPaimonCatalogContext();
        context.setCatalogLoaderEnabled(environment.catalogLoader() != null);
        context.setSupportsVersionManagement(environment.supportsVersionManagement());
        context.setSupportsPartitionModification(supportsPartitionModification);
        if (environment.uuid() != null) {
            context.setTableUuid(environment.uuid());
        }
        if (environment.catalogContext() != null) {
            context.setOptions(environment.catalogContext().options().toMap());
            Map<String, String> hadoopConfig = new HashMap<>();
            environment.catalogContext().hadoopConf().forEach(entry -> {
                String[] sources = environment.catalogContext().hadoopConf().getPropertySources(entry.getKey());
                // Both sides ship the same Hadoop defaults. Carry site files and explicit overrides,
                // rather than copying thousands of built-in settings into each descriptor.
                if (sources == null || sources.length == 0
                        || Arrays.stream(sources).anyMatch(source -> !source.endsWith("-default.xml"))) {
                    hadoopConfig.put(entry.getKey(), entry.getValue());
                }
            });
            context.setHadoopConfig(hadoopConfig);
        } else {
            context.setOptions(Collections.emptyMap());
            context.setHadoopConfig(Collections.emptyMap());
        }
        result.setCatalogContext(context);
        return result;
    }

    static Map<String, String> normalizeStorage(String location, Map<String, String> original) {
        Map<String, String> result = new HashMap<>(original);
        int separator = location.indexOf("://");
        String scheme = separator < 0 ? "" : location.substring(0, separator).toLowerCase(Locale.ROOT);
        if (scheme.equals("oss")) {
            copyIfMissing(result, "AWS_ENDPOINT", "fs.oss.endpoint");
            copyIfMissing(result, "AWS_ACCESS_KEY", "fs.oss.accessKeyId");
            copyIfMissing(result, "AWS_SECRET_KEY", "fs.oss.accessKeySecret");
            copyIfMissing(result, "AWS_TOKEN", "fs.oss.securityToken");
            return result;
        }
        if (!scheme.equals("s3") && !scheme.equals("s3a")) {
            return result;
        }
        String[][] aliases = {
                {"AWS_ACCESS_KEY", "s3.access-key"}, {"AWS_SECRET_KEY", "s3.secret-key"},
                {"AWS_TOKEN", "s3.session.token"}, {"AWS_ENDPOINT", "s3.endpoint"},
                {"AWS_REGION", "s3.region"}, {"use_path_style", "s3.path-style-access"},
                {"fs.s3a.access.key", "s3.access-key"}, {"fs.s3a.secret.key", "s3.secret-key"},
                {"fs.s3a.endpoint.region", "s3.region"},
                {"AWS_ROLE_ARN", "s3.assumed.role.arn"}, {"AWS_EXTERNAL_ID", "s3.assumed.role.externalId"},
                {"fs.oss.accessKeyId", "s3.access-key"}, {"fs.oss.accessKeySecret", "s3.secret-key"},
                {"fs.oss.sessionToken", "s3.session.token"}, {"fs.oss.endpoint", "s3.endpoint"},
                {"fs.oss.region", "s3.region"}
        };
        for (String[] alias : aliases) {
            copyIfMissing(result, alias[0], alias[1]);
        }
        if ("ANONYMOUS".equals(original.get("AWS_CREDENTIALS_PROVIDER_TYPE"))) {
            result.put("s3.anonymous", "true");
        }
        return result;
    }

    private static void copyIfMissing(Map<String, String> options, String source, String target) {
        String value = options.get(source);
        if (value != null && !value.isEmpty()) {
            options.putIfAbsent(target, value);
        }
    }
}
