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

package org.apache.doris.datasource.paimon;

import org.apache.doris.thrift.TPaimonCatalogContext;
import org.apache.doris.thrift.TPaimonTableDesc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Portable metadata pinned by FE and consumed by both Paimon writers. */
public final class PaimonTableDescriptor {
    private PaimonTableDescriptor() {
    }

    public static TPaimonTableDesc describe(FileStoreTable table, String database, String name,
            Map<String, String> storageOptions, boolean rustWriter, boolean supportsPartitionModification) {
        TPaimonTableDesc result = new TPaimonTableDesc();
        result.setTablePath(table.location().toString());
        Map<String, String> writerOptions = new HashMap<>(table.schema().options());
        if (rustWriter) {
            writerOptions.put("write.parquet-buffer-size", "8 mb");
        }
        result.setSchemaJson(PaimonUtil.encodeTableSchemaToJson(table.schema().copy(writerOptions)));
        CatalogEnvironment environment = table.catalogEnvironment();
        Identifier identifier = environment.identifier();
        // Doris names may be aliases or case mappings; catalog services need the pinned remote identity.
        result.setDatabaseName(identifier == null ? database : identifier.getDatabaseName());
        result.setTableName(identifier == null ? name : identifier.getTableName());
        result.setBranch(CoreOptions.branch(table.options()));
        result.setStorageOptions(normalizeStorage(storageOptions));
        result.setCommitMessageVersion(new CommitMessageSerializer().getVersion());
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
                // rather than copying thousands of built-in settings into each writer descriptor.
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

    static Map<String, String> normalizeStorage(Map<String, String> original) {
        Map<String, String> result = new HashMap<>(original);
        String[][] aliases = {
                {"AWS_ACCESS_KEY", "s3.access-key"}, {"AWS_SECRET_KEY", "s3.secret-key"},
                {"AWS_TOKEN", "s3.session.token"}, {"AWS_ENDPOINT", "s3.endpoint"},
                {"AWS_REGION", "s3.region"}, {"use_path_style", "s3.path-style-access"},
                {"fs.s3a.access.key", "s3.access-key"}, {"fs.s3a.secret.key", "s3.secret-key"},
                {"fs.s3a.endpoint.region", "s3.region"}
        };
        for (String[] alias : aliases) {
            if (original.containsKey(alias[0])) {
                result.putIfAbsent(alias[1], original.get(alias[0]));
            }
        }
        return result;
    }
}
