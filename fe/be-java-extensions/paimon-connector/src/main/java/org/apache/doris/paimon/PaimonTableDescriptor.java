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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.utils.JniUtil;
import org.apache.doris.thrift.TPaimonCatalogContext;
import org.apache.doris.thrift.TPaimonTableDesc;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Rebuilds the FE-pinned table without Java object serialization or a catalog schema lookup. */
final class PaimonTableDescriptor {
    private PaimonTableDescriptor() {
    }

    static FileStoreTable resolve(byte[] bytes, Map<String, String> hadoopConfig) throws Exception {
        TPaimonTableDesc descriptor = new TPaimonTableDesc();
        JniUtil.deserializeThrift(new TBinaryProtocol.Factory(), descriptor, bytes);
        if (!descriptor.isSetTablePath() || !descriptor.isSetSchemaJson()
                || !descriptor.isSetDatabaseName() || !descriptor.isSetTableName() || !descriptor.isSetBranch()
                || !descriptor.isSetCatalogContext()) {
            throw new IllegalArgumentException("Incomplete Paimon table descriptor; upgrade the FE");
        }
        TPaimonCatalogContext wire = descriptor.getCatalogContext();
        Configuration configuration = new Configuration();
        if (wire.isSetHadoopConfig()) {
            wire.getHadoopConfig().forEach(configuration::set);
        }
        hadoopConfig.forEach(configuration::set);
        Map<String, String> catalogOptions = wire.isSetOptions() ? wire.getOptions() : Collections.emptyMap();
        CatalogContext catalogContext = CatalogContext.create(Options.fromMap(catalogOptions), configuration);
        CatalogLoader loader = wire.isCatalogLoaderEnabled()
                ? () -> CatalogFactory.createCatalog(catalogContext) : null;
        // Only FE publishes snapshots and owns the catalog commit lock. The BE uses these
        // services to write/prepare files and to abort its own unpublished files.
        CatalogEnvironment environment = new CatalogEnvironment(
                new Identifier(descriptor.getDatabaseName(), descriptor.getTableName(), descriptor.getBranch()),
                wire.getTableUuid(), loader, null, null, catalogContext,
                wire.isSupportsVersionManagement(), wire.isSupportsPartitionModification());
        TableSchema schema = JsonSerdeUtil.fromJson(descriptor.getSchemaJson(), TableSchema.class);
        Map<String, String> tableOptions = new HashMap<>(schema.options());
        tableOptions.put(CoreOptions.BRANCH.key(), descriptor.getBranch());
        tableOptions.put(CoreOptions.PATH.key(), descriptor.getTablePath());
        schema = schema.copy(tableOptions);
        Map<String, String> storageOptions = new HashMap<>(catalogOptions);
        if (descriptor.isSetStorageOptions()) {
            storageOptions.putAll(descriptor.getStorageOptions());
        }
        Path path = new Path(descriptor.getTablePath());
        FileIO fileIO = FileIO.get(path, CatalogContext.create(Options.fromMap(storageOptions), configuration));
        try {
            // Write the pinned target itself. Read-only fallback branches and time-travel
            // options must not trigger another schema lookup during reconstruction.
            return schema.primaryKeys().isEmpty()
                    ? new AppendOnlyFileStoreTable(fileIO, path, schema, environment)
                    : new PrimaryKeyFileStoreTable(fileIO, path, schema, environment);
        } catch (Exception failure) {
            try {
                fileIO.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }
}
