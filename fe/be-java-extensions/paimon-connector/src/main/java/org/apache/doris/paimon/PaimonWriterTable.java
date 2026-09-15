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

import org.apache.doris.thrift.TPaimonCatalogEnvironment;
import org.apache.doris.thrift.TPaimonTableDescriptor;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogFactory;
import org.apache.paimon.rest.RESTCatalogLoader;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;

import java.io.IOException;

/** Reconstructs the FE-selected table without serializing JVM objects or reloading its schema. */
final class PaimonWriterTable {
    private PaimonWriterTable() {
    }

    static FileStoreTable create(TPaimonTableDescriptor descriptor) throws IOException {
        Configuration hadoopConfig = new Configuration(false);
        descriptor.getHadoopConfig().forEach(hadoopConfig::set);
        CatalogContext context = CatalogContext.create(
                Options.fromMap(descriptor.getCatalogOptions()), hadoopConfig);
        Path path = new Path(descriptor.getRootPath());
        TableSchema schema = TableSchema.fromJson(descriptor.getSchemaJson());
        TPaimonCatalogEnvironment catalog = descriptor.getCatalogEnvironment();
        CatalogEnvironment environment;
        FileIO fileIO;
        if (catalog == null) {
            environment = new CatalogEnvironment(null, null, null, null, null, context, false, false);
            fileIO = FileIO.get(path, context);
        } else {
            Identifier identifier = new Identifier(catalog.getDatabaseName(), catalog.getObjectName());
            CatalogLoader loader = null;
            if (catalog.isSnapshotLoader()) {
                // FE already resolved the REST configuration. Preserve the SDK loader's behavior
                // instead of fetching /config again on every snapshot lookup.
                loader = RESTCatalogFactory.IDENTIFIER.equals(context.options().get(CatalogOptions.METASTORE))
                        ? new RESTCatalogLoader(context) : () -> CatalogFactory.createCatalog(context);
            }
            // BE writes files and aborts uncommitted messages; FE owns snapshot commits and catalog locks.
            environment = new CatalogEnvironment(identifier, catalog.getUuid(), loader,
                    null, null, context, catalog.isVersionManagement(), false);
            fileIO = catalog.isRestTokenEnabled()
                    ? new RESTTokenFileIO(context, null, identifier, path) : FileIO.get(path, context);
        }
        // Do not call copy/options-based factories: time-travel options can reload a different schema.
        return schema.primaryKeys().isEmpty()
                ? new AppendOnlyFileStoreTable(fileIO, path, schema, environment)
                : new PrimaryKeyFileStoreTable(fileIO, path, schema, environment);
    }
}
