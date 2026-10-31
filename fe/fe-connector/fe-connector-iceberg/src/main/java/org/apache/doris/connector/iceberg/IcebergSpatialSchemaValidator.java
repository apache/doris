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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.Map;

/** Constraints of the Doris spatial Parquet writer, checked before DDL commits and data writes. */
final class IcebergSpatialSchemaValidator {
    private IcebergSpatialSchemaValidator() {
    }

    /** Returns whether any leaf is spatial so V3/Parquet checks also apply to nested columns. */
    static boolean containsSpatial(Type type) {
        if (type.typeId() == Type.TypeID.GEOMETRY || type.typeId() == Type.TypeID.GEOGRAPHY) {
            return true;
        }
        boolean spatial = false;
        if (type.isNestedType()) {
            for (Types.NestedField field : type.asNestedType().fields()) {
                spatial |= containsSpatial(field.type());
            }
        }
        return spatial;
    }

    static boolean containsSpatial(Schema schema) {
        return schema.columns().stream().anyMatch(field -> containsSpatial(field.type()));
    }

    static void validateTable(int formatVersion, String fileFormat) {
        if (formatVersion < 3) {
            throw new DorisConnectorException("Iceberg spatial columns require format-version >= 3");
        }
        String effectiveFormat = fileFormat == null ? TableProperties.DEFAULT_FILE_FORMAT_DEFAULT : fileFormat;
        if (!"parquet".equalsIgnoreCase(effectiveFormat)) {
            throw new DorisConnectorException("Iceberg spatial columns require Parquet, but the write format is "
                    + effectiveFormat);
        }
    }

    static boolean containsFormatProperty(Map<String, String> properties) {
        return properties.containsKey("write-format")
                || properties.containsKey(TableProperties.DEFAULT_FILE_FORMAT)
                || properties.containsKey(TableProperties.FORMAT_VERSION);
    }

    /** A non-owning adapter only for DDL that can change spatial schema or its required format. */
    static Table forDdl(Table table, boolean validate) {
        // Metadata/read-only tables must retain their own SDK mutation refusal.
        if (!validate || !(table instanceof BaseTable)) {
            return table;
        }
        BaseTable base = (BaseTable) table;
        return new BaseTable(new DdlOperations(base.operations(), base.name()),
                base.name(), base.reporter(), base.labels());
    }

    /** Checks every SDK commit candidate, including a candidate rebuilt after a client CAS retry. */
    private static final class DdlOperations implements TableOperations {
        private final TableOperations delegate;
        private final String tableName;

        private DdlOperations(TableOperations delegate, String tableName) {
            this.delegate = delegate;
            this.tableName = tableName;
        }

        @Override
        public TableMetadata current() {
            return delegate.current();
        }

        @Override
        public TableMetadata refresh() {
            return delegate.refresh();
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updated) {
            // A Parquet preflight may precede another client's ORC change. Validate the SDK's
            // actual candidate rather than the earlier load, without replacing its CAS/retry.
            // REST server-side rebases unseen by the client remain governed by REST requirements.
            if (containsSpatial(updated.schema())) {
                Table candidate = new BaseTable(new StaticTableOperations(
                        updated, delegate.io(), delegate.locationProvider()), tableName);
                validateTable(updated.formatVersion(),
                        IcebergWriterHelper.resolveFileFormatName(candidate, updated.properties()));
            }
            delegate.commit(base, updated);
        }

        @Override
        public FileIO io() {
            return delegate.io();
        }

        @Override
        public EncryptionManager encryption() {
            return delegate.encryption();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return delegate.metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider() {
            return delegate.locationProvider();
        }

        @Override
        public TableOperations temp(TableMetadata metadata) {
            return new DdlOperations(delegate.temp(metadata), tableName);
        }

        @Override
        public long newSnapshotId() {
            return delegate.newSnapshotId();
        }

        @Override
        public boolean requireStrictCleanup() {
            return delegate.requireStrictCleanup();
        }
    }
}
