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


package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorFileSource;
import org.apache.doris.connector.spi.ConnectorFileSourceOps;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Resolves a {@code format = "lance"} file table function to the dataset at its URI, and serves that dataset's
 * columns. Created per statement; the metadata read when the handle is resolved is shared with the scan through
 * {@link LanceStatementScope}.
 */
final class LanceFileMetadata implements ConnectorMetadata, ConnectorFileSourceOps {
    /** Informational format name the engine records with the schema, as for a catalog table. */
    private static final String TABLE_FORMAT_TYPE = "LANCE";

    private final LanceFileFormatConnector connector;

    LanceFileMetadata(LanceFileFormatConnector connector) {
        this.connector = connector;
    }

    /**
     * Opens the dataset at the table function's URI with its storage and pins the newest version, which the
     * columns and every range of the scan then read. The storage options are the ones a filesystem catalog would
     * build from the same properties.
     */
    @Override
    public ConnectorTableHandle getFileTableHandle(ConnectorSession session, ConnectorFileSource source) {
        String uri = source.getLocation();
        Map<String, String> storageOptions;
        try {
            storageOptions = LanceStorageOptions.fromDorisStorageProperties(uri, source.getStorageProperties());
        } catch (IllegalArgumentException e) {
            throw new DorisConnectorException(e.getMessage(), e);
        }
        LanceTableMetadata metadata = connector.read(uri, storageOptions, OptionalLong.empty());
        LanceFileTableHandle handle = new LanceFileTableHandle(uri, metadata.getVersion(), storageOptions,
                source.getBackendStorageProperties());
        LanceStatementScope.sharedFileMetadata(session, handle, () -> metadata);
        return handle;
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
        LanceFileTableHandle fileHandle = (LanceFileTableHandle) handle;
        return new ConnectorTableSchema(fileHandle.getUri(),
                LanceTypeMapping.toColumns(metadata(session, fileHandle).getSchema()), TABLE_FORMAT_TYPE,
                Collections.emptyMap());
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorSession session,
            ConnectorTableHandle handle) {
        List<Field> fields = metadata(session, (LanceFileTableHandle) handle).getSchema().getFields();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (Field field : fields) {
            handles.put(field.getName(), new NamedColumnHandle(field.getName()));
        }
        return handles;
    }

    /** A conjunct holding a CAST stays in Doris, as for a catalog table: see {@link LanceConnectorMetadata}. */
    @Override
    public boolean supportsCastPredicatePushdown(ConnectorSession session) {
        return false;
    }

    /** The metadata of the handle's pinned version, read again only when the statement keeps no memo. */
    static LanceTableMetadata metadata(ConnectorSession session, LanceFileTableHandle handle,
            LanceFileFormatConnector connector) {
        return LanceStatementScope.sharedFileMetadata(session, handle, () -> connector.read(handle.getUri(),
                handle.getStorageOptions(), OptionalLong.of(handle.getVersion())));
    }

    private LanceTableMetadata metadata(ConnectorSession session, LanceFileTableHandle handle) {
        return metadata(session, handle, connector);
    }
}
