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

import org.apache.doris.connector.lance.LanceMetadataMetrics.Operation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.ReadOptions;

import java.util.Collections;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

/**
 * Reads the Lance datasets that the file table functions name, as {@code s3("uri" = ..., "format" = "lance")}:
 * the connector {@link LanceConnectorProvider#createFileFormatConnector} builds. It belongs to no catalog and keeps
 * nothing from one statement to the next. Each read opens its dataset with the storage its table function carries,
 * and with an allocator and a Lance Session of its own: table functions bring credentials of their own, and a
 * shared Session would keep an object store for each set of them.
 *
 * <p>Building it touches no {@code org.lance} class, as for the catalog connector: the engine builds it when a
 * statement first names the format.
 */
final class LanceFileFormatConnector implements Connector {
    private final LongSupplier arrowMemoryLimitBytes;

    LanceFileFormatConnector(ConnectorContext context) {
        this(() -> LanceConf.arrowMemoryLimitBytes(context));
    }

    /** @param arrowMemoryLimitBytes the Arrow memory one read may use, from {@code lance.conf} */
    LanceFileFormatConnector(LongSupplier arrowMemoryLimitBytes) {
        this.arrowMemoryLimitBytes = arrowMemoryLimitBytes;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new LanceFileMetadata(this);
    }

    /** Built per call because it holds no state. */
    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return LanceScanPlanProvider.forFiles(this);
    }

    /**
     * Reads the version, schema and fragments of the dataset at {@code uri}, at the newest version when
     * {@code version} is empty. Pins the plugin classloader for the read: lance-core resolves classes by name, and
     * the engine does not pin it for metadata calls.
     */
    LanceTableMetadata read(String uri, Map<String, String> storageOptions, OptionalLong version) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(LanceFileFormatConnector.class.getClassLoader());
        ReadOptions.Builder options = LanceCatalogClient.ownSessionOptions(storageOptions);
        if (version.isPresent()) {
            options.setVersion(version.getAsLong());
        }
        try (LanceMetadataMetrics.Timer timer = new LanceMetadataMetrics().start(Operation.METADATA_READ);
                BufferAllocator allocator = new RootAllocator(arrowMemoryLimitBytes.getAsLong());
                Dataset dataset = Dataset.open().allocator(allocator).uri(uri).readOptions(options.build())
                        .build()) {
            return LanceCatalogClient.readMetadata(dataset, new LanceTableAccess(uri, storageOptions, false), timer);
        } catch (LinkageError e) {
            throw new DorisConnectorException("Failed to load the Lance JNI library. FE loads it from"
                    + " lance-core in plugins/connector/lance/lib: " + e, e);
        } catch (Exception e) {
            // The URI is masked along with the credentials: a provider error may echo either.
            throw LanceErrorMessages.failure("Failed to load Lance metadata from the dataset at the uri", e, uri,
                    storageOptions, Collections.emptyList());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
