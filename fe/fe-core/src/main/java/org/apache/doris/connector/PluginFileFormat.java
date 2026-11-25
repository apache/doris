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


package org.apache.doris.connector;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * The reader of one format that a connector plugin serves to the file table functions
 * ({@link ConnectorProvider#getFileFormats()}): the connector the plugin built for it, and the catalog id the engine
 * gives that connector. The connector belongs to no catalog, so its id is a negative number that no catalog has,
 * which keeps its per-statement state, keyed by catalog id, apart from every catalog's.
 */
public final class PluginFileFormat {
    private final String format;
    private final Connector connector;
    private final long catalogId;

    PluginFileFormat(String format, Connector connector, long catalogId) {
        this.format = Objects.requireNonNull(format, "format");
        this.connector = Objects.requireNonNull(connector, "connector");
        this.catalogId = catalogId;
    }

    /** The format name, in lower case. */
    public String getFormat() {
        return format;
    }

    public Connector getConnector() {
        return connector;
    }

    public long getCatalogId() {
        return catalogId;
    }

    /** A session of the current statement for this format's connector, named after the format. */
    public ConnectorSession buildSession() {
        ConnectContext ctx = ConnectContext.get();
        ConnectorSessionBuilder builder = ctx != null ? ConnectorSessionBuilder.from(ctx)
                : ConnectorSessionBuilder.create();
        return builder.withCatalogId(catalogId).withCatalogName(format).build();
    }
}
