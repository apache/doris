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

package org.apache.doris.cdcclient.source.factory;

import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.mysql.MySqlSourceReader;
import org.apache.doris.cdcclient.source.reader.postgres.PostgresSourceReader;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SourceReader register. */
public final class SourceReaderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SourceReaderFactory.class);
    private static final Map<DataSource, Supplier<SourceReader>> REGISTRY =
            new ConcurrentHashMap<>();

    static {
        register(DataSource.MYSQL, MySqlSourceReader::new);
        register(DataSource.POSTGRES, PostgresSourceReader::new);
    }

    private SourceReaderFactory() {}

    private static void register(DataSource source, Supplier<SourceReader> supplier) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(supplier, "supplier");
        REGISTRY.put(source, supplier);
        LOG.info("Registered SourceReader provider for {}", source);
    }

    public static SourceReader createSourceReader(DataSource source) {
        Supplier<SourceReader> supplier = REGISTRY.get(source);
        if (supplier == null) {
            throw new IllegalArgumentException(
                    "Unsupported SourceReader with datasource : " + source);
        }
        return supplier.get();
    }
}
