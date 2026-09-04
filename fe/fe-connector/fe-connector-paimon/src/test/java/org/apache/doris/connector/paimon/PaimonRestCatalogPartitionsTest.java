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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class PaimonRestCatalogPartitionsTest {
    private static final Identifier TABLE = Identifier.create("db", "table");

    @Test
    void serverPartitionResultRemainsAuthoritative() throws Exception {
        List<Partition> serverPartitions = Collections.singletonList(null);
        List<Partition> manifestPartitions = Collections.emptyList();
        AtomicBoolean fallbackCalled = new AtomicBoolean();

        List<Partition> result = PaimonRestCatalogPartitions.listPartitions(
                identifier -> serverPartitions, TABLE, () -> {
                    fallbackCalled.set(true);
                    return manifestPartitions;
                });

        Assertions.assertSame(serverPartitions, result);
        Assertions.assertFalse(fallbackCalled.get());
    }

    @Test
    void unsupportedEndpointUsesTheSuppliedEffectiveTableFallback() throws Exception {
        List<Partition> manifestPartitions = Collections.singletonList(null);

        List<Partition> result = PaimonRestCatalogPartitions.listPartitions(
                identifier -> {
                    throw new NotImplementedException("not implemented");
                },
                TABLE,
                () -> manifestPartitions);

        Assertions.assertSame(manifestPartitions, result);
    }
}
