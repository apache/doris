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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.spi.ConnectorContractValidator;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Task 6 (write-capability unification, P2): the per-connector expected-set assertion (the pragmatic
 * "declaration == implementation" check for the write-capability invariant the removed
 * {@code ConnectorContractValidator} #1 runtime probe is NOT safe to make) plus the structural contract
 * validator, exercised against a real {@link MaxComputeDorisConnector}. MaxCompute is the one connector that
 * exercises the {@code requiresPartitionLocalSort} triad (local-sort implies parallel write AND full-schema
 * write order) end to end, so this doubles as a positive control for {@link ConnectorContractValidator}'s
 * invariant #3. Properties are the same offline-safe minimal AK/SK config {@code testConnection}-adjacent
 * tests already use ({@code MaxComputeConnectorProviderTest#connectivityProps}); {@code getWritePlanProvider()}
 * only builds local ODPS client/settings objects (no network) at this property shape.
 */
public class MaxComputeConnectorContractTest {

    private static Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put(MCCatalogProperties.PROJECT, "mc_project");
        props.put(MCCatalogProperties.ENDPOINT,
                "http://service.cn-beijing.maxcompute.aliyun-inc.com/api");
        props.put(MCCatalogProperties.ACCESS_KEY, "access_key");
        props.put(MCCatalogProperties.SECRET_KEY, "secret_key");
        return props;
    }

    @Test
    public void declaredWriteCapabilitiesMatchAndPassContractValidator() {
        MaxComputeDorisConnector connector = new MaxComputeDorisConnector(validProps(), null);

        ConnectorWritePlanProvider writeProvider = connector.getWritePlanProvider();
        Assertions.assertNotNull(writeProvider, "MaxCompute connector must expose a write plan provider");
        Assertions.assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE),
                writeProvider.supportedOperations());
        Assertions.assertFalse(writeProvider.supportsWriteBranch(),
                "MaxCompute does not support writing into a named table branch");
        Assertions.assertTrue(writeProvider.requiresParallelWrite());
        Assertions.assertTrue(writeProvider.requiresFullSchemaWriteOrder());
        Assertions.assertTrue(writeProvider.requiresPartitionLocalSort());
        Assertions.assertFalse(writeProvider.requiresMaterializeStaticPartitionValues());

        ConnectorContractValidator.validate(connector, "max_compute");
    }
}
