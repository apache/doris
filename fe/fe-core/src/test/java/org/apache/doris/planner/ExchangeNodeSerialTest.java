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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanNode;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * Tests that ExchangeNode.toThrift sets is_serial correctly.
 * is_serial is a per-operator property: only UNPARTITIONED (or use_serial_exchange)
 * exchanges should be serial, NOT bucket shuffle exchanges that share a fragment
 * with a serial scan.
 */
public class ExchangeNodeSerialTest {

    private ExchangeNode createExchangeNode(TPartitionType partitionType,
            boolean hasSerialScanNode, boolean useSerialSource) {
        // Use Mockito spy to avoid complex PlanNode constructor dependencies
        ExchangeNode exchange = Mockito.mock(ExchangeNode.class, Mockito.withSettings()
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));

        // Set the fields that toThrift accesses
        exchange.tupleIds = Lists.newArrayList(new TupleId(0));
        exchange.conjuncts = Collections.emptyList();

        // Use reflection to set private partitionType field
        try {
            java.lang.reflect.Field ptField = ExchangeNode.class.getDeclaredField("partitionType");
            ptField.setAccessible(true);
            ptField.set(exchange, partitionType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.hasSerialScanNode()).thenReturn(hasSerialScanNode);
        Mockito.when(fragment.useSerialSource(Mockito.any())).thenReturn(useSerialSource);
        exchange.fragment = fragment;

        return exchange;
    }

    /**
     * BUCKET_SHUFFLE exchange in a pooling fragment with serial scan:
     * isSerialOperator()=false, hasSerialScanNode()=true, useSerialSource()=true
     * → is_serial must be false (scan seriality should not propagate to exchange)
     */
    @Test
    public void testBucketShuffleExchangeNotSerialInPoolingFragment() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        try (MockedStatic<ConnectContext> mocked = Mockito.mockStatic(ConnectContext.class)) {
            mocked.when(ConnectContext::get).thenReturn(ctx);

            ExchangeNode exchange = createExchangeNode(
                    TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, true, true);

            TPlanNode thriftNode = new TPlanNode();
            exchange.toThrift(thriftNode);

            Assertions.assertFalse(thriftNode.isIsSerialOperator(),
                    "BUCKET_SHUFFLE exchange should NOT be serial even when fragment has serial scan");
        }
    }

    /**
     * UNPARTITIONED exchange in a pooling fragment:
     * isSerialOperator()=true (UNPARTITIONED), useSerialSource()=true
     * → is_serial must be true
     */
    @Test
    public void testUnpartitionedExchangeSerialInPoolingFragment() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        try (MockedStatic<ConnectContext> mocked = Mockito.mockStatic(ConnectContext.class)) {
            mocked.when(ConnectContext::get).thenReturn(ctx);

            ExchangeNode exchange = createExchangeNode(
                    TPartitionType.UNPARTITIONED, true, true);

            TPlanNode thriftNode = new TPlanNode();
            exchange.toThrift(thriftNode);

            Assertions.assertTrue(thriftNode.isIsSerialOperator(),
                    "UNPARTITIONED exchange should be serial in pooling fragment");
        }
    }

    /**
     * UNPARTITIONED exchange in a non-pooling fragment:
     * isSerialOperator()=true, useSerialSource()=false
     * → is_serial must be false (not in pooling mode)
     */
    @Test
    public void testUnpartitionedExchangeNotSerialWithoutPooling() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        try (MockedStatic<ConnectContext> mocked = Mockito.mockStatic(ConnectContext.class)) {
            mocked.when(ConnectContext::get).thenReturn(ctx);

            ExchangeNode exchange = createExchangeNode(
                    TPartitionType.UNPARTITIONED, false, false);

            TPlanNode thriftNode = new TPlanNode();
            exchange.toThrift(thriftNode);

            Assertions.assertFalse(thriftNode.isIsSerialOperator(),
                    "UNPARTITIONED exchange should NOT be serial without pooling");
        }
    }

    /**
     * HASH_PARTITIONED exchange in a pooling fragment with serial scan:
     * isSerialOperator()=false (HASH is not serial), useSerialSource()=true
     * → is_serial must be false
     */
    @Test
    public void testHashExchangeNotSerialInPoolingFragment() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        try (MockedStatic<ConnectContext> mocked = Mockito.mockStatic(ConnectContext.class)) {
            mocked.when(ConnectContext::get).thenReturn(ctx);

            ExchangeNode exchange = createExchangeNode(
                    TPartitionType.HASH_PARTITIONED, true, true);

            TPlanNode thriftNode = new TPlanNode();
            exchange.toThrift(thriftNode);

            Assertions.assertFalse(thriftNode.isIsSerialOperator(),
                    "HASH exchange should NOT be serial even when fragment has serial scan");
        }
    }
}
