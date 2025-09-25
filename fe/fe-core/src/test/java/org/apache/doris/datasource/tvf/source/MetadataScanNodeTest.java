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

package org.apache.doris.datasource.tvf.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.MetadataTableValuedFunction;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TScanRangeLocations;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Test for MetadataScanNode, focusing on initedScanRangeLocations logic
 */
@RunWith(MockitoJUnitRunner.class)
public class MetadataScanNodeTest {

    @Mock
    private MetadataTableValuedFunction mockTvf;

    @Mock
    private Backend mockBackend;

    private TupleDescriptor tupleDescriptor;
    private PlanNodeId planNodeId;

    @Before
    public void setUp() {
        tupleDescriptor = new TupleDescriptor(new TupleId(1));
        planNodeId = new PlanNodeId(1);
    }

    /**
     * Test that initedScanRangeLocations is false initially
     */
    @Test
    public void testInitedScanRangeLocationsInitialState() throws Exception {
        MetadataScanNode scanNode = new MetadataScanNode(planNodeId, tupleDescriptor, mockTvf);

        // Use reflection to access the private field
        Field field = MetadataScanNode.class.getDeclaredField("initedScanRangeLocations");
        field.setAccessible(true);
        boolean initedValue = (Boolean) field.get(scanNode);

        Assert.assertFalse("initedScanRangeLocations should be false initially", initedValue);
    }

    /**
     * Test that initedScanRangeLocations becomes true after first call to
     * getScanRangeLocations
     */
    @Test
    public void testInitedScanRangeLocationsAfterFirstCall() throws Exception {
        // Setup mocks
        Mockito.when(mockBackend.getId()).thenReturn(1L);
        Mockito.when(mockBackend.getHost()).thenReturn("localhost");
        Mockito.when(mockBackend.getBePort()).thenReturn(9060);

        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.BACKENDS);
        // Not setting serializedSplits so isSetSerializedSplits() returns false

        Mockito.when(mockTvf.getMetaScanRange(Mockito.anyList())).thenReturn(metaScanRange);

        MetadataScanNode scanNode = new MetadataScanNode(planNodeId, tupleDescriptor, mockTvf);

        // Mock the backend policy using reflection
        mockBackendPolicy(scanNode);

        // Use reflection to check initial state
        Field field = MetadataScanNode.class.getDeclaredField("initedScanRangeLocations");
        field.setAccessible(true);

        Assert.assertFalse("initedScanRangeLocations should be false initially",
                (Boolean) field.get(scanNode));

        // Call getScanRangeLocations for the first time
        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(1000);

        // Check that initedScanRangeLocations is now true
        Assert.assertTrue("initedScanRangeLocations should be true after first call",
                (Boolean) field.get(scanNode));

        // Verify we got some scan range locations
        Assert.assertNotNull("Scan range locations should not be null", locations);
    }

    /**
     * Test that multiple calls to getScanRangeLocations don't reinitialize
     */
    @Test
    public void testMultipleCallsToGetScanRangeLocations() throws Exception {
        // Setup mocks
        Mockito.when(mockBackend.getId()).thenReturn(1L);
        Mockito.when(mockBackend.getHost()).thenReturn("localhost");
        Mockito.when(mockBackend.getBePort()).thenReturn(9060);

        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.BACKENDS);

        Mockito.when(mockTvf.getMetaScanRange(Mockito.anyList())).thenReturn(metaScanRange);

        MetadataScanNode scanNode = new MetadataScanNode(planNodeId, tupleDescriptor, mockTvf);
        mockBackendPolicy(scanNode);

        // Call getScanRangeLocations multiple times
        List<TScanRangeLocations> locations1 = scanNode.getScanRangeLocations(1000);
        List<TScanRangeLocations> locations2 = scanNode.getScanRangeLocations(1000);
        List<TScanRangeLocations> locations3 = scanNode.getScanRangeLocations(1000);

        // All calls should return the same cached result
        Assert.assertEquals("Multiple calls should return same result",
                locations1.size(), locations2.size());
        Assert.assertEquals("Multiple calls should return same result",
                locations1.size(), locations3.size());

        // Verify that getMetaScanRange was only called once (during first call)
        Mockito.verify(mockTvf, Mockito.times(1)).getMetaScanRange(Mockito.anyList());
    }

    /**
     * Helper method to mock the backend policy using reflection
     */
    private void mockBackendPolicy(MetadataScanNode scanNode) throws Exception {
        // Create a mock backend policy
        Object mockBackendPolicy = Mockito
                .mock(Class.forName("org.apache.doris.datasource.FederationBackendPolicy"));

        // Set up the mock to return our mock backend
        Mockito.when(mockBackendPolicy.getClass().getMethod("getNextBe").invoke(mockBackendPolicy))
                .thenReturn(mockBackend);

        // Use reflection to set the backendPolicy field
        Field backendPolicyField = scanNode.getClass().getSuperclass().getDeclaredField("backendPolicy");
        backendPolicyField.setAccessible(true);
        backendPolicyField.set(scanNode, mockBackendPolicy);
    }
}
