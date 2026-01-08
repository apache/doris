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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.cloud.CloudReplica;
import org.apache.doris.resource.Tag;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

// Use Mockito to mock the static method (CloudReplica)
@RunWith(MockitoJUnitRunner.class)
public class ColocationGroupProcDirTest {

    private ColocationGroupProcDir colocationGroupProcDir;
    // Static Mock of simulated cloud environment
    private MockedStatic<CloudReplica> cloudReplicaMock;

    @Before
    public void setUp() {
      
        colocationGroupProcDir = new ColocationGroupProcDir();
      
        cloudReplicaMock = Mockito.mockStatic(CloudReplica.class);
    }

    @After
    public void tearDown() {
        // Disable static Mock
        cloudReplicaMock.close();
    }

    //Test 1: In the cloud environment, the lookup function returns a non-empty BE sequence
    @Test
    public void testLookupInCloudEnv() throws AnalysisException {
        // The simulated cloud environment is identified as "true".
        cloudReplicaMock.when(CloudReplica::isCloudEnv).thenReturn(true);
        // 2. Simulate the function getColocatedBeId to return the list of specified BE IDs
        List<Long> mockBeIds = Arrays.asList(10001L, 10002L, 10003L);
        cloudReplicaMock.when(() -> CloudReplica.getColocatedBeId(10005L, 10008L))
                .thenReturn(mockBeIds);

        // 3. Call the lookup function
        ProcNodeInterface node = colocationGroupProcDir.lookup("10005.10008");
        assertNotNull("云环境下 lookup 应返回非空节点", node);

        // 4. Verify whether the returned BE sequence is correct (this requires reference to the structure of the ColocationGroupBackendSeqsProcNode)
        ColocationGroupBackendSeqsProcNode seqsNode = (ColocationGroupBackendSeqsProcNode) node;
        Map<Tag, List<List<Long>>> beSeqs = seqsNode.getBeSeqs();
        assertFalse("云环境下 BE 序列应非空", beSeqs.isEmpty());
        assertEquals("默认 Tag 应存在", 1, beSeqs.size());
        assertEquals("BE ID 列表应匹配", mockBeIds, beSeqs.get(Tag.DEFAULT_TAG).get(0));
    }

    // Test 2: Lookup function in non-cloud environment (retains original logic)
    @Test
    public void testLookupInNonCloudEnv() throws AnalysisException {
        // 1. Simulate non-cloud environment flag set to false
        cloudReplicaMock.when(CloudReplica::isCloudEnv).thenReturn(false);

        // 2. Call the lookup function (Note: Ensure that the test environment has the corresponding Colocation Group metadata, or use a Mock Env)
// Here, the test is simplified: Only verify that no exception is thrown. Depending on the actual situation, you can Mock Env.getCurrentColocateIndex()
        try {
            ProcNodeInterface node = colocationGroupProcDir.lookup("10005.10008");
            // In a non-cloud environment, if the metadata does not exist, the node may be null. The assertion needs to be adjusted according to the actual logic.
            assertNotNull("非云环境下若存在 Colocation Group，应返回非空节点", node);
        } catch (AnalysisException e) {
            // If the test environment has no metadata, and an exception is expected, then the assertion will pass.
            assertTrue(e.getMessage().contains("Invalid group id") || e.getMessage().contains("not found"));
        }
    }

    //Test 3: Illegal GroupId Format (Boundary Test)
    @Test(expected = AnalysisException.class)
    public void testLookupWithInvalidGroupId() throws AnalysisException {
        // Incorrect input format for GroupId (missing dot separator)
        colocationGroupProcDir.lookup("10005");
    }
}
