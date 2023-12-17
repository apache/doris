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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.external.FederationBackendPolicy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Stopwatch;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FederationBackendPolicyTest {
    @Mocked
    private Env env;

    @Before
    public void setUp() {

        SystemInfoService service = new SystemInfoService();

        for (int i = 0; i < 190; i++) {
            Backend backend = new Backend(Long.valueOf(i), "192.168.1." + i, 9050);
            backend.setAlive(true);
            service.addBackend(backend);
        }
        for (int i = 0; i < 10; i++) {
            Backend backend = new Backend(Long.valueOf(190 + i), "192.168.1." + i, 9051);
            backend.setAlive(true);
            service.addBackend(backend);
        }
        for (int i = 0; i < 10; i++) {
            Backend backend = new Backend(Long.valueOf(200 + i), "192.168.2." + i, 9050);
            backend.setAlive(false);
            service.addBackend(backend);
        }

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

    }

    @Test
    public void testGetNextBe() throws UserException {
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        int backendNum = 200;
        int invokeTimes = 1000000;
        Assertions.assertEquals(policy.numBackends(), backendNum);
        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < invokeTimes; i++) {
            Assertions.assertFalse(policy.getNextBe().getHost().contains("192.168.2."));
        }
        sw.stop();
        System.out.println("Invoke getNextBe() " + invokeTimes
                    + " times cost [" + sw.elapsed(TimeUnit.MILLISECONDS) + "] ms");
    }

    @Test
    public void testGetNextLocalBe() throws UserException {
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        int backendNum = 200;
        int invokeTimes = 1000000;
        Assertions.assertEquals(policy.numBackends(), backendNum);
        List<String> localHosts = Arrays.asList("192.168.1.0", "192.168.1.1", "192.168.1.2");
        TScanRangeLocations scanRangeLocations = getScanRangeLocations("path1", 0, 100);
        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < invokeTimes; i++) {
            Assertions.assertTrue(localHosts.contains(policy.getNextLocalBe(localHosts, scanRangeLocations).getHost()));
        }
        sw.stop();
        System.out.println("Invoke getNextLocalBe() " + invokeTimes
                    + " times cost [" + sw.elapsed(TimeUnit.MILLISECONDS) + "] ms");
    }

    @Test
    public void testConsistentHash() throws UserException {
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        int backendNum = 200;
        Assertions.assertEquals(policy.numBackends(), backendNum);

        TScanRangeLocations scanRangeLocations = getScanRangeLocations("path1", 0, 100);
        Assertions.assertEquals(39, policy.getNextConsistentBe(scanRangeLocations).getId());

        scanRangeLocations = getScanRangeLocations("path2", 0, 100);
        Assertions.assertEquals(78, policy.getNextConsistentBe(scanRangeLocations).getId());
    }

    private TScanRangeLocations getScanRangeLocations(String path, long startOffset, long size) {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);
        scanRange.getExtScanRange().getFileScanRange().addToRanges(createRangeDesc(path, startOffset, size));
        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);
        return locations;
    }

    private TFileRangeDesc createRangeDesc(String path, long startOffset, long size) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setPath(path);
        rangeDesc.setStartOffset(startOffset);
        rangeDesc.setSize(size);
        return rangeDesc;
    }
}
