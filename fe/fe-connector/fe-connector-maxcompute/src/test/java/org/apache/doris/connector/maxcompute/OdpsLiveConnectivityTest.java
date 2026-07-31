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

import com.aliyun.odps.Odps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * R-004 part 2 — live ODPS connectivity smoke (credentials required; <b>user-run</b>).
 *
 * <p>Complements {@link OdpsClassloaderIsolationTest} (part 1, no-creds isolation correctness): this
 * one confirms the client built by {@link MCConnectorClientFactory} can actually reach a real
 * MaxCompute endpoint and authenticate. It is <b>skipped</b> unless all four environment variables
 * below are set, so it is inert in CI and never commits credentials. The cutover is declared complete
 * only after a maintainer reports this green.</p>
 *
 * <pre>
 *   MC_ENDPOINT=https://service.&lt;region&gt;.maxcompute.aliyun.com/api \
 *   MC_PROJECT=&lt;project&gt; MC_ACCESS_KEY=&lt;ak&gt; MC_SECRET_KEY=&lt;sk&gt; \
 *   mvn -pl :fe-connector-maxcompute test -Dtest=OdpsLiveConnectivityTest
 * </pre>
 */
public class OdpsLiveConnectivityTest {

    @Test
    public void liveMetadataRoundTrip() {
        String endpoint = System.getenv("MC_ENDPOINT");
        String project = System.getenv("MC_PROJECT");
        String accessKey = System.getenv("MC_ACCESS_KEY");
        String secretKey = System.getenv("MC_SECRET_KEY");
        Assumptions.assumeTrue(
                endpoint != null && project != null && accessKey != null && secretKey != null,
                "skipped: set MC_ENDPOINT / MC_PROJECT / MC_ACCESS_KEY / MC_SECRET_KEY to run live");

        Map<String, String> props = new HashMap<>();
        props.put(MCConnectorProperties.ACCESS_KEY, accessKey);
        props.put(MCConnectorProperties.SECRET_KEY, secretKey);

        Odps odps = MCConnectorClientFactory.createClient(props);
        odps.setEndpoint(endpoint);
        odps.setDefaultProject(project);

        // One trivial metadata round-trip exercises endpoint + auth end to end.
        Assertions.assertDoesNotThrow(() -> odps.projects().get(project).reload());
    }
}
