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

package org.apache.doris.cloud.security;

import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link SecurityChecker#checkEndpointForSsrf} is the single primitive the catalog SSRF gate delegates to.
 * The real third-party SDK is operator-supplied and absent in CI, so these tests drive its branches through
 * the {@code doConnect} seam: the SDK signals an intranet target by throwing {@link SecurityException} from
 * inside the connect, a plain connectivity failure is {@link IOException}, and when no checker is installed
 * the whole probe is a no-op.
 */
public class SecurityCheckerTest {

    private enum ConnectBehavior { SUCCESS, SSRF, IO }

    /** A SecurityChecker whose net-hook bracket and connect are recorded, not real. */
    private static final class RecordingChecker extends SecurityChecker {
        private boolean enabled = true;
        private boolean startThrows = false;
        private ConnectBehavior connectBehavior = ConnectBehavior.SUCCESS;
        private final List<String> events = new ArrayList<>();

        @Override
        public String getSafeJdbcUrl(String originJdbcUrl) {
            return originJdbcUrl;
        }

        @Override
        public void startSSRFChecking(String originUri) throws Exception {
            start();
        }

        @Override
        public void startSSRFChecking() throws Exception {
            start();
        }

        private void start() throws Exception {
            events.add("start");
            if (startThrows) {
                throw new Exception("cannot install net-hook");
            }
        }

        @Override
        public void stopSSRFChecking() {
            events.add("stop");
        }

        @Override
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        protected void doConnect(String host, int port, int connectTimeoutMs) throws IOException {
            events.add("connect");
            switch (connectBehavior) {
                case SSRF:
                    throw new SecurityException("simulated SSRFUnsafeConnectionException for " + host);
                case IO:
                    throw new IOException("connection refused");
                default:
                    break;
            }
        }
    }

    @Test
    public void disabledCheckerSkipsProbeEntirely() throws DdlException {
        RecordingChecker checker = new RecordingChecker();
        checker.enabled = false;
        // Intranet-looking host, but a disabled checker must not even connect.
        checker.checkEndpointForSsrf("10.0.0.1", 9083);
        Assertions.assertTrue(checker.events.isEmpty(), "disabled checker must not touch the network");
    }

    @Test
    public void successfulProbeBracketsConnectAndReturns() throws DdlException {
        RecordingChecker checker = new RecordingChecker();
        checker.connectBehavior = ConnectBehavior.SUCCESS;
        checker.checkEndpointForSsrf("public.example.com", 9083);
        Assertions.assertEquals(Arrays.asList("start", "connect", "stop"), checker.events);
    }

    @Test
    public void intranetTargetIsRejectedAsDdlException() {
        RecordingChecker checker = new RecordingChecker();
        checker.connectBehavior = ConnectBehavior.SSRF;
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> checker.checkEndpointForSsrf("10.0.0.1", 9083));
        Assertions.assertTrue(e.getMessage().contains("SSRF policy"), e.getMessage());
        // stop still runs in the finally block even when the connect throws.
        Assertions.assertEquals(Arrays.asList("start", "connect", "stop"), checker.events);
    }

    @Test
    public void unreachableEndpointIsNotAnSsrfRejection() throws DdlException {
        RecordingChecker checker = new RecordingChecker();
        checker.connectBehavior = ConnectBehavior.IO;
        // A refused/timed-out connect is a reachability fact, not an SSRF signal: must not throw.
        checker.checkEndpointForSsrf("public-but-down.example.com", 9083);
        Assertions.assertEquals(Arrays.asList("start", "connect", "stop"), checker.events);
    }

    @Test
    public void failedHookInstallSurfacesAsDdlExceptionWithoutConnecting() {
        RecordingChecker checker = new RecordingChecker();
        checker.startThrows = true;
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> checker.checkEndpointForSsrf("some.host", 9083));
        Assertions.assertTrue(e.getMessage().contains("Failed to initialize SSRF checking"), e.getMessage());
        // No connect attempt, and nothing to stop since the hook was never installed.
        Assertions.assertEquals(Arrays.asList("start"), checker.events);
    }

    @Test
    public void dummyCheckerIsDisabled() {
        Assertions.assertFalse(new DummySecurityChecker().isEnabled());
    }
}
