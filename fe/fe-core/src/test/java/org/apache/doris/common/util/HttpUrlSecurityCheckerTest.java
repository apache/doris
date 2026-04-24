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

package org.apache.doris.common.util;

import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpUrlSecurityCheckerTest {
    @Test
    public void testRejectUnsafeHttpTargetsByDefault() {
        Assertions.assertThrows(UserException.class,
                () -> HttpUrlSecurityChecker.validate("http://127.0.0.1/data", new String[] {}));
        Assertions.assertThrows(UserException.class,
                () -> HttpUrlSecurityChecker.validate("http://[::1]/data", new String[] {}));
        Assertions.assertThrows(UserException.class,
                () -> HttpUrlSecurityChecker.validate("http://[::127.0.0.1]/data", new String[] {}));
        Assertions.assertThrows(UserException.class,
                () -> HttpUrlSecurityChecker.validate("http://169.254.169.254/latest/meta-data", new String[] {}));
        Assertions.assertThrows(UserException.class,
                () -> HttpUrlSecurityChecker.validate("http://10.0.0.1/data", new String[] {}));
        Assertions.assertThrows(UserException.class,
                () -> HttpUrlSecurityChecker.validate("file:///tmp/data", new String[] {}));
    }

    @Test
    public void testAllowExplicitPrivateEndpointAllowlist() throws UserException {
        HttpUrlSecurityChecker.validate("http://127.0.0.1/data", new String[] {"127.0.0.1/32"});
        HttpUrlSecurityChecker.validate("http://10.1.2.3/data", new String[] {"10.0.0.0/8"});
        HttpUrlSecurityChecker.validate("http://localhost/data", new String[] {"localhost"});
    }

    @Test
    public void testAllowPublicIpTargets() throws UserException {
        HttpUrlSecurityChecker.validate("https://93.184.216.34/index.html", new String[] {});
        HttpUrlSecurityChecker.validate("https://[2606:2800:220:1:248:1893:25c8:1946]/", new String[] {});
    }
}
