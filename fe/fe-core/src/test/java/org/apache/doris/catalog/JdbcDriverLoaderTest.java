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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class JdbcDriverLoaderTest {
    private static final String DRIVER_BYTES = "doris-jdbc-driver";
    private static final String DRIVER_CHECKSUM = "ef2b3218a863c98bc78fdf447331b206";

    @Test
    public void testValidateDriverChecksumRejectsMismatch() throws Exception {
        runWithChecksumEnabled(() -> runWithDriverUrl(driverUrl -> {
            DdlException exception = Assert.assertThrows(DdlException.class,
                    () -> JdbcDriverLoader.validateDriverChecksum(driverUrl, "bad-checksum"));
            Assert.assertTrue(exception.getMessage().contains("does not match the computed checksum"));
        }));
    }

    @Test
    public void testRegisterDriverValidatesChecksumBeforeLoadingClass() throws Exception {
        runWithChecksumEnabled(() -> runWithDriverUrl(driverUrl -> {
            IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                    () -> JdbcDriverLoader.registerDriver(driverUrl,
                            "org.apache.doris.catalog.NotExistingDriver", "bad-checksum",
                            getClass().getClassLoader()));
            Assert.assertTrue(exception.getMessage().contains("does not match the computed checksum"));
        }));
    }

    @Test
    public void testValidateDriverChecksumReturnsComputedChecksum() throws Exception {
        runWithChecksumEnabled(() -> runWithDriverUrl(driverUrl -> {
            Assert.assertEquals(DRIVER_CHECKSUM, JdbcDriverLoader.validateDriverChecksum(driverUrl, DRIVER_CHECKSUM));
            Assert.assertEquals(DRIVER_CHECKSUM, JdbcDriverLoader.validateDriverChecksum(driverUrl, ""));
        }));
    }

    private void runWithChecksumEnabled(ThrowingRunnable runnable) throws Exception {
        synchronized (FeConstants.class) {
            boolean oldRunningUnitTest = FeConstants.runningUnitTest;
            FeConstants.runningUnitTest = false;
            try {
                runnable.run();
            } finally {
                FeConstants.runningUnitTest = oldRunningUnitTest;
            }
        }
    }

    private void runWithDriverUrl(ThrowingConsumer<String> consumer) throws Exception {
        Path driverPath = Files.createTempFile("doris-jdbc-driver", ".jar");
        try {
            Files.write(driverPath, DRIVER_BYTES.getBytes(StandardCharsets.UTF_8));
            consumer.accept(driverPath.toUri().toString());
        } finally {
            Files.deleteIfExists(driverPath);
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }
}
