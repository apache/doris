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

package org.apache.doris.paimon.s3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class PaimonS3ShadeTest {

    private static final Path CLASSES = Path.of("target", "classes");

    @Test
    public void keepsEmbeddedRuntimeWithoutOuterMultiReleaseOverlay() {
        Assertions.assertTrue(Files.exists(CLASSES.resolve("org/apache/paimon/s3/S3Loader.class")));
        Assertions.assertTrue(Files.exists(CLASSES.resolve(
                "META-INF/services/org.apache.paimon.fs.FileIOLoader")));
        Assertions.assertTrue(Files.exists(CLASSES.resolve(
                "paimon-plugin-s3/org/bouncycastle/jce/provider/BouncyCastleProvider.class")));
        Assertions.assertFalse(Files.exists(CLASSES.resolve("META-INF/versions")));
    }
}
