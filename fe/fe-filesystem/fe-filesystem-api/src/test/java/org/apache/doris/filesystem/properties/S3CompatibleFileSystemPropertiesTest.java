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

package org.apache.doris.filesystem.properties;

import org.apache.doris.filesystem.FileSystemType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class S3CompatibleFileSystemPropertiesTest {

    private static S3CompatibleFileSystemProperties withUsePathStyle(String usePathStyle) {
        return new S3CompatibleFileSystemProperties() {
            @Override
            public String getEndpoint() {
                return "";
            }

            @Override
            public String getRegion() {
                return "";
            }

            @Override
            public String getAccessKey() {
                return "";
            }

            @Override
            public String getSecretKey() {
                return "";
            }

            @Override
            public String getSessionToken() {
                return "";
            }

            @Override
            public String getRoleArn() {
                return "";
            }

            @Override
            public String getExternalId() {
                return "";
            }

            @Override
            public String getBucket() {
                return "";
            }

            @Override
            public String getRootPath() {
                return "";
            }

            @Override
            public String getMaxConnections() {
                return "";
            }

            @Override
            public String getRequestTimeoutMs() {
                return "";
            }

            @Override
            public String getConnectionTimeoutMs() {
                return "";
            }

            @Override
            public String getUsePathStyle() {
                return usePathStyle;
            }

            @Override
            public Set<String> getSupportedSchemes() {
                return Set.of("s3", "s3a");
            }

            @Override
            public String providerName() {
                return "TEST";
            }

            @Override
            public StorageKind kind() {
                return StorageKind.OBJECT_STORAGE;
            }

            @Override
            public FileSystemType type() {
                return FileSystemType.OFS;
            }

            @Override
            public Map<String, String> rawProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> matchedProperties() {
                return Collections.emptyMap();
            }
        };
    }

    @Test
    public void testIsUsePathStyleParsesValidValues() {
        Assertions.assertTrue(withUsePathStyle("true").isUsePathStyle());
        Assertions.assertTrue(withUsePathStyle("TRUE").isUsePathStyle());
        Assertions.assertTrue(withUsePathStyle(" true ").isUsePathStyle());
        Assertions.assertFalse(withUsePathStyle("false").isUsePathStyle());
        Assertions.assertFalse(withUsePathStyle("False").isUsePathStyle());
    }

    @Test
    public void testIsUsePathStyleTreatsBlankAsFalse() {
        Assertions.assertFalse(withUsePathStyle(null).isUsePathStyle());
        Assertions.assertFalse(withUsePathStyle("").isUsePathStyle());
        Assertions.assertFalse(withUsePathStyle("   ").isUsePathStyle());
    }

    @Test
    public void testIsUsePathStyleRejectsInvalidValues() {
        for (String invalid : new String[] {"ture", "1", "yes", "on"}) {
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> withUsePathStyle(invalid).isUsePathStyle());
            Assertions.assertTrue(e.getMessage().contains(invalid));
        }
    }

    @Test
    public void testHasInvalidUsePathStyle() {
        Assertions.assertFalse(withUsePathStyle("true").hasInvalidUsePathStyle());
        Assertions.assertFalse(withUsePathStyle(null).hasInvalidUsePathStyle());
        Assertions.assertTrue(withUsePathStyle("ture").hasInvalidUsePathStyle());
    }
}
