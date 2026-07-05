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

package org.apache.doris.connector.iceberg.dlf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the one piece of pure logic in the ported {@link DLFCatalog}: the OSS endpoint rewrite. The
 * live FileIO / metastore-client construction is exercised only at the P6.6 docker plugin-zip gate.
 */
public class DLFCatalogTest {

    @Test
    public void toS3CompatibleEndpointRewritesOssToS3OssAndAddsScheme() {
        // WHY: legacy DLFCatalog.initializeFileIO rewrites the native OSS endpoint oss-<region> to its
        // S3-compatible form s3.oss-<region> (S3FileIO speaks the S3 protocol) and prepends http:// when no
        // scheme is present. MUTATION: skipping the replace, or not adding the scheme, -> red.
        Assertions.assertEquals("http://s3.oss-cn-hangzhou.aliyuncs.com",
                DLFCatalog.toS3CompatibleEndpoint("oss-cn-hangzhou.aliyuncs.com", "cn-hangzhou"));
    }

    @Test
    public void toS3CompatibleEndpointPreservesExistingScheme() {
        // WHY: when the endpoint already carries a scheme only the host is rewritten; the scheme is preserved
        // (the contains("://") guard must not double-prefix). MUTATION: unconditional http:// prefix -> red.
        Assertions.assertEquals("https://s3.oss-cn-beijing.aliyuncs.com",
                DLFCatalog.toS3CompatibleEndpoint("https://oss-cn-beijing.aliyuncs.com", "cn-beijing"));
    }
}
