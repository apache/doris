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

public class DLFCatalogTest {

    @Test
    public void toS3CompatibleEndpointRewritesOssToS3OssAndAddsScheme() {
        Assertions.assertEquals("http://s3.oss-cn-hangzhou.aliyuncs.com",
                DLFCatalog.toS3CompatibleEndpoint("oss-cn-hangzhou.aliyuncs.com", "cn-hangzhou"));
    }

    @Test
    public void toS3CompatibleEndpointPreservesExistingScheme() {
        Assertions.assertEquals("https://s3.oss-cn-beijing.aliyuncs.com",
                DLFCatalog.toS3CompatibleEndpoint("https://oss-cn-beijing.aliyuncs.com", "cn-beijing"));
    }

    @Test
    public void toS3CompatibleEndpointIsIdempotentForPublicEndpoint() {
        Assertions.assertEquals("https://s3.oss-cn-beijing.aliyuncs.com",
                DLFCatalog.toS3CompatibleEndpoint("https://s3.oss-cn-beijing.aliyuncs.com", "cn-beijing"));
    }

    @Test
    public void toS3CompatibleEndpointIsIdempotentForInternalEndpoint() {
        Assertions.assertEquals("http://s3.oss-cn-hangzhou-internal.aliyuncs.com",
                DLFCatalog.toS3CompatibleEndpoint("s3.oss-cn-hangzhou-internal.aliyuncs.com", "cn-hangzhou"));
    }
}
