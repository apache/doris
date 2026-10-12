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

package org.apache.doris.filesystem.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EndpointHostsTest {

    @Test
    void hostOf_stripsSchemePathAndUserInfo() {
        Assertions.assertEquals("bucket", EndpointHosts.hostOf("s3://bucket/archive/AMAZONAWS.COM/f"));
        Assertions.assertEquals("bucket.s3.us-east-1.amazonaws.com",
                EndpointHosts.hostOf("https://bucket.s3.us-east-1.amazonaws.com/key"));
        Assertions.assertEquals("host", EndpointHosts.hostOf("oss://user@host/key"));
        Assertions.assertEquals("bucket", EndpointHosts.hostOf("oss://user:pw@bucket/key"));
    }

    @Test
    void hostOf_treatsASchemeLessValueAsABareHost() {
        Assertions.assertEquals("cn-beijing.oss-dls.aliyuncs.com",
                EndpointHosts.hostOf("cn-beijing.oss-dls.aliyuncs.com"));
        Assertions.assertEquals("CN-BEIJING.OSS-DLS.ALIYUNCS.COM:443",
                EndpointHosts.hostOf("CN-BEIJING.OSS-DLS.ALIYUNCS.COM:443"));
    }

    @Test
    void hostOf_survivesValuesThatDoNotParseAsAUri() {
        // A space in the key makes java.net.URI throw; the host must still be extracted.
        Assertions.assertEquals("bucket",
                EndpointHosts.hostOf("oss://bucket/my dir/oss-dls.aliyuncs.com/file.parquet"));
        Assertions.assertEquals("", EndpointHosts.hostOf(null));
    }
}
