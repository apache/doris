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

package org.apache.doris.fs.obj;

import org.apache.doris.datasource.property.storage.OSSProperties;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;

/**
 * Manages the lifecycle of an Alibaba Cloud OSS native client.
 * Thread-unsafe; create per-operation and close via try-with-resources.
 */
public class OssNativeClient implements AutoCloseable {
    private final OSS ossClient;

    public OssNativeClient(OSSProperties props) {
        String token = props.getOrigProps().get("s3.session_token");
        if (token != null && !token.isEmpty()) {
            this.ossClient = new OSSClientBuilder()
                    .build(props.getEndpoint(), props.getAccessKey(), props.getSecretKey(), token);
        } else {
            this.ossClient = new OSSClientBuilder()
                    .build(props.getEndpoint(), props.getAccessKey(), props.getSecretKey());
        }
    }

    public OSS get() {
        return ossClient;
    }

    @Override
    public void close() {
        ossClient.shutdown();
    }
}
