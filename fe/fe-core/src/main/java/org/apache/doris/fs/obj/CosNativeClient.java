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

import org.apache.doris.datasource.property.storage.COSProperties;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.region.Region;

/**
 * Manages the lifecycle of a Tencent Cloud COS native client.
 * Thread-unsafe; create per-operation and close via try-with-resources.
 */
public class CosNativeClient implements AutoCloseable {
    private final COSClient cosClient;

    public CosNativeClient(COSProperties props) {
        BasicCOSCredentials cred = new BasicCOSCredentials(props.getAccessKey(), props.getSecretKey());
        ClientConfig config = new ClientConfig();
        config.setRegion(new Region(props.getRegion()));
        config.setHttpProtocol(HttpProtocol.http);
        this.cosClient = new COSClient(cred, config);
    }

    public COSClient get() {
        return cosClient;
    }

    @Override
    public void close() {
        cosClient.shutdown();
    }
}
