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

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.storage.S3Properties;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.http.HttpMethodName;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;

/**
 * Baidu Cloud BOS-specific {@link ObjStorage} implementation.
 *
 * <p>BOS does not support STS AssumeRole. Only presigned PUT URL is supported
 * via the Baidu BOS SDK. All other operations inherit from {@link S3ObjStorage}.
 */
public class BosObjStorage extends S3ObjStorage {
    private static final Logger LOG = LogManager.getLogger(BosObjStorage.class);
    private static final long SESSION_EXPIRE_SECONDS = 3600L;

    public BosObjStorage(S3Properties properties) {
        super(properties);
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        throw new DdlException("STS is not supported for Baidu BOS");
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        BosClientConfiguration config = new BosClientConfiguration();
        config.setCredentials(new DefaultBceCredentials(
                s3Properties.getAccessKey(), s3Properties.getSecretKey()));
        config.setEndpoint(s3Properties.getEndpoint());
        BosClient client = new BosClient(config);
        try {
            URL url = client.generatePresignedUrl(
                    s3Properties.getBucket(), objectKey,
                    (int) SESSION_EXPIRE_SECONDS, HttpMethodName.PUT);
            LOG.info("Generated BOS presigned URL for key={}", objectKey);
            return url.toString();
        } finally {
            client.shutdown();
        }
    }
}
