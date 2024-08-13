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

package org.apache.doris.cloud.storage;

import org.apache.doris.common.DdlException;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.http.HttpMethodName;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;

public class BosRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(BosRemote.class);

    public BosRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        BosClientConfiguration config = new BosClientConfiguration();
        config.setCredentials(new DefaultBceCredentials(obj.getAk(), obj.getSk()));
        config.setEndpoint(obj.getEndpoint());
        BosClient client = new BosClient(config);
        URL url = client.generatePresignedUrl(obj.getBucket(), normalizePrefix(fileName),
                (int) SESSION_EXPIRE_SECOND, HttpMethodName.PUT);
        LOG.info("Bos getPresignedUrl: {}", url);
        return url.toString();
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        throw new DdlException("Get sts token for BOS is unsupported");
    }

    @Override
    public String toString() {
        return "BosRemote{obj=" + obj + '}';
    }
}
