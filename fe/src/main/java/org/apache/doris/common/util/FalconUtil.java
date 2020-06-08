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

package org.apache.doris.common.util;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class FalconUtil {

    private static final Logger LOG = LogManager.getLogger(FalconUtil.class);

    private static String falconUrl = "http://127.0.0.1:1988/v1/push";

    public FalconUtil() {

    }

    public static void send(String service, String metric, int seconds, int period, double availability, String tags) {
        HttpClient client = new HttpClient();
        PostMethod post = new PostMethod(falconUrl);
        String msg = String.format("[{ \"endpoint\":\"%s\", \"metric\":\"%s\", \"timestamp\":%d, \"value\":%f, \"step\":%d, \"counterType\":\"GAUGE\", \"tags\":\"%s\" }]",
                service, metric, seconds, availability, period, tags);
        post.setRequestBody(msg);
        try {
            LOG.debug("Push falcon metrics: " + msg);
            int ret = client.executeMethod(post);
            if (ret >= 300) {
                LOG.warn("Push falcon failed: " + ret + " " + post.getResponseBodyAsString());
            }
        } catch (IOException e) {
            LOG.warn("Push metrics to falcon failed: " + msg, e);
        }
    }
}
