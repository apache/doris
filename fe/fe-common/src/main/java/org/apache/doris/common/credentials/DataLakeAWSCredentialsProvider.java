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

package org.apache.doris.common.credentials;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

public class DataLakeAWSCredentialsProvider implements AWSCredentialsProvider  {

    private final Configuration conf;

    public DataLakeAWSCredentialsProvider(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public AWSCredentials getCredentials() {
        String accessKey = StringUtils.trim(conf.get(Constants.ACCESS_KEY));
        String secretKey = StringUtils.trim(conf.get(Constants.SECRET_KEY));
        String sessionToken = StringUtils.trim(conf.get(Constants.SESSION_TOKEN));
        if (!StringUtils.isNullOrEmpty(accessKey) && !StringUtils.isNullOrEmpty(secretKey)) {
            return (StringUtils.isNullOrEmpty(sessionToken) ? new BasicAWSCredentials(accessKey,
                secretKey) : new BasicSessionCredentials(accessKey, secretKey, sessionToken));
        } else {
            throw new SdkClientException(
                "Unable to load AWS credentials from hive conf (fs.s3a.access.key and fs.s3a.secret.key)");
        }
    }

    @Override
    public void refresh() {
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
