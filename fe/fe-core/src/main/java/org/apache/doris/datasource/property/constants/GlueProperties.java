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

package org.apache.doris.datasource.property.constants;

import org.apache.doris.common.credentials.CloudCredential;

import com.amazonaws.glue.catalog.util.AWSGlueConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GlueProperties extends BaseProperties {
    public static final String ENDPOINT = "glue.endpoint";
    public static final String REGION = "glue.region";
    public static final String ACCESS_KEY = "glue.access_key";
    public static final String SECRET_KEY = "glue.secret_key";
    public static final String SESSION_TOKEN = "glue.session_token";

    public static final List<String> META_KEYS = Arrays.asList(AWSGlueConfig.AWS_GLUE_ENDPOINT,
            AWSGlueConfig.AWS_REGION, AWSGlueConfig.AWS_GLUE_ACCESS_KEY, AWSGlueConfig.AWS_GLUE_SECRET_KEY,
            AWSGlueConfig.AWS_GLUE_SESSION_TOKEN);

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }

    public static CloudCredential getCompatibleCredential(Map<String, String> props) {
        // Compatible with older versions.
        CloudCredential credential = getCloudCredential(props, AWSGlueConfig.AWS_GLUE_ACCESS_KEY,
                    AWSGlueConfig.AWS_GLUE_SECRET_KEY, AWSGlueConfig.AWS_GLUE_SESSION_TOKEN);
        if (!credential.isWhole()) {
            credential = BaseProperties.getCompatibleCredential(props);
        }
        return credential;
    }
}
