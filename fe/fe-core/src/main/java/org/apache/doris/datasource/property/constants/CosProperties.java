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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CosProperties extends BaseProperties {
    public static final String COS_PREFIX = "cos.";
    public static final String COS_FS_PREFIX = "fs.cos";

    public static final String ENDPOINT = "cos.endpoint";
    public static final String ACCESS_KEY = "cos.access_key";
    public static final String SECRET_KEY = "cos.secret_key";
    public static final String REGION = "cos.region";
    public static final String SESSION_TOKEN = "cos.session_token";
    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, ACCESS_KEY, SECRET_KEY);

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }

    /**
     * This class contains constants related to the COS (Tencent Cloud Object Storage) properties.
     * <p>
     * The constants in the `HadoopFsCosConstants` inner class are copied from
     * `org.apache.hadoop.fs.CosNConfigKeys`. This approach is intentionally taken to
     * avoid a compile-time dependency on the `hadoop-cos` library. By doing so, we
     * ensure that this project remains decoupled from `hadoop-cos`, allowing it to be
     * compiled and built independently.
     * <p>
     * We can control whether to include COS-related dependencies by configuring
     * a build parameter. By default, the COS-related dependencies are not included in
     * the packaging process. If the package does not contain these dependencies but
     * the functionality related to Hadoop COS is required, users will need to manually
     * copy the relevant dependencies into the `fe/lib` directory.
     * <p>
     * However, since this is not an uberjar and the required dependencies are not bundled
     * together, manually copying dependencies is not recommended due to potential
     * issues such as version conflicts or missing transitive dependencies.
     * <p>
     * Users are encouraged to configure the build process to include the necessary
     * dependencies when Hadoop COS support is required, ensuring a smoother
     * and more reliable deployment.
     * <p>
     * Additionally, by copying these constants instead of directly depending on
     * `hadoop-cos`, there is an additional maintenance overhead. Any changes in
     * `CosNConfigKeys` in future versions of `hadoop-cos` will not be automatically
     * reflected here. It is important to manually track and update these constants
     * as needed to ensure compatibility.
     */
    public static class HadoopFsCosConstants {
        public static final String HADOOP_FS_COS_CLASS_NAME = "org.apache.hadoop.fs.CosFileSystem";

        public static final String COSN_ENDPOINT_SUFFIX_KEY = "fs.cosn.bucket.endpoint_suffix";
        public static final String COSN_USERINFO_SECRET_ID_KEY = "fs.cosn.userinfo.secretId";
        public static final String COSN_USERINFO_SECRET_KEY_KEY = "fs.cosn.userinfo.secretKey";
    }
}
