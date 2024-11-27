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

public class ObsProperties extends BaseProperties {
    public static final String OBS_PREFIX = "obs.";
    public static final String OBS_FS_PREFIX = "fs.obs";

    public static final String ENDPOINT = "obs.endpoint";
    public static final String REGION = "obs.region";
    public static final String ACCESS_KEY = "obs.access_key";
    public static final String SECRET_KEY = "obs.secret_key";
    public static final String SESSION_TOKEN = "obs.session_token";
    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, ACCESS_KEY, SECRET_KEY);

    public static class FS {
        public static final String SESSION_TOKEN = "fs.obs.session.token";
        public static final String IMPL_DISABLE_CACHE = "fs.obs.impl.disable.cache";
    }

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }

    /**
     * This class contains constants related to the OBS (Hua Wei Object Storage Service) properties.
     * <p>
     * The constants in the `HadoopFsObsConstants` inner class are copied from
     * `org.apache.hadoop.fs.obs.OBSConstants`. This approach is deliberately taken to
     * avoid a compile-time dependency on the `hadoop-huaweicloud` library. By doing so, we
     * ensure that this project remains decoupled from `hadoop-obs`, allowing it to be
     * compiled and built independently.
     * <p>
     * Similar to the Obs properties, we can control whether to include OBS-related
     * dependencies by configuring a build parameter. By default, the OBS-related
     * dependencies are not included in the packaging process. If the package does not
     * contain these dependencies but the functionality related to Hadoop OBS is required,
     * users will need to manually copy the relevant dependencies into the `fe/lib` directory.
     * <p>
     * However, manually copying dependencies is not recommended since this is not an
     * uberjar, and there could be potential issues such as version conflicts or missing
     * transitive dependencies.
     * <p>
     * Users are encouraged to configure the build process to include the necessary
     * dependencies when Hadoop OBS support is required, ensuring a smoother
     * and more reliable deployment.
     * <p>
     * Additionally, by copying these constants instead of directly depending on
     * `hadoop-huaweicloud`, there is an additional maintenance overhead. Any changes in
     * `OBSConstants` in future versions of `hadoop-obs` will not be automatically
     * reflected here. It is important to manually track and update these constants
     * as needed to ensure compatibility.
     */
    public static class HadoopFsObsConstants {
        public static final String HADOOP_FS_OBS_CLASS_NAME = "org.apache.hadoop.fs.obs.OBSFileSystem";

        public static final String ENDPOINT = "fs.obs.endpoint";
        public static final String ACCESS_KEY = "fs.obs.access.key";
        public static final String SECRET_KEY = "fs.obs.secret.key";
    }
}
