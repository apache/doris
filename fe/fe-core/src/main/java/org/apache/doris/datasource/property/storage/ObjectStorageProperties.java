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

package org.apache.doris.datasource.property.storage;

import java.util.Map;

/**
 * Interface representing the properties and configurations for object storage systems.
 * This interface provides methods for converting the storage properties to specific
 * configurations for different protocols, such as Hadoop HDFS and AWS S3.
 */
public interface ObjectStorageProperties {

    /**
     * Converts the object storage properties to a configuration map compatible with the
     * Hadoop HDFS protocol. This method allows the object storage to be used in a Hadoop-based
     * environment by providing the necessary configuration details in the form of key-value pairs.
     *
     * @param config a map to populate with the HDFS-compatible configuration parameters.
     *               These parameters will be used by Hadoop clients to connect to the object storage system.
     */
    void toHadoopConfiguration(Map<String, String> config);

    /**
     * Converts the object storage properties to a configuration map compatible with the
     * AWS S3 protocol. This method provides the necessary configuration parameters for connecting
     * to the object storage system via AWS S3 API, allowing it to be used in S3-compatible environments.
     *
     * @param config a map to populate with the S3-compatible configuration parameters.
     *               These parameters will be used by AWS S3 clients or compatible services to connect
     *               to the object storage system.
     */
    void toNativeS3Configuration(Map<String, String> config);
}
