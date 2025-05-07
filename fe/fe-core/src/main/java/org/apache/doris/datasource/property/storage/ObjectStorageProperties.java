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

/**
 * Interface representing the properties and configurations for object storage systems.
 * This interface provides methods for converting the storage properties to specific
 * configurations for different protocols, such as Hadoop HDFS and AWS S3.
 */
public interface ObjectStorageProperties {

    String getEndpoint();

    String getRegion();

    String getAccessKey();

    String getSecretKey();

    void setEndpoint(String endpoint);

    void setRegion(String region);

}
