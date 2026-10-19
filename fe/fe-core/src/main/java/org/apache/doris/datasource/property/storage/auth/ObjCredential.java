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

package org.apache.doris.datasource.property.storage.auth;

import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.thrift.TS3StorageParam;

/**
 * Provider-native credential that can be serialized for Cloud or BE.
 *
 * <p>This transport abstraction is deliberately independent of {@code S3ObjStorage}.
 * Runtime S3 client creation is extended through
 * {@code AbstractS3CompatibleProperties.createS3Client} instead.
 */
public interface ObjCredential {
    void applyTo(ObjectStoreInfoPB.Builder builder);

    void applyTo(TS3StorageParam param);
}
