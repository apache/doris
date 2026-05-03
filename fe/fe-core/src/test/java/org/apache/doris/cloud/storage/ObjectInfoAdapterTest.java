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

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.StoragePropertiesConverter;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ObjectInfoAdapterTest {

    @Test
    public void testToStoragePropertiesPreservesAwsRoleFields() {
        ObjectInfo objectInfo = new ObjectInfo(
                Cloud.ObjectStoreInfoPB.Provider.S3,
                "",
                "",
                "snapshot-bucket",
                "s3.us-west-2.amazonaws.com",
                "us-west-2",
                "snapshot/prefix",
                "snapshot-session",
                "arn:aws:iam::123456789012:role/snapshot-role",
                "snapshot-external-id",
                null);

        StorageProperties storageProperties = ObjectInfoAdapter.toStorageProperties(objectInfo);
        Map<String, String> backendProps = StoragePropertiesConverter.toMap(storageProperties);

        Assert.assertEquals("S3", backendProps.get("_STORAGE_TYPE_"));
        Assert.assertEquals("s3.us-west-2.amazonaws.com", backendProps.get("AWS_ENDPOINT"));
        Assert.assertEquals("us-west-2", backendProps.get("AWS_REGION"));
        Assert.assertEquals("snapshot-bucket", backendProps.get("AWS_BUCKET"));
        Assert.assertEquals("arn:aws:iam::123456789012:role/snapshot-role",
                backendProps.get("AWS_ROLE_ARN"));
        Assert.assertEquals("snapshot-external-id", backendProps.get("AWS_EXTERNAL_ID"));
    }
}
