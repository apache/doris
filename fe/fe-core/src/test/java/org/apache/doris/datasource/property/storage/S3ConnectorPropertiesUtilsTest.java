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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.HashMap;
import java.util.Map;

class S3ConnectorPropertiesUtilsTest {

    @Test
    void testCheckLoadPropsAndReturnUri_success() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "https://bucket.s3.us-west-2.amazonaws.com/key");
        String result = S3PropertyUtils.validateAndGetUri(props);
        Assertions.assertEquals("https://bucket.s3.us-west-2.amazonaws.com/key", result);
    }

    @Test
    void testCheckLoadPropsAndReturnUri_missingKey() {
        Map<String, String> props = new HashMap<>();
        Executable executable = () -> S3PropertyUtils.validateAndGetUri(props);
        StoragePropertiesException exception = Assertions.assertThrows(StoragePropertiesException.class, executable);
        Assertions.assertEquals("props is empty", exception.getMessage());

        props.put("someKey", "value");
        executable = () -> S3PropertyUtils.validateAndGetUri(props);
        exception = Assertions.assertThrows(StoragePropertiesException.class, executable);
        Assertions.assertEquals("props must contain uri", exception.getMessage());
    }

    @Test
    void testConstructEndpointFromUrl_success() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "https://my-bucket.s3.us-east-1.amazonaws.com/some/file.txt");
        String endpoint = S3PropertyUtils.constructEndpointFromUrl(props, "false", "false");
        Assertions.assertEquals("s3.us-east-1.amazonaws.com", endpoint);
    }

    @Test
    void testConstructEndpointFromUrl_nullOrBlank() throws UserException {
        Map<String, String> props = new HashMap<>();
        Assertions.assertNull(S3PropertyUtils.constructEndpointFromUrl(props, "true", "false"));

        props.put("uri", "");
        Assertions.assertNull(S3PropertyUtils.constructEndpointFromUrl(props, "false", "true"));

        props.put("uri", "invalid uri without scheme");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> S3PropertyUtils.constructEndpointFromUrl(props, "true", "true"));
    }

    @Test
    void testConstructRegionFromUrl_success() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "https://my-bucket.s3.us-west-1.amazonaws.com/test.txt");
        String region = S3PropertyUtils.constructRegionFromUrl(props, "false", "false");
        Assertions.assertEquals("us-west-1", region);
    }

    @Test
    void testConstructRegionFromUrl_nullOrInvalid() throws UserException {
        Map<String, String> props = new HashMap<>();
        Assertions.assertNull(S3PropertyUtils.constructRegionFromUrl(props, "false", "false"));

        props.put("uri", "");
        Assertions.assertNull(S3PropertyUtils.constructRegionFromUrl(props, "false", "true"));

        props.put("uri", "not a uri");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> S3PropertyUtils.constructRegionFromUrl(props, "false", "true"));
        props.put("uri", "https://my-bucket.s3.us-west-1.amazonaws.com/test.txt");
        Assertions.assertEquals("us-west-1", S3PropertyUtils.constructRegionFromUrl(props, "false", "true"));
    }

    @Test
    void testConvertToS3Address_success() throws UserException {
        String httpsUrl = "https://my-bucket.s3.us-east-1.amazonaws.com/test/key.txt";
        String s3Path = S3PropertyUtils.validateAndNormalizeUri(httpsUrl, "false", "false");
        Assertions.assertEquals("s3://my-bucket/test/key.txt", s3Path);

        String alreadyS3 = "s3://bucket-name/path/file.csv";
        Assertions.assertEquals(alreadyS3, S3PropertyUtils.validateAndNormalizeUri(alreadyS3, "true", "true"));
    }

    @Test
    void testConvertToS3Address_invalid() {
        Assertions.assertThrows(StoragePropertiesException.class, () -> S3PropertyUtils.validateAndNormalizeUri(null, "false", "true"));
        Assertions.assertThrows(StoragePropertiesException.class, () -> S3PropertyUtils.validateAndNormalizeUri("", "false", "false"));
        Assertions.assertThrows(UserException.class, () -> S3PropertyUtils.validateAndNormalizeUri("not a uri", "true", "true"));
    }
}
