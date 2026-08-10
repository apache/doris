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

package org.apache.doris.connector.fluss;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The storage settings of a lake configuration, and the names Doris binds them by.
 *
 * <p>Both columns are written out as literals rather than referenced from a constant: the left one is
 * paimon's spelling and the right one is Doris's, and this test exists precisely because the two differ in
 * ways no rule can express. A constant on either side would make the test agree with whatever the code
 * says.
 */
public class LakeStorageOptionsTest {

    @Test
    public void everyKnownStorageSettingIsTranslatedToItsDorisName() {
        Map<String, String> lakeOptions = new LinkedHashMap<>();
        lakeOptions.put("s3.access-key", "AK");
        lakeOptions.put("s3.secret-key", "SK");
        lakeOptions.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        lakeOptions.put("s3.region", "us-east-1");
        lakeOptions.put("fs.oss.accessKeyId", "oss-ak");
        lakeOptions.put("fs.oss.accessKeySecret", "oss-sk");
        lakeOptions.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        lakeOptions.put("fs.obs.access.key", "obs-ak");
        lakeOptions.put("fs.obs.secret.key", "obs-sk");
        lakeOptions.put("fs.obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");

        Map<String, String> expected = new HashMap<>();
        expected.put("s3.access_key", "AK");
        expected.put("s3.secret_key", "SK");
        expected.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        expected.put("s3.region", "us-east-1");
        expected.put("oss.access_key", "oss-ak");
        expected.put("oss.secret_key", "oss-sk");
        expected.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        expected.put("obs.access_key", "obs-ak");
        expected.put("obs.secret_key", "obs-sk");
        expected.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");

        Assertions.assertEquals(expected, LakeStorageOptions.toStorageProperties(lakeOptions));
    }

    @Test
    public void bothSpellingsPaimonAcceptsReachTheSameDorisName() {
        // paimon's own loaders declare these pairs as interchangeable, so a user who copied either one out
        // of a working paimon configuration has to end up configuring the same thing here.
        Map<String, String> dotted = new LinkedHashMap<>();
        dotted.put("s3.access.key", "AK");
        dotted.put("s3.secret.key", "SK");
        dotted.put("fs.obs.access-key", "obs-ak");
        dotted.put("fs.obs.secret-key", "obs-sk");

        Map<String, String> expected = new HashMap<>();
        expected.put("s3.access_key", "AK");
        expected.put("s3.secret_key", "SK");
        expected.put("obs.access_key", "obs-ak");
        expected.put("obs.secret_key", "obs-sk");

        Assertions.assertEquals(expected, LakeStorageOptions.toStorageProperties(dotted));
    }

    @Test
    public void settingsThatAreNotStorageAreLeftOut() {
        Map<String, String> lakeOptions = new LinkedHashMap<>();
        lakeOptions.put("warehouse", "s3://bucket/lake");
        lakeOptions.put("metastore", "filesystem");
        lakeOptions.put("uri", "thrift://hms:9083");
        // Spelled the way Hadoop spells it, and forwarded to the paimon sibling as such. It is not in the
        // table, so it is not this connector's to translate — it keeps travelling with the lake catalog.
        lakeOptions.put("fs.s3a.access.key", "AK");

        // The catalog's storage map is bound by fe-filesystem: a paimon catalog option in it would be
        // matched by no property class, so it would be neither used nor reported.
        Assertions.assertEquals(new HashMap<String, String>(),
                LakeStorageOptions.toStorageProperties(lakeOptions));
        Assertions.assertFalse(LakeStorageOptions.isStorageOption("warehouse"));
        Assertions.assertFalse(LakeStorageOptions.isStorageOption("fs.s3a.access.key"));
    }

    @Test
    public void translationIsByNameAndNotByASpellingRule() {
        // The temptation is "replace dashes with underscores". It gets s3.access-key right and everything
        // else wrong: OSS and OBS spell the same setting differently, and a rule would also rewrite keys
        // that are not storage at all.
        Assertions.assertEquals(Collections.singletonMap("oss.access_key", "AK"),
                LakeStorageOptions.toStorageProperties(
                        Collections.singletonMap("fs.oss.accessKeyId", "AK")));
        Assertions.assertFalse(LakeStorageOptions.isStorageOption("paimon.catalog.type"));
        Assertions.assertFalse(LakeStorageOptions.isStorageOption("s3.access_key"),
                "the Doris name is an output of this table, never an input");
    }
}
