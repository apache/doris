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

package org.apache.doris.master;

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.persist.StorageInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;

public class MetaHelperTest {

    @Test
    public void test() throws JsonProcessingException {
        ResponseBody<StorageInfo> bodyBefore = buildResponseBody();
        ObjectMapper mapper = new ObjectMapper();
        String bodyStr = mapper.writeValueAsString(bodyBefore);
        ResponseBody<StorageInfo> bodyAfter = MetaHelper.parseResponse(bodyStr, StorageInfo.class);
        Assert.assertEquals(bodyAfter, bodyBefore);
    }

    private ResponseBody<StorageInfo> buildResponseBody() {
        StorageInfo infoBefore = new StorageInfo();
        infoBefore.setClusterID(1);
        infoBefore.setEditsSeq(2L);
        infoBefore.setImageSeq(3L);
        ResponseBody<StorageInfo> bodyBefore = new ResponseBody<>();
        bodyBefore.setCode(RestApiStatusCode.UNAUTHORIZED.code);
        bodyBefore.setCount(5);
        bodyBefore.setData(infoBefore);
        bodyBefore.setMsg("msg");
        return bodyBefore;
    }

    File tempDir = new File(System.getProperty("java.io.tmpdir"), "tempDir");

    @BeforeEach
    void setUp() {

        if (tempDir.exists()) {
            tempDir.delete();
        }
        tempDir.mkdir();
    }

    @Test
    public void testFile() throws IOException {

        String errorFilename = "..testfile.";
        File errorFileWithSuffix = new File(tempDir, errorFilename);
        String rightFilename = "image.1";
        File rightFileWithSuffix = new File(tempDir, rightFilename);

        Config.meta_helper_security_mode = true;

        if (errorFileWithSuffix.exists()) {
            errorFileWithSuffix.delete();
        }
        Assert.assertThrows(Exception.class, () -> MetaHelper.complete(errorFilename, tempDir));
        Assert.assertThrows(Exception.class, () -> MetaHelper.getFile(errorFilename, tempDir));
        if (rightFileWithSuffix.exists()) {
            rightFileWithSuffix.delete();
        }
        Assert.assertEquals(rightFileWithSuffix.getName() + ".part", MetaHelper.getFile(rightFilename, tempDir).getName());

    }

    @Test
    public void testFileNameCheck() {
        Config.meta_helper_security_mode = true;
        MetaHelper.checkIsValidFileName("VERSION");
        MetaHelper.checkIsValidFileName("image.1");
        MetaHelper.checkIsValidFileName("image.1.part");
        MetaHelper.checkIsValidFileName("image.1.part.1");
        Assert.assertThrows(IllegalArgumentException.class, () -> MetaHelper.checkIsValidFileName("../testfile."));


    }

    @AfterEach
    public void tearDown() {
        if (tempDir.exists()) {
            tempDir.delete();
        }
    }

}
