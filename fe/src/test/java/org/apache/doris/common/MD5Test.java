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

package org.apache.doris.common;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

public class MD5Test {

    private static String fileName = "job_info.txt";

    @BeforeClass
    public static void createFile() {
        String json = "{'key': 'value'}";

        try (PrintWriter out = new PrintWriter(fileName)) {
            out.print(json);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void deleteFile() {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void test() {
        File localFile = new File(fileName);
        String md5sum = null;
        try {
            md5sum = DigestUtils.md5Hex(new FileInputStream(localFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(md5sum);
        String fullName = fileName + "__" + md5sum;
        System.out.println(fullName);

        System.out.println(fullName.lastIndexOf("__"));
        System.out.println(fullName.substring(fullName.lastIndexOf("__") + 2));
        System.out.println(fullName.substring(0, fullName.lastIndexOf("__")));
        System.out.println(md5sum.length());
    }

}
