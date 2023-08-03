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

package org.apache.doris.httpv2.util;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Md5Util {

    /**
     * Get file md5sum
     * @param file The file to be calculated md5
     * @return md5sum
     * @throws IOException
     */
    public static String getMd5String(File file) throws IOException {
        try {
            String md5sum = DigestUtils.md5Hex(new FileInputStream(file));
            Preconditions.checkNotNull(md5sum);
            return md5sum;
        } catch (FileNotFoundException e) {
            throw new IOException("file " + file.getCanonicalPath() + "does not exist");
        } catch (IOException e) {
            throw e;
        }
    }
}
