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

package org.apache.doris.filesystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

class FileSystemTransferUtilTest {

    // --- globToRegex ---

    @Test
    void starMatchesAnyCharExceptSlash() {
        Pattern pattern = FileSystemTransferUtil.globToRegex("s3://bucket/dir/*.csv");
        Assertions.assertTrue(pattern.matcher("s3://bucket/dir/file.csv").matches());
        Assertions.assertTrue(pattern.matcher("s3://bucket/dir/another-file.csv").matches());
        Assertions.assertFalse(pattern.matcher("s3://bucket/dir/sub/file.csv").matches());
    }

    @Test
    void questionMarkMatchesSingleChar() {
        Pattern pattern = FileSystemTransferUtil.globToRegex("s3://bucket/dir/file?.csv");
        Assertions.assertTrue(pattern.matcher("s3://bucket/dir/file1.csv").matches());
        Assertions.assertTrue(pattern.matcher("s3://bucket/dir/fileA.csv").matches());
        Assertions.assertFalse(pattern.matcher("s3://bucket/dir/file12.csv").matches());
        Assertions.assertFalse(pattern.matcher("s3://bucket/dir/file.csv").matches());
    }

    @Test
    void dotIsEscaped() {
        Pattern pattern = FileSystemTransferUtil.globToRegex("s3://bucket/dir/file.csv");
        Assertions.assertTrue(pattern.matcher("s3://bucket/dir/file.csv").matches());
        Assertions.assertFalse(pattern.matcher("s3://bucket/dir/fileXcsv").matches());
    }

    @Test
    void specialCharsAreEscaped() {
        Pattern pattern = FileSystemTransferUtil.globToRegex("s3://bucket/dir/file{1}.csv");
        Assertions.assertTrue(pattern.matcher("s3://bucket/dir/file{1}.csv").matches());
        Assertions.assertFalse(pattern.matcher("s3://bucket/dir/file1.csv").matches());
    }

    @Test
    void bracketAndParenAreEscaped() {
        Pattern pattern = FileSystemTransferUtil.globToRegex("prefix(a)[b]end");
        Assertions.assertTrue(pattern.matcher("prefix(a)[b]end").matches());
    }

    @Test
    void pipeAndPlusAreEscaped() {
        Pattern pattern = FileSystemTransferUtil.globToRegex("a|b+c");
        Assertions.assertTrue(pattern.matcher("a|b+c").matches());
        Assertions.assertFalse(pattern.matcher("abc").matches());
    }

    @Test
    void backslashIsEscaped() {
        Pattern pattern = FileSystemTransferUtil.globToRegex("a\\b");
        Assertions.assertTrue(pattern.matcher("a\\b").matches());
    }

    @Test
    void noWildcardMatchesExact() {
        Pattern pattern = FileSystemTransferUtil.globToRegex("s3://bucket/exact-path.txt");
        Assertions.assertTrue(pattern.matcher("s3://bucket/exact-path.txt").matches());
        Assertions.assertFalse(pattern.matcher("s3://bucket/other-path.txt").matches());
    }
}
