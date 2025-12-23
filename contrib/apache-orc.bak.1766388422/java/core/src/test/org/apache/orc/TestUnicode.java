/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUnicode {
  Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
      + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  private static Stream<Arguments> data() {
    ArrayList<Arguments> data = new ArrayList<>();
    for (int j = 0; j < 2; j++) {
      for (int i = 1; i <= 5; i++) {
        data.add(Arguments.of(j == 0 ? "char" : "varchar", i, true));
      }
    }
    return data.stream();
  }

  static final String[] utf8strs = new String[] {
      // Character.UnicodeBlock GREEK (2 bytes)
      "\u03b1\u03b2\u03b3", "\u03b1\u03b2", "\u03b1\u03b2\u03b3\u03b4",
      "\u03b1\u03b2\u03b3\u03b4",
      // Character.UnicodeBlock MALAYALAM (3 bytes)
      "\u0d06\u0d30\u0d3e", "\u0d0e\u0d28\u0d4d\u0d24\u0d3e", "\u0d13\u0d7c\u0d15\u0d4d",
      // Unicode emoji (4 bytes)
      "\u270f\ufe0f\ud83d\udcdd\u270f\ufe0f", "\ud83c\udf3b\ud83d\udc1d\ud83c\udf6f",
      "\ud83c\udf7a\ud83e\udd43\ud83c\udf77" };

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testUtf8(String type, int maxLength, boolean hasRTrim) throws Exception {
    if (type.equals("varchar")) {
      testVarChar(maxLength);
    } else {
      testChar(maxLength, hasRTrim);
    }
  }

  // copied from HiveBaseChar
  public static String enforceMaxLength(String val, int maxLength) {
    if (val == null) {
      return null;
    }
    String value = val;

    if (maxLength > 0) {
      int valLength = val.codePointCount(0, val.length());
      if (valLength > maxLength) {
        // Truncate the excess chars to fit the character length.
        // Also make sure we take supplementary chars into account.
        value = val.substring(0, val.offsetByCodePoints(0, maxLength));
      }
    }
    return value;
  }

  // copied from HiveBaseChar
  public static String getPaddedValue(String val, int maxLength, boolean rtrim) {
    if (val == null) {
      return null;
    }
    if (maxLength < 0) {
      return val;
    }

    int valLength = val.codePointCount(0, val.length());
    if (valLength > maxLength) {
      return enforceMaxLength(val, maxLength);
    }

    if (maxLength > valLength && rtrim == false) {
      // Make sure we pad the right amount of spaces; valLength is in terms of code points,
      // while StringUtils.rpad() is based on the number of java chars.
      int padLength = val.length() + (maxLength - valLength);
      val = StringUtils.rightPad(val, padLength);
    }
    return val;
  }

  public void testChar(int maxLength, boolean hasRTrim) throws Exception {
    // char(n)
    TypeDescription schema = TypeDescription.createChar().withMaxLength(maxLength);
    String[] expected = new String[utf8strs.length];
    for (int i = 0; i < utf8strs.length; i++) {
      expected[i] = getPaddedValue(utf8strs[i], maxLength, hasRTrim);
    }
    verifyWrittenStrings(schema, utf8strs, expected, maxLength);
  }

  public void testVarChar(int maxLength) throws Exception {
    // char(n)
    TypeDescription schema = TypeDescription.createVarchar().withMaxLength(maxLength);
    String[] expected = new String[utf8strs.length];
    for (int i = 0; i < utf8strs.length; i++) {
      expected[i] = enforceMaxLength(utf8strs[i], maxLength);
    }
    verifyWrittenStrings(schema, utf8strs, expected, maxLength);
  }

  public void verifyWrittenStrings(TypeDescription schema, String[] inputs, String[] expected, int maxLength)
      throws Exception {
    Writer writer =
        OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema)
            .compress(CompressionKind.NONE).bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector col = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < inputs.length; i++) {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      col.setVal(batch.size++, inputs[i].getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader =
        OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    col = (BytesColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        assertEquals(expected[idx], toString(col, r),
            String.format("test for %s:%d", schema, maxLength));
        idx++;
      }
    }
    fs.delete(testFilePath, false);
  }

  static String toString(BytesColumnVector vector, int row) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (!vector.noNulls && vector.isNull[row]) {
      return null;
    }
    return new String(vector.vector[row], vector.start[row], vector.length[row],
        StandardCharsets.UTF_8);
  }
}
