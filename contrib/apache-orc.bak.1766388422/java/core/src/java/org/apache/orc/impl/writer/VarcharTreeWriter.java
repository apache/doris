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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.Utf8Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Under the covers, varchar is written to ORC the same way as string.
 */
public class VarcharTreeWriter extends StringBaseTreeWriter {
  private final int maxLength;

  VarcharTreeWriter(TypeDescription schema,
                    WriterEncryptionVariant encryption,
                    WriterContext context) throws IOException {
    super(schema, encryption, context);
    maxLength = schema.getMaxLength();
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    BytesColumnVector vec = (BytesColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        // 0, length times
        writeTruncated(vec, 0, length);
      }
    } else {
      for(int i=0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          // offset + i, once per loop
          writeTruncated(vec, i + offset, 1);
        }
      }
    }
  }

  private void writeTruncated(BytesColumnVector vec, int row, int repeats)
      throws IOException {
    int itemLength =
        Utf8Utils.truncateBytesTo(maxLength, vec.vector[row], vec.start[row], vec.length[row]);
    if (useDictionaryEncoding) {
      int id = dictionary.add(vec.vector[row], vec.start[row], itemLength);
      for (int i = 0; i < repeats; ++i) {
        rows.add(id);
      }
    } else {
      for (int i = 0; i < repeats; ++i) {
        directStreamOutput.write(vec.vector[row], vec.start[row], itemLength);
        lengthOutput.write(itemLength);
      }
    }
    indexStatistics.updateString(vec.vector[row], vec.start[row], itemLength, repeats);
    if (createBloomFilter) {
      if (bloomFilter != null) {
        // translate from UTF-8 to the default charset
        bloomFilter.addString(new String(vec.vector[row], vec.start[row], itemLength,
            StandardCharsets.UTF_8));
      }
      bloomFilterUtf8.addBytes(vec.vector[row], vec.start[row], itemLength);
    }
  }
}
