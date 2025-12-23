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
import java.util.Arrays;

/**
 * Under the covers, char is written to ORC the same way as string.
 */
public class CharTreeWriter extends StringBaseTreeWriter {
  private final int maxLength;
  private final byte[] padding;

  CharTreeWriter(TypeDescription schema,
                 WriterEncryptionVariant encryption,
                 WriterContext context) throws IOException {
    super(schema, encryption, context);
    maxLength = schema.getMaxLength();
    // utf-8 is currently 4 bytes long, but it could be upto 6
    padding = new byte[6*maxLength];
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    BytesColumnVector vec = (BytesColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        // 0, length times
        writePadded(vec, 0, length);
      }
    } else {
      for(int i=0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          // offset + i, once per loop
          writePadded(vec, i + offset, 1);
        }
      }
    }
  }

  private void writePadded(BytesColumnVector vec, int row, int repeats) throws IOException {
    final byte[] ptr;
    final int ptrOffset;
    final int ptrLength;
    int charLength = Utf8Utils.charLength(vec.vector[row], vec.start[row], vec.length[row]);
    if (charLength >= maxLength) {
      ptr = vec.vector[row];
      ptrOffset = vec.start[row];
      ptrLength =
          Utf8Utils
              .truncateBytesTo(maxLength, vec.vector[row], vec.start[row], vec.length[row]);
    } else {
      ptr = padding;
      // the padding is exactly 1 byte per char
      ptrLength = vec.length[row] + (maxLength - charLength);
      ptrOffset = 0;
      System.arraycopy(vec.vector[row], vec.start[row], ptr, 0, vec.length[row]);
      Arrays.fill(ptr, vec.length[row], ptrLength, (byte) ' ');
    }
    if (useDictionaryEncoding) {
      int id = dictionary.add(ptr, ptrOffset, ptrLength);
      for (int i = 0; i < repeats; ++i) {
        rows.add(id);
      }
    } else {
      for (int i = 0; i < repeats; ++i) {
        directStreamOutput.write(ptr, ptrOffset, ptrLength);
        lengthOutput.write(ptrLength);
      }
    }
    indexStatistics.updateString(ptr, ptrOffset, ptrLength, repeats);
    if (createBloomFilter) {
      if (bloomFilter != null) {
        // translate from UTF-8 to the default charset
        bloomFilter.addString(new String(ptr, ptrOffset, ptrLength, StandardCharsets.UTF_8));
      }
      bloomFilterUtf8.addBytes(ptr, ptrOffset, ptrLength);
    }
  }
}
