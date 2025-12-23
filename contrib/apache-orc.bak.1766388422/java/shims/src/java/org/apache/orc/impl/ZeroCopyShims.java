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
package org.apache.orc.impl;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

class ZeroCopyShims {
  private static final class ByteBufferPoolAdapter implements ByteBufferPool {
    private HadoopShims.ByteBufferPoolShim pool;

    ByteBufferPoolAdapter(HadoopShims.ByteBufferPoolShim pool) {
      this.pool = pool;
    }

    @Override
    public ByteBuffer getBuffer(boolean direct, int length) {
      return this.pool.getBuffer(direct, length);
    }

    @Override
    public void putBuffer(ByteBuffer buffer) {
      this.pool.putBuffer(buffer);
    }
  }

  private static final class ZeroCopyAdapter implements HadoopShims.ZeroCopyReaderShim {
    private final FSDataInputStream in;
    private final ByteBufferPoolAdapter pool;
    private static final EnumSet<ReadOption> CHECK_SUM = EnumSet
        .noneOf(ReadOption.class);
    private static final EnumSet<ReadOption> NO_CHECK_SUM = EnumSet
        .of(ReadOption.SKIP_CHECKSUMS);

    ZeroCopyAdapter(FSDataInputStream in,
                           HadoopShims.ByteBufferPoolShim poolshim) {
      this.in = in;
      if (poolshim != null) {
        pool = new ByteBufferPoolAdapter(poolshim);
      } else {
        pool = null;
      }
    }

    @Override
    public ByteBuffer readBuffer(int maxLength, boolean verifyChecksums)
        throws IOException {
      EnumSet<ReadOption> options = NO_CHECK_SUM;
      if (verifyChecksums) {
        options = CHECK_SUM;
      }
      return this.in.read(this.pool, maxLength, options);
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      this.in.releaseBuffer(buffer);
    }

    @Override
    public void close() throws IOException {
      this.in.close();
    }
  }

  public static HadoopShims.ZeroCopyReaderShim getZeroCopyReader(
      FSDataInputStream in, HadoopShims.ByteBufferPoolShim pool) throws IOException {
    return new ZeroCopyAdapter(in, pool);
  }

}
