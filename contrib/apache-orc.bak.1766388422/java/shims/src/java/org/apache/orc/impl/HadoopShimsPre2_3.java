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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * Shims for versions of Hadoop up to and including 2.2.x
 */
public class HadoopShimsPre2_3 implements HadoopShims {

  HadoopShimsPre2_3() {
  }

  @Override
  public DirectDecompressor getDirectDecompressor(
      DirectCompressionType codec) {
    return null;
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    /* not supported */
    return null;
  }

  @Override
  public boolean endVariableLengthBlock(OutputStream output) {
    return false;
  }

  @Override
  public KeyProvider getHadoopKeyProvider(Configuration conf, Random random) {
    return new NullKeyProvider();
  }

}
