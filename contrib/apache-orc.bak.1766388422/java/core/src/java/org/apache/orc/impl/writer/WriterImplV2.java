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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.impl.WriterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An ORCv2 file writer. The file is divided into stripes, which is the natural
 * unit of work when reading. Each stripe is buffered in memory until the
 * memory reaches the stripe size and then it is written out broken down by
 * columns. Each column is written by a TreeWriter that is specific to that
 * type of column. TreeWriters may have children TreeWriters that handle the
 * sub-types. Each of the TreeWriters writes the column's data as a set of
 * streams.
 * <p>
 * This class is unsynchronized like most Stream objects, so from the creation
 * of an OrcFile and all access to a single instance has to be from a single
 * thread.
 * <p>
 * There are no known cases where these happen between different threads today.
 * <p>
 * Caveat: the MemoryManager is created during WriterOptions create, that has
 * to be confined to a single thread as well.
 *
 */
public class WriterImplV2 extends WriterImpl {

  private static final Logger LOG = LoggerFactory.getLogger(WriterImplV2.class);

  public WriterImplV2(FileSystem fs,
                      Path path,
                      OrcFile.WriterOptions opts) throws IOException {
    super(fs, path, opts);
    LOG.warn("ORC files written in " +
        OrcFile.Version.UNSTABLE_PRE_2_0.getName() + " will not be" +
          " readable by other versions of the software. It is only for" +
          " developer testing.");
  }
}
