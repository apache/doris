/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// TODO: Make OrcTail implement FileMetadata or Reader interface
public final class OrcTail {
  private static final Logger LOG = LoggerFactory.getLogger(OrcTail.class);

  // postscript + footer - Serialized in OrcSplit
  private final OrcProto.FileTail fileTail;
  // serialized representation of metadata, footer and postscript
  private final BufferChunk serializedTail;
  private final TypeDescription schema;
  // used to invalidate cache entries
  private final long fileModificationTime;
  private final Reader reader;

  public OrcTail(OrcProto.FileTail fileTail,
                 ByteBuffer serializedTail) throws IOException {
    this(fileTail, serializedTail, -1);
  }

  public OrcTail(OrcProto.FileTail fileTail, ByteBuffer serializedTail,
                 long fileModificationTime) throws IOException {
    this(fileTail,
        new BufferChunk(serializedTail, getStripeStatisticsOffset(fileTail)),
        fileModificationTime);
  }

  public OrcTail(OrcProto.FileTail fileTail, BufferChunk serializedTail,
                 long fileModificationTime) throws IOException {
    this(fileTail, serializedTail, fileModificationTime, null);
  }

  public OrcTail(OrcProto.FileTail fileTail, BufferChunk serializedTail,
                 long fileModificationTime, Reader reader) throws IOException {
    this.fileTail = fileTail;
    this.serializedTail = serializedTail;
    this.fileModificationTime = fileModificationTime;
    List<OrcProto.Type> types = getTypes();
    OrcUtils.isValidTypeTree(types, 0);
    this.schema = OrcUtils.convertTypeFromProtobuf(types, 0);
    this.reader = reader;
  }

  public ByteBuffer getSerializedTail() {
    if (serializedTail.next == null) {
      return serializedTail.getData();
    } else {
      // make a single buffer...
      int len = 0;
      for(BufferChunk chunk=serializedTail;
          chunk != null;
          chunk = (BufferChunk) chunk.next) {
        len += chunk.getLength();
      }
      ByteBuffer result = ByteBuffer.allocate(len);
      for(BufferChunk chunk=serializedTail;
          chunk != null;
          chunk = (BufferChunk) chunk.next) {
        ByteBuffer tmp = chunk.getData();
        result.put(tmp.array(), tmp.arrayOffset() + tmp.position(),
            tmp.remaining());
      }
      result.flip();
      return result;
    }
  }

  /**
   * Gets the buffer chunks that correspond to the stripe statistics,
   * file tail, and post script.
   * @return the shared buffers with the contents of the file tail
   */
  public BufferChunk getTailBuffer() {
    return serializedTail;
  }

  public long getFileModificationTime() {
    return fileModificationTime;
  }

  public OrcProto.Footer getFooter() {
    return fileTail.getFooter();
  }

  public OrcProto.PostScript getPostScript() {
    return fileTail.getPostscript();
  }

  public OrcFile.WriterVersion getWriterVersion() {
    OrcProto.PostScript ps = fileTail.getPostscript();
    OrcProto.Footer footer = fileTail.getFooter();
    OrcFile.WriterImplementation writer =
        OrcFile.WriterImplementation.from(footer.getWriter());
    return OrcFile.WriterVersion.from(writer, ps.getWriterVersion());
  }

  public List<StripeInformation> getStripes() {
    return OrcUtils.convertProtoStripesToStripes(getFooter().getStripesList());
  }

  public CompressionKind getCompressionKind() {
    return CompressionKind.valueOf(fileTail.getPostscript().getCompression().name());
  }

  public int getCompressionBufferSize() {
    OrcProto.PostScript postScript = fileTail.getPostscript();
    return ReaderImpl.getCompressionBlockSize(postScript);
  }

  public int getMetadataSize() {
    return (int) getPostScript().getMetadataLength();
  }

  public List<OrcProto.Type> getTypes() {
    return getFooter().getTypesList();
  }

  public TypeDescription getSchema() {
    return schema;
  }

  public OrcProto.FileTail getFileTail() {
    return fileTail;
  }

  static long getMetadataOffset(OrcProto.FileTail tail) {
    OrcProto.PostScript ps = tail.getPostscript();
    return tail.getFileLength()
        - 1
        - tail.getPostscriptLength()
        - ps.getFooterLength()
        - ps.getMetadataLength();
  }

  static long getStripeStatisticsOffset(OrcProto.FileTail tail) {
    OrcProto.PostScript ps = tail.getPostscript();
    return getMetadataOffset(tail) - ps.getStripeStatisticsLength();
  }

  /**
   * Get the file offset of the metadata section of footer.
   * @return the byte offset of the start of the metadata
   */
  public long getMetadataOffset() {
    return getMetadataOffset(fileTail);
  }

  /**
   * Get the file offset of the stripe statistics.
   * @return the byte offset of the start of the stripe statistics
   */
  public long getStripeStatisticsOffset() {
    return getStripeStatisticsOffset(fileTail);
  }

  /**
   * Get the position of the end of the file.
   * @return the byte length of the file
   */
  public long getFileLength() {
    return fileTail.getFileLength();
  }

  public OrcProto.FileTail getMinimalFileTail() {
    OrcProto.FileTail.Builder fileTailBuilder = OrcProto.FileTail.newBuilder(fileTail);
    OrcProto.Footer.Builder footerBuilder = OrcProto.Footer.newBuilder(fileTail.getFooter());
    footerBuilder.clearStatistics();
    fileTailBuilder.setFooter(footerBuilder.build());
    return fileTailBuilder.build();
  }

  /**
   * Get the stripe statistics from the file tail.
   * This code is for compatibility with ORC 1.5.
   * @return the stripe statistics
   * @deprecated the user should use Reader.getStripeStatistics instead.
   */
  public List<StripeStatistics> getStripeStatistics() throws IOException {
    if (reader == null) {
      LOG.warn("Please use Reader.getStripeStatistics or give `Reader` to OrcTail constructor.");
      return new ArrayList<>();
    } else {
      return reader.getStripeStatistics();
    }
  }
}
