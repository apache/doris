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

import java.util.List;

/**
 * Cached file metadata. Right now, it caches everything; we don't have to store all the
 * protobuf structs actually, we could just store what we need, but that would require that
 * ORC stop depending on them too. Luckily, they shouldn't be very big.
 * @deprecated Use {@link org.apache.orc.impl.OrcTail} instead
 */
public interface FileMetadata {
  boolean isOriginalFormat();

  List<StripeInformation> getStripes();

  CompressionKind getCompressionKind();

  int getCompressionBufferSize();

  int getRowIndexStride();

  int getColumnCount();

  int getFlattenedColumnCount();

  Object getFileKey();

  List<Integer> getVersionList();

  int getMetadataSize();

  int getWriterImplementation();

  int getWriterVersionNum();

  List<OrcProto.Type> getTypes();

  List<OrcProto.StripeStatistics> getStripeStats();

  long getContentLength();

  long getNumberOfRows();

  List<OrcProto.ColumnStatistics> getFileStats();
}
