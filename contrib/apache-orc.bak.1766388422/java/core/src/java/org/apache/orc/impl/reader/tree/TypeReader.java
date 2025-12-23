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

package org.apache.orc.impl.reader.tree;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.io.filter.FilterContext;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;
import java.util.EnumSet;

public interface TypeReader {
  void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException;

  void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException;

  void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException;

  void seek(PositionProvider index, ReadPhase readPhase) throws IOException;

  void skipRows(long rows, ReadPhase readPhase) throws IOException;

  void nextVector(ColumnVector previous,
                  boolean[] isNull,
                  int batchSize,
                  FilterContext filterContext,
                  ReadPhase readPhase) throws IOException;

  int getColumnId();

  ReaderCategory getReaderCategory();

  /**
   * Determines if the child of the parent should be allowed based on the read level. The child
   * is allowed based on the read level or if the child is a FILTER_PARENT, this allows the handling
   * of NON_FILTER children on the FILTER_PARENT child
   * @param child the child reader that is being evaluated
   * @param readPhase the requested read level
   * @return true if allowed by read level or if it is a FILTER_PARENT otherwise false
   */
  static boolean shouldProcessChild(TypeReader child, ReadPhase readPhase) {
    return readPhase.contains(child.getReaderCategory()) ||
           child.getReaderCategory() == ReaderCategory.FILTER_PARENT;
  }

  enum ReaderCategory {
    FILTER_CHILD,    // Primitive type that is a filter column
    FILTER_PARENT,   // Compound type with filter children
    NON_FILTER       // Non-filter column
  }

  enum ReadPhase {
    // Used to read all columns in the absence of filters
    ALL(EnumSet.allOf(ReaderCategory.class)),
    // Used to perform read of the filter columns in the presence of filters
    LEADERS(EnumSet.of(ReaderCategory.FILTER_PARENT, ReaderCategory.FILTER_CHILD)),
    // Used to perform the read of non-filter columns after a match on the filter columns when a
    // skip is not needed on the non-filter columns
    FOLLOWERS(EnumSet.of(ReaderCategory.NON_FILTER)),
    // Used to reposition the FILTER_PARENTs when a forward seek is required within the same row
    // group
    LEADER_PARENTS(EnumSet.of(ReaderCategory.FILTER_PARENT)),
    // Used to reposition the FILTER_PARENTs and NON_FILTERs, this is required to accurately
    // determine the the non-null rows to skip.
    FOLLOWERS_AND_PARENTS(EnumSet.of(ReaderCategory.FILTER_PARENT, ReaderCategory.NON_FILTER));

    EnumSet<ReaderCategory> categories;
    ReadPhase(EnumSet<ReaderCategory> categories) {
      this.categories = categories;
    }

    public boolean contains(ReaderCategory readerCategory) {
      return categories.contains(readerCategory);
    }
  }
}
