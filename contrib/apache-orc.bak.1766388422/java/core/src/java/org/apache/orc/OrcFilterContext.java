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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;

/**
 * This defines the input for any filter operation. This is an extension of
 * [[{@link VectorizedRowBatch}]] with schema.
 * <p>
 * This offers a convenience method of finding the column vector from a given column name
 * that the filters can invoke to get access to the column vector.
 */
public interface OrcFilterContext extends MutableFilterContext {
  /**
   * Retrieves the column vector that matches the specified name. Allows support for nested struct
   * references e.g. order.date where date is a field in a struct called order.
   *
   * @param name The column name whose vector should be retrieved
   * @return The column vectors from the root to the column name. The array levels match the name
   * levels with Array[0] referring to the top level, followed by the subsequent levels. For
   * example of order.date Array[0] refers to order and Array[1] refers to date
   * @throws IllegalArgumentException if the field is not found or if the nested field is not part
   *                                  of a struct
   */
  ColumnVector[] findColumnVector(String name);

  /**
   * Utility method for determining if the leaf vector of the branch can be treated as having
   * noNulls.
   * This method navigates from the top to the leaf and checks if we have nulls anywhere in the
   * branch as compared to checking just the leaf vector.
   *
   * @param vectorBranch The input vector branch from the root to the leaf
   * @return true if the entire branch satisfies noNull else false
   */
  static boolean noNulls(ColumnVector[] vectorBranch) {
    for (ColumnVector v : vectorBranch) {
      if (!v.noNulls) {
        return false;
      }
    }
    return true;
  }

  /**
   * Utility method for determining if a particular row element in the vector branch is null.
   * This method navigates from the top to the leaf and checks if we have nulls anywhere in the
   * branch as compared to checking just the leaf vector.
   *
   * @param vectorBranch The input vector branch from the root to the leaf
   * @param idx          The row index being tested
   * @return true if the entire branch is not null for the idx otherwise false
   * @throws IllegalArgumentException If a multivalued vector such as List or Map is encountered in
   *                                  the branch.
   */
  static boolean isNull(ColumnVector[] vectorBranch, int idx) throws IllegalArgumentException {
    for (ColumnVector v : vectorBranch) {
      if (v instanceof ListColumnVector || v instanceof MapColumnVector) {
        throw new IllegalArgumentException(String.format(
          "Found vector: %s in branch. List and Map vectors are not supported in isNull "
          + "determination", v));
      }
      // v.noNulls = false does not mean that we have at least one null value
      if (!v.noNulls && v.isNull[v.isRepeating ? 0 : idx]) {
        return true;
      }
    }
    return false;
  }
}
