// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.functions.agg;

/** aggregate functions which have corresponding MultiDistinctXXX class,
 * e.g. SUM,SUM0,COUNT,GROUP_CONCAT
 * */
public interface SupportMultiDistinct {
    AggregateFunction convertToMultiDistinct();

    /**
     * Validate that this distinct function can actually be rewritten to its multi-distinct form
     * for its current argument types, throwing a user-facing AnalysisException otherwise.
     *
     * <p>Only invoked when the multi-distinct rewrite is required (more than one distinct
     * argument in the same aggregate). The default is a no-op; functions whose BE multi-distinct
     * implementation supports only a subset of the types their signature advertises (e.g.
     * array_agg / collect_list, backed by a hash-set that cannot hold complex/object values)
     * override this to reject the unsupported types up front instead of failing at BE runtime.
     */
    default void checkSupportMultiDistinct() {}
}
