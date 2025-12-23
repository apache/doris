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

package org.apache.orc.impl.filter;

import org.apache.orc.OrcFilterContext;

/**
 * A filter that operates on the supplied
 * {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch} and updates the selections.
 * <p>
 * This is the interface that is the basis of both the leaf filters such as Equals, In and logical
 * filters such as And, Or and Not
 */
public interface VectorFilter {

  /**
   * Filter the vectorized row batch that is wrapped into the FilterContext.
   * @param fc     The filter context that wraps the VectorizedRowBatch
   * @param bound  The bound of the scan, it is expected that the filter only operates on the bound
   *               and change the selection status of the rows scoped by the bound. The filter is
   *               expected to leave the bound unchanged.
   * @param selOut The filter should update the selOut for the elements scoped by bound. The selOut
   *               should be sorted in ascending order
   */
  void filter(OrcFilterContext fc, Selected bound, Selected selOut);
}
