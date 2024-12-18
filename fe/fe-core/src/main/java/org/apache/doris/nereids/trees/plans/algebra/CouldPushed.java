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

package org.apache.doris.nereids.trees.plans.algebra;

/**
 *  Indicating whether it can be pushed down, such as limit or filter"
 */
public interface CouldPushed {


    /**
     * For example, regarding filter push down, if the filter is pushed down below the join and is successful,
     * then the previous filter no longer needs to be retained, in which case needRemain is set to true.
     * If the filter is pushed down into the scan and the filter still needs to be retained,
     * then needRemain is set to false
     */
    boolean needRemain();

    /**
     * For example, regarding filter push down, if the filter is pushed down below the join and is successful,
     * then the pushed-down filter's isPushed is set to true. Similarly, if the limit is pushed down to the join
     * and is successful, the pushed-down limit's isPushed is also set to true,
     * while the original retained limit's isPushed is set to false.
     */
    boolean isPushed();
}
