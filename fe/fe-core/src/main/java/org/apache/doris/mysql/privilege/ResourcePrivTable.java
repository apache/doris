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

package org.apache.doris.mysql.privilege;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * ResourcePrivTable saves all resources privs
 */
public class ResourcePrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(ResourcePrivTable.class);

    public void getPrivs(String resourceName, PrivBitSet savedPrivs) {
        // need check all entries, because may have 2 entries match resourceName,
        // For example, if the resourceName is g1, there are two entry `%` and `g1` compound requirements
        for (PrivEntry entry : entries) {
            ResourcePrivEntry resourcePrivEntry = (ResourcePrivEntry) entry;
            // check resource
            if (resourcePrivEntry.getResourcePattern().match(resourceName)) {
                savedPrivs.or(resourcePrivEntry.getPrivSet());
            }
        }
    }
}
