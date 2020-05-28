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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Text;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/*
 * ResourcePrivTable saves all resources privs
 */
public class ResourcePrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(ResourcePrivTable.class);

    /*
     * Return first priv which match the user@host on resourceName The returned priv will be
     * saved in 'savedPrivs'.
     */
    public void getPrivs(UserIdentity currentUser, String resourceName, PrivBitSet savedPrivs) {
        ResourcePrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            ResourcePrivEntry resourcePrivEntry = (ResourcePrivEntry) entry;

            if (!resourcePrivEntry.match(currentUser, true)) {
                continue;
            }

            // check resource
            if (!resourcePrivEntry.getResourcePattern().match(resourceName)) {
                continue;
            }

            matchedEntry = resourcePrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = ResourcePrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }
}
