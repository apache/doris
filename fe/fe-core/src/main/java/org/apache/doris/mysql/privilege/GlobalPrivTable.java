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

import org.apache.doris.mysql.privilege.PrivObject.PrivObjectType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/*
 * GlobalPrivTable saves all global privs and also password for users
 */
public class GlobalPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(GlobalPrivTable.class);

    public GlobalPrivTable() {
    }

    public void getPrivs(PrivBitSet savedPrivs) {
        if (CollectionUtils.isEmpty(entries)) {
            return;
        }
        // GlobalPrivTable saves global permissions.
        // Unlike CatalogPrivTable, it needs to save an entry for each catalog,
        // so the length of entries can only be 1 at most.
        Preconditions.checkArgument(entries.size() == 1);
        savedPrivs.or(entries.get(0).getPrivSet());
    }

    public List<PrivObject> getPrivObjects() {
        List<PrivObject> res = Lists.newArrayList();
        if (CollectionUtils.isEmpty(entries)) {
            return res;
        }
        PrivBitSet privSet = entries.get(0).getPrivSet();
        if (!privSet.isEmpty()) {
            res.add(new PrivObject(null, null, null, null, PrivObjectType.GLOBAL, privSet.toPrivilegeNames()));
        }
        return res;
    }
}
