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

package org.apache.doris.cooldown;

import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Delete file conf for cooldown.
 */
public class CooldownDelete {
    private static final Logger LOG = LogManager.getLogger(CooldownDelete.class);

    private long beId;
    private long tabletId;
    private TUniqueId deleteId;

    public CooldownDelete(long beId, long tabletId, TUniqueId deleteId) {
        this.beId = beId;
        this.tabletId = tabletId;
        this.deleteId = deleteId;
    }

    public long getBeId() {
        return beId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public TUniqueId getDeleteId() {
        return deleteId;
    }
}
