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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.info.TableNameInfo;

import java.util.Objects;
import java.util.Set;

/**
 * Shared immutable context for one FE-side incremental refresh attempt.
 */
public class IVMRefreshContext {
    private final TableNameInfo mvName;
    private final Set<TableNameInfo> changedBaseTables;

    public IVMRefreshContext(TableNameInfo mvName, Set<TableNameInfo> changedBaseTables) {
        this.mvName = Objects.requireNonNull(mvName, "mvName can not be null");
        this.changedBaseTables = Objects.requireNonNull(changedBaseTables, "changedBaseTables can not be null");
    }

    public TableNameInfo getMvName() {
        return mvName;
    }

    public Set<TableNameInfo> getChangedBaseTables() {
        return changedBaseTables;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IVMRefreshContext that = (IVMRefreshContext) o;
        return Objects.equals(mvName, that.mvName)
                && Objects.equals(changedBaseTables, that.changedBaseTables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mvName, changedBaseTables);
    }

    @Override
    public String toString() {
        return "IVMRefreshContext{"
                + "mvName=" + mvName
                + ", changedBaseTables=" + changedBaseTables
                + '}';
    }
}
