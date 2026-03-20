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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * Shared immutable context for one FE-side incremental refresh attempt.
 */
public class IVMRefreshContext {
    private final MTMV mtmv;
    private final ConnectContext connectContext;
    private final MTMVRefreshContext mtmvRefreshContext;

    public IVMRefreshContext(MTMV mtmv, ConnectContext connectContext, MTMVRefreshContext mtmvRefreshContext) {
        this.mtmv = Objects.requireNonNull(mtmv, "mtmv can not be null");
        this.connectContext = Objects.requireNonNull(connectContext, "connectContext can not be null");
        this.mtmvRefreshContext = Objects.requireNonNull(mtmvRefreshContext, "mtmvRefreshContext can not be null");
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public MTMVRefreshContext getMtmvRefreshContext() {
        return mtmvRefreshContext;
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
        return Objects.equals(mtmv, that.mtmv)
                && Objects.equals(connectContext, that.connectContext)
                && Objects.equals(mtmvRefreshContext, that.mtmvRefreshContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mtmv, connectContext, mtmvRefreshContext);
    }

    @Override
    public String toString() {
        return "IVMRefreshContext{"
                + "mtmv=" + mtmv.getName()
                + ", mtmvRefreshContext=" + mtmvRefreshContext
                + '}';
    }
}
