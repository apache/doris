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

package org.apache.doris.optimizer.base;

import org.apache.doris.optimizer.OptUtils;

public class OptOrderItem {
    private OptColumnRef columnRef;
    private boolean isAsc;
    private boolean isNullFirst;

    public OptOrderItem(OptColumnRef columnRef, boolean isAsc, boolean isNullFirst) {
        this.columnRef = columnRef;
        this.isAsc = isAsc;
        this.isNullFirst = isNullFirst;
    }

    public OptColumnRef  getColumnRef() { return columnRef; }
    public boolean isAsc() { return isAsc; }
    public boolean isNullFirst() { return isNullFirst; }

    public boolean matches(OptOrderItem rhs) {
        return isAsc == rhs.isAsc && isNullFirst == rhs.isNullFirst && columnRef.equals(rhs.columnRef);
    }

    @Override
    public int hashCode() {
        int code = columnRef.hashCode();
        code = OptUtils.combineHash(code, isAsc);
        code = OptUtils.combineHash(code, isNullFirst);
        return code;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof OptOrderItem)) {
            return false;
        }
        return matches((OptOrderItem) object);
    }
}
