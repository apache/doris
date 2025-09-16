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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Predicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;

public abstract class Predicate extends Expr {
    protected boolean isEqJoinConjunct;

    public Predicate() {
        super();
        type = Type.BOOLEAN;
        this.isEqJoinConjunct = false;
    }

    protected Predicate(Predicate other) {
        super(other);
        type = other.type;
        isEqJoinConjunct = other.isEqJoinConjunct;
    }

    public void setIsEqJoinConjunct(boolean v) {
        isEqJoinConjunct = v;
    }

    /**
     * Returns true if one of the children is a slotref (possibly wrapped in a cast)
     * and the other children are all constant. Returns the slotref in 'slotRef' and
     * its child index in 'idx'.
     * This will pick up something like "col = 5", but not "2 * col = 10", which is
     * what we want.
     */
    public boolean isSingleColumnPredicate(Reference<SlotRef> slotRefRef, Reference<Integer> idxRef) {
        // find slotref
        SlotRef slotRef = null;
        int i = 0;
        for (; i < children.size(); ++i) {
            slotRef = getChild(i).unwrapSlotRef();
            if (slotRef != null) {
                break;
            }
        }
        if (slotRef == null) {
            return false;
        }

        // make sure everything else is constant
        for (int j = 0; j < children.size(); ++j) {
            if (i == j) {
                continue;
            }
            if (!getChild(j).isConstant()) {
                return false;
            }
        }

        if (slotRefRef != null) {
            slotRefRef.setRef(slotRef);
        }
        if (idxRef != null) {
            idxRef.setRef(Integer.valueOf(i));
        }
        return true;
    }


    /**
     * If predicate is of the form "<slotref> = <slotref>", returns both SlotRefs,
     * otherwise returns null.
     */
    public Pair<SlotId, SlotId> getEqSlots() {
        return null;
    }
}
