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

package org.apache.doris.nereids.rules.exploration;

/**
 * JoinReorderContext for Duplicate free.
 * Paper:
 * - Optimizing Join Enumeration in Transformation-based Query Optimizers
 * - Improving Join Reorderability with Compensation Operators
 */
public class JoinReorderContext {
    // left deep tree
    private boolean hasCommute = false;
    private boolean hasLAsscom = false;

    // zig-zag tree
    private boolean hasCommuteZigZag = false;

    // bushy tree
    private boolean hasExchange = false;
    private boolean hasRightAssociate = false;
    private boolean hasLeftAssociate = false;

    public JoinReorderContext() {
    }

    /**
     * copy a JoinReorderContext.
     */
    public void copyFrom(JoinReorderContext joinReorderContext) {
        this.hasCommute = joinReorderContext.hasCommute;
        this.hasLAsscom = joinReorderContext.hasLAsscom;
        this.hasExchange = joinReorderContext.hasExchange;
        this.hasLeftAssociate = joinReorderContext.hasLeftAssociate;
        this.hasRightAssociate = joinReorderContext.hasRightAssociate;
        this.hasCommuteZigZag = joinReorderContext.hasCommuteZigZag;
    }

    /**
     * clear all.
     */
    public void clear() {
        hasCommute = false;
        hasLAsscom = false;
        hasCommuteZigZag = false;
        hasExchange = false;
        hasRightAssociate = false;
        hasLeftAssociate = false;
    }

    public boolean isHasCommute() {
        return hasCommute;
    }

    public void setHasCommute(boolean hasCommute) {
        this.hasCommute = hasCommute;
    }

    public boolean isHasLAsscom() {
        return hasLAsscom;
    }

    public void setHasLAsscom(boolean hasLAsscom) {
        this.hasLAsscom = hasLAsscom;
    }

    public boolean isHasExchange() {
        return hasExchange;
    }

    public void setHasExchange(boolean hasExchange) {
        this.hasExchange = hasExchange;
    }

    public boolean isHasRightAssociate() {
        return hasRightAssociate;
    }

    public void setHasRightAssociate(boolean hasRightAssociate) {
        this.hasRightAssociate = hasRightAssociate;
    }

    public boolean isHasLeftAssociate() {
        return hasLeftAssociate;
    }

    public void setHasLeftAssociate(boolean hasLeftAssociate) {
        this.hasLeftAssociate = hasLeftAssociate;
    }

    public boolean isHasCommuteZigZag() {
        return hasCommuteZigZag;
    }

    public void setHasCommuteZigZag(boolean hasCommuteZigZag) {
        this.hasCommuteZigZag = hasCommuteZigZag;
    }
}
