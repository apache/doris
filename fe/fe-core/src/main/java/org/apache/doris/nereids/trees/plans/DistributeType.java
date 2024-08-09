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

package org.apache.doris.nereids.trees.plans;

/**
 * Hints for join.
 * <p>
 * Hints for the right child of join are supported currently.
 * Left input and right input of join could have different hints for further extension.
 */
public enum DistributeType {
    NONE(JoinDistributeType.NONE, JoinDistributeType.NONE),
    BROADCAST_RIGHT(JoinDistributeType.NONE, JoinDistributeType.BROADCAST),
    SHUFFLE_RIGHT(JoinDistributeType.NONE, JoinDistributeType.SHUFFLE);

    /**
     * Join hint type for single join input plan.
     */
    public enum JoinDistributeType {
        // No join hint.
        NONE,
        // Shuffle join hint.
        SHUFFLE,
        // Broadcast join hint.
        BROADCAST,
    }

    private final JoinDistributeType leftHint;
    private final JoinDistributeType rightHint;

    DistributeType(JoinDistributeType leftHint, JoinDistributeType rightHint) {
        this.leftHint = leftHint;
        this.rightHint = rightHint;
    }
}
