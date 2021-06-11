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

package org.apache.doris.planner;

import com.google.common.collect.BoundType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BoundExpander<T extends Comparable<T>> {
    private static final Logger LOG = LogManager.getLogger(BoundExpander.class);

    protected T lowerBound;
    protected T upperBound;
    protected BoundType lowerBoundType;
    protected BoundType upperBoundType;

    protected final void updateLowerBound(T newLowerBound, BoundType newLowerBoundType) {
        if (null == lowerBound) {
            lowerBound = newLowerBound;
            lowerBoundType = newLowerBoundType;
        } else {
            int ret = newLowerBound.compareTo(lowerBound);
            if (ret < 0) {
                lowerBound = newLowerBound;
                lowerBoundType = newLowerBoundType;
            } else if (ret == 0) {
                if (newLowerBoundType == BoundType.CLOSED) {
                    lowerBoundType = newLowerBoundType;
                }
            }
        }
    }

    protected final void updateUpperBound(T newUpperBound, BoundType newUpperBoundType) {
        if (null == upperBound) {
            upperBound = newUpperBound;
            upperBoundType = newUpperBoundType;
        } else {
            int ret = newUpperBound.compareTo(upperBound);
            if (ret > 0) {
                upperBound = newUpperBound;
                upperBoundType = newUpperBoundType;
            } else if (ret == 0) {
                if (newUpperBoundType == BoundType.CLOSED) {
                    upperBoundType = newUpperBoundType;
                }
            }
        }
    }

}
