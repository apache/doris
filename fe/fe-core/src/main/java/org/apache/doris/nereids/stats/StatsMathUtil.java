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

package org.apache.doris.nereids.stats;

import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;

/**
 * Math util for statistics derivation
 */
public class StatsMathUtil {

    public static double nonZeroDivisor(double d) {
        return d == 0.0 ? 1 : d;
    }

    /**
     *  Try to find non NaN min.
     */
    public static double minNonNaN(double a, double b) {
        if (Double.isNaN(a)) {
            return b;
        }
        if (Double.isNaN(b)) {
            return a;
        }
        return Math.min(a, b);
    }

    /**
     *  Try to find non NaN max.
     */
    public static double maxNonNaN(double a, double b) {
        if (Double.isNaN(a)) {
            return b;
        }
        if (Double.isNaN(b)) {
            return a;
        }
        return Math.max(a, b);
    }

    public static double divide(double a, double b) {
        if (Double.isNaN(a) || Double.isNaN(b)) {
            return Double.NaN;
        }
        return a / nonZeroDivisor(b);
    }

    /**
     * compute the multi columns unite ndv
     */
    public static double jointNdv(List<Double> ndvs) {
        if (CollectionUtils.isEmpty(ndvs)) {
            return -1;
        }
        if (ndvs.stream().anyMatch(n -> n <= 0)) {
            return -1;
        }
        ndvs.sort(Collections.reverseOrder());
        double multiNdv = 1;
        for (int i = 0; i < ndvs.size(); i++) {
            multiNdv = multiNdv * Math.pow(ndvs.get(i), 1 / Math.pow(2, i));
        }
        return multiNdv;
    }
}
