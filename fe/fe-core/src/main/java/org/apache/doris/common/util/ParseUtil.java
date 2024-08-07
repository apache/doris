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

package org.apache.doris.common.util;

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseUtil {
    private static ImmutableMap<String, Long> validDataVolumnUnitMultiplier =
            ImmutableMap.<String, Long>builder().put("B", 1L)
            .put("K", 1024L)
            .put("KB", 1024L)
            .put("M", 1024L * 1024)
            .put("MB", 1024L * 1024)
            .put("G", 1024L * 1024 * 1024)
            .put("GB", 1024L * 1024 * 1024)
            .put("T", 1024L * 1024 * 1024 * 1024)
            .put("TB", 1024L * 1024 * 1024 * 1024)
            .put("P", 1024L * 1024 * 1024 * 1024 * 1024)
            .put("PB", 1024L * 1024 * 1024 * 1024 * 1024).build();

    private static Pattern dataVolumnPattern = Pattern.compile("(\\d+)(\\D*)");

    public static long analyzeDataVolume(String dataVolumnStr) throws AnalysisException {
        long dataVolumn = 0;
        Matcher m = dataVolumnPattern.matcher(dataVolumnStr);
        if (m.matches()) {
            try {
                dataVolumn = Long.parseLong(m.group(1));
            } catch (NumberFormatException nfe) {
                throw new AnalysisException("invalid data volumn:" + m.group(1));
            }
            if (dataVolumn <= 0L) {
                throw new AnalysisException("Data volumn must larger than 0");
            }

            String unit = "B";
            String tmpUnit = m.group(2);
            if (!Strings.isNullOrEmpty(tmpUnit)) {
                unit = tmpUnit.toUpperCase();
            }
            if (validDataVolumnUnitMultiplier.containsKey(unit)) {
                dataVolumn = dataVolumn * validDataVolumnUnitMultiplier.get(unit);
            } else {
                throw new AnalysisException("invalid unit:" + tmpUnit);
            }
        } else {
            throw new AnalysisException("invalid data volumn expression:" + dataVolumnStr);
        }
        return dataVolumn;
    }

    public static long analyzeReplicaNumber(String replicaNumberStr) throws AnalysisException {
        long replicaNumber = 0;
        try {
            replicaNumber = Long.parseLong(replicaNumberStr);
        } catch (NumberFormatException nfe) {
            throw new AnalysisException("invalid data volumn:" + replicaNumberStr);
        }
        if (replicaNumber <= 0L) {
            throw new AnalysisException("Replica volumn must larger than 0");
        }
        return replicaNumber;
    }

    public static long analyzeTransactionNumber(String transactionNumberStr) throws AnalysisException {
        long transactionNumber = 0;
        try {
            transactionNumber = Long.parseLong(transactionNumberStr);
        } catch (NumberFormatException nfe) {
            throw new AnalysisException("invalid data volumn:" + transactionNumberStr);
        }
        if (transactionNumber <= 0L) {
            throw new AnalysisException("Transaction quota size must larger than 0");
        }
        return transactionNumber;
    }

}
