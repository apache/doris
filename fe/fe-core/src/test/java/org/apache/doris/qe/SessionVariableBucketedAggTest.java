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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SessionVariableBucketedAggTest {

    @Test
    void testDefaultAndForwarding() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        Assertions.assertEquals(0, sessionVariable.bucketedAggNumBuckets);
        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.BUCKETED_AGG_NUM_BUCKETS, new StringLiteral("16")));

        SessionVariable forwarded = new SessionVariable();
        forwarded.setForwardedSessionVariables(sessionVariable.getForwardVariables());
        Assertions.assertEquals(16, forwarded.bucketedAggNumBuckets);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "1", "2", "4", "8", "16", "32", "64", "128", "256"})
    void testAcceptsAutomaticAndPowersOfTwo(String value) throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.BUCKETED_AGG_NUM_BUCKETS, new StringLiteral(value)));
        Assertions.assertEquals(Integer.parseInt(value), sessionVariable.bucketedAggNumBuckets);
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "-2147483648", "3", "7", "255", "257", "512", "2147483648", "1.5", "auto", ""})
    void testRejectsInvalidBucketCounts(String value) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.bucketedAggNumBuckets = 16;
        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                        SessionVariable.BUCKETED_AGG_NUM_BUCKETS, new StringLiteral(value))));
        Assertions.assertTrue(exception.getMessage().contains(
                "bucketed_agg_num_buckets must be 0 or a power of two between 1 and 256"));
        Assertions.assertEquals(16, sessionVariable.bucketedAggNumBuckets);
    }
}
