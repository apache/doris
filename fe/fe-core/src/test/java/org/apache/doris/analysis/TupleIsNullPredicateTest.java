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

package org.apache.doris.analysis;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TupleIsNullPredicateTest {

    @Test
    public void testIsBoundToTuples() {
        List<TupleId> tupleIds = Lists.newArrayList();
        tupleIds.add(new TupleId(20));
        tupleIds.add(new TupleId(21));
        TupleIsNullPredicate tupleIsNullPredicate = new TupleIsNullPredicate(tupleIds);
        Assert.assertFalse(tupleIsNullPredicate.isBoundByTupleIds(Lists.newArrayList(new TupleId(1))));
    }
}
