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

package org.apache.doris.nereids.datasets.ssb;

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;

import org.junit.jupiter.api.BeforeAll;

public abstract class SSBTestBase extends AnalyzeCheckTestBase {

    @BeforeAll
    public void beforeClass() {
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        SSBUtils.createTables(this);
    }
}
