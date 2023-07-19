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

package org.apache.doris.nereids.datasets.tpch;

import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.common.FeConstants;

public abstract class TPCHTestBase extends AnalyzeCheckTestBase {
    @Override
    protected void runBeforeAll() throws Exception {
        // The internal table for TPCHTestBase is constructed in order to facilitate
        // the execution of certain tests that require the invocation of a deriveStats job.
        // This deriveStats job is responsible for retrieving statistics from the aforementioned
        // internal table.
        FeConstants.disableInternalSchemaDb = false;
        new InternalSchemaInitializer().start();
        createDatabase("tpch");
        connectContext.setDatabase("default_cluster:tpch");
        TPCHUtils.createTables(this);
    }
}
