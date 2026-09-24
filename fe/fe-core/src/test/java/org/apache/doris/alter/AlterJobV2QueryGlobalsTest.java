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

package org.apache.doris.alter;

import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * An alter job's tasks carry TQueryGlobals, whose now_string a backend requires when it decodes the
 * task. A job created by a frontend daemon (InternalSchemaInitializer upgrading the audit table)
 * has no ConnectContext; it used to send an empty TQueryGlobals, and the backend cancelled the job.
 */
public class AlterJobV2QueryGlobalsTest {

    @Test
    public void testJobWithoutSessionCarriesCompleteQueryGlobals() {
        ConnectContext.remove();
        Assertions.assertNull(ConnectContext.get());

        SchemaChangeJobV2 job = new SchemaChangeJobV2("", 1L, 2L, 3L, "audit_log", 1000L);

        Assertions.assertTrue(job.queryGlobals.isSetNowString());
        Assertions.assertTrue(job.queryGlobals.isSetTimestampMs());
        Assertions.assertTrue(job.queryGlobals.isSetTimeZone());
        Assertions.assertNotNull(job.queryOptions);
    }
}
