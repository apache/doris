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

package org.apache.doris.plugin.audit;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.plugin.AuditEvent;

import org.junit.Assert;
import org.junit.Test;

public class AuditLogBuilderTest {

    @Test
    public void testTimestampOutput() {
        AuditLogBuilder auditLogBuilder = new AuditLogBuilder();
            // 1 set a valid value
            {
                long currentTime = 1741760376000L;
                AuditEvent auditEvent = new AuditEvent.AuditEventBuilder()
                        .setTimestamp(currentTime).build();
                String result = Deencapsulation.invoke(auditLogBuilder, "getAuditLogString", auditEvent);
                Assert.assertTrue(result.contains("Timestamp=2025-03-12 14:19:36.000"));
            }

            // 2 not set value
            {
                AuditEvent auditEvent = new AuditEvent.AuditEventBuilder().build();
                String result = Deencapsulation.invoke(auditLogBuilder, "getAuditLogString", auditEvent);
                Assert.assertTrue(result.contains("Timestamp=\\N"));
            }
    }

}
