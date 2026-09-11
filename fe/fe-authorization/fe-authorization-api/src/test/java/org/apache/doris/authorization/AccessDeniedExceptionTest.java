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

package org.apache.doris.authorization;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AccessDeniedExceptionTest {

    private static final AuthorizedSubject SUBJECT = AuthorizedSubject.of("someone", "%");
    private static final AuthorizedResource TABLE = AuthorizedResource.table("ctl", "db", "tbl");

    /**
     * Refusal is the ordinary answer, not an error: listing what a user may see asks about every object there
     * is and is refused for most of them. Recording a stack trace for each one would cost more than the
     * decisions themselves, so this exception must not have one - and nothing in the class can quietly
     * reintroduce it.
     */
    @Test
    public void testARefusalCarriesNoStackTrace() {
        AccessDeniedException denied = AccessDeniedException.of(SUBJECT, TABLE,
                AccessRequirement.of(AccessAction.SELECT), "somewhere");

        Assertions.assertEquals(0, denied.getStackTrace().length);
    }

    @Test
    public void testARefusalSaysWhoWasRefusedWhatAndByWhom() {
        AccessDeniedException denied = AccessDeniedException.of(SUBJECT, TABLE,
                AccessRequirement.of(AccessAction.SELECT), "ranger-doris");

        Assertions.assertSame(TABLE, denied.getResource());
        Assertions.assertEquals(AccessRequirement.of(AccessAction.SELECT), denied.getRequirement().orElse(null));
        Assertions.assertEquals("ranger-doris", denied.getDeniedBy().orElse(null));
        String message = denied.getMessage();
        Assertions.assertTrue(message.contains("'someone'@'%'"), message);
        Assertions.assertTrue(message.contains("SELECT"), message);
        Assertions.assertTrue(message.contains("ctl.db.tbl"), message);
        Assertions.assertTrue(message.contains("ranger-doris"), message);
    }

    /**
     * Some refusals are already worded, and the wording is the answer - a column check refuses by naming the
     * column that failed. Recomposing the message from the requirement would reduce "this column" to "these
     * columns".
     */
    @Test
    public void testAWordedRefusalIsReportedAsWritten() {
        AccessDeniedException denied = AccessDeniedException.withMessage(
                "Permission denied on column [salary]", TABLE, "default");

        Assertions.assertEquals("Permission denied on column [salary]", denied.getMessage());
        Assertions.assertFalse(denied.getRequirement().isPresent());
    }

    /** A source that does not name itself still produces a usable message. */
    @Test
    public void testAnAnonymousRefusalStillReads() {
        AccessDeniedException denied = AccessDeniedException.of(SUBJECT, AuthorizedResource.global(),
                AccessRequirement.of(AccessAction.ADMIN), null);

        Assertions.assertFalse(denied.getDeniedBy().isPresent());
        Assertions.assertTrue(denied.getMessage().contains("global"), denied.getMessage());
    }
}
