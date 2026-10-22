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

public class AuthorizedSubjectTest {

    /**
     * All three parts identify the account. Two accounts differing only in where they connect from are
     * different accounts holding different grants, and a subject that compared equal across that difference
     * would let a source answer for the wrong one.
     */
    @Test
    public void testAnAccountIsItsNameItsHostAndHowTheHostIsRead() {
        AuthorizedSubject anywhere = AuthorizedSubject.of("user", "%");

        Assertions.assertEquals(AuthorizedSubject.of("user", "%"), anywhere);
        Assertions.assertEquals(AuthorizedSubject.of("user", "%").hashCode(), anywhere.hashCode());
        Assertions.assertNotEquals(AuthorizedSubject.of("other", "%"), anywhere);
        Assertions.assertNotEquals(AuthorizedSubject.of("user", "192.168.%"), anywhere);
        Assertions.assertNotEquals(AuthorizedSubject.of("user", "%", true), anywhere);
    }

    @Test
    public void testAHostReadAsADomainSaysSo() {
        Assertions.assertFalse(AuthorizedSubject.of("user", "doris.example.com").isDomain());
        Assertions.assertTrue(AuthorizedSubject.of("user", "doris.example.com", true).isDomain());
    }

    /** Printed the way an account is written in Doris, because it ends up in refusal messages. */
    @Test
    public void testAnAccountPrintsAsAnAccount() {
        Assertions.assertEquals("'user'@'192.168.%'", AuthorizedSubject.of("user", "192.168.%").toString());
    }

    @Test
    public void testAnIncompleteAccountIsRefused() {
        Assertions.assertThrows(NullPointerException.class, () -> AuthorizedSubject.of(null, "%"));
        Assertions.assertThrows(NullPointerException.class, () -> AuthorizedSubject.of("user", null));
    }
}
