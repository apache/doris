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

/**
 * Same contract as {@link RowFilterSpecTest}, for column masking: the SQL result cache compares the mask
 * recorded at plan time with the mask evaluated now, so equality must track the policy exactly - equal while
 * the policy is untouched, unequal the moment it is edited.
 */
public class DataMaskSpecTest {

    @Test
    public void testSpecsWithSameContentAreEqual() {
        DataMaskSpec planned = new DataMaskSpec("7:3", "CONCAT('XXXX', SUBSTR(phone, -4))");
        DataMaskSpec reevaluated = new DataMaskSpec("7:3", "CONCAT('XXXX', SUBSTR(phone, -4))");

        Assertions.assertEquals(planned, reevaluated,
                "an unchanged mask policy must re-evaluate to an equal spec, or the cache is evicted every lookup");
        Assertions.assertEquals(planned.hashCode(), reevaluated.hashCode());
    }

    @Test
    public void testUpdatedPolicyIsNotEqual() {
        DataMaskSpec planned = new DataMaskSpec("7:3", "NULL");

        Assertions.assertNotEquals(planned, new DataMaskSpec("7:4", "NULL"),
                "a new policy version must compare unequal so the cache invalidates");
        Assertions.assertNotEquals(planned, new DataMaskSpec("7:3", "phone"),
                "unmasking a column must compare unequal so the cache invalidates");
    }

    @Test
    public void testBlankMaskIsRejected() {
        // A blank mask expression would leave the raw column in the projection - i.e. no masking at all.
        Assertions.assertThrows(IllegalArgumentException.class, () -> new DataMaskSpec("7:3", " "));
        Assertions.assertThrows(NullPointerException.class, () -> new DataMaskSpec("7:3", null));
        Assertions.assertThrows(NullPointerException.class, () -> new DataMaskSpec(null, "NULL"));
    }
}
