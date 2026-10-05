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

package org.apache.doris.foundation.type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ResultOrTest {

    @Test
    public void testOk() {
        ResultOr<String, String> result = ResultOr.ok("value");
        Assertions.assertTrue(result.isOk());
        Assertions.assertFalse(result.isErr());
        Assertions.assertEquals("value", result.unwrap());
        Assertions.assertEquals("Ok(value)", result.toString());
    }

    @Test
    public void testErr() {
        ResultOr<String, String> result = ResultOr.err("boom");
        Assertions.assertTrue(result.isErr());
        Assertions.assertFalse(result.isOk());
        Assertions.assertEquals("boom", result.unwrapErr());
        Assertions.assertEquals("Err(boom)", result.toString());
    }

    @Test
    public void testUnwrapErrOnOkThrows() {
        ResultOr<String, String> result = ResultOr.ok("value");
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class, result::unwrapErr);
        Assertions.assertEquals("Tried to unwrapOk", ex.getMessage());
    }

    @Test
    public void testUnwrapOnErrThrows() {
        ResultOr<String, String> result = ResultOr.err("boom");
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class, result::unwrap);
        Assertions.assertEquals("Tried to unwrap Err: boom", ex.getMessage());
    }
}
