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

package org.apache.doris.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UtilTest {

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithMessageNoSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        Assertions.assertEquals("java.lang.Exception: Root cause message",
                Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithMessageWithSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        rootCause.addSuppressed(new Exception("Suppressed message"));
        Assertions.assertEquals(
                "java.lang.Exception: Root cause message With suppressed[0]:Suppressed message",
                Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithMessageWithMultiSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        rootCause.addSuppressed(new Exception("Suppressed message"));
        rootCause.addSuppressed(new Exception("Suppressed message2"));
        Assertions.assertEquals(
                "java.lang.Exception: Root cause message"
                            + " With suppressed[0]:Suppressed message"
                            + " With suppressed[1]:Suppressed message2",
                Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithoutMessageNoSuppressed() {
        Exception rootCause = new Exception();
        Assertions.assertEquals("java.lang.Exception", Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithoutMessageWithSuppressed() {
        Exception rootCause = new Exception();
        rootCause.addSuppressed(new Exception("Suppressed message"));
        Assertions.assertEquals(
                "java.lang.Exception With suppressed[0]:Suppressed message",
                Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageChainedExceptionWithChainedSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        Exception chainedException = new Exception("Chained exception", rootCause);
        chainedException.addSuppressed(new Exception("Suppressed message"));
        Assertions.assertEquals("java.lang.Exception: Root cause message",
                Util.getRootCauseWithSuppressedMessage(chainedException));
    }

    @Test
    public void getRootCauseWithSuppressedMessageChainedExceptionWithCauseSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        Exception chainedException = new Exception("Chained exception", rootCause);
        rootCause.addSuppressed(new Exception("Suppressed message"));
        Assertions.assertEquals(
                "java.lang.Exception: Root cause message With suppressed[0]:Suppressed message",
                Util.getRootCauseWithSuppressedMessage(chainedException));
    }

    @Test
    public void sha256longEcoding() {
        String str = "东南卫视";
        String str1 = "东方卫视";
        Assertions.assertNotEquals(Util.sha256long(str), Util.sha256long(str1));
    }
    
    @Test
    public void sha256longHandlesLongMinValue() {
	String testStr = "test_long_min_value_case";
	long result = Util.sha256long(testStr);

	Assertions.assertNotEquals(Long.MIN_VALUE, result,
                "sha256long should not return Long.MIN_VALUE");

	Assertions.assertEquals(result, Util.sha256long(testStr),
                "Same input should produce same output");
    }
}
