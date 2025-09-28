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

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class SafeStringBuilderTest {
    private SafeStringBuilder builder;
    private final int TEST_MAX_CAPACITY = 100;

    @Before
    public void setUp() {
        builder = new SafeStringBuilder(TEST_MAX_CAPACITY);
    }

    @Test
    public void testDefaultConstructor() {
        SafeStringBuilder defaultBuilder = new SafeStringBuilder();
        assertEquals(Integer.MAX_VALUE - 16, defaultBuilder.getMaxCapacity());
    }

    @Test
    public void testConstructorWithSmallCapacity() {
        SafeStringBuilder smallBuilder = new SafeStringBuilder(10);
        assertEquals(0, smallBuilder.getMaxCapacity());
    }

    @Test
    public void testAppendStringWithinCapacity() {
        String testString = "Hello";
        builder.append(testString);
        assertEquals(testString, builder.toString());
        assertFalse(builder.isTruncated());
    }

    @Test
    public void testMultipleAppendsWithinCapacity() {
        builder.append("Hello").append(" ").append("World");
        assertEquals("Hello World", builder.toString());
        assertFalse(builder.isTruncated());
    }

    @Test
    public void testAppendStringExceedingCapacity() {
        String fillString = repeat('X', TEST_MAX_CAPACITY - 5);
        builder.append(fillString);

        String exceedString = "123456";
        builder.append(exceedString);

        // Should be truncated to exactly max capacity
        assertEquals(TEST_MAX_CAPACITY - 16, builder.length());
        assertTrue(builder.isTruncated());
        assertTrue(builder.toString().endsWith("...[TRUNCATED]"));
    }

    @Test
    public void testAppendObject() {
        Object testObj = new Object() {
            @Override
            public String toString() {
                return "TestObject";
            }
        };
        builder.append(testObj);
        assertEquals("TestObject", builder.toString());
    }

    @Test
    public void testLength() {
        assertEquals(0, builder.length());
        builder.append("123");
        assertEquals(3, builder.length());
    }

    @Test
    public void testToStringNotTruncated() {
        builder.append("Normal string");
        assertEquals("Normal string", builder.toString());
    }

    @Test
    public void testToStringTruncated() {
        // Force truncation
        builder.append(repeat('X', TEST_MAX_CAPACITY - 5));
        assertTrue(builder.toString().endsWith("...[TRUNCATED]"));
    }

    @Test
    public void testAppendAfterTruncation() {
        // First append that causes truncation
        builder.append(repeat('X', TEST_MAX_CAPACITY + 1));
        assertTrue(builder.isTruncated());

        // Subsequent append should be ignored
        builder.append("This should not appear");
        assertTrue(builder.toString().endsWith("...[TRUNCATED]"));
        assertFalse(builder.toString().contains("This should not appear"));
    }

    @Test
    public void testExactCapacity() {
        String exactString = repeat('X', TEST_MAX_CAPACITY - 16);
        builder.append(exactString);
        assertEquals(exactString, builder.toString());
        assertFalse(builder.isTruncated());
    }

    private String repeat(char c, int count) {
        char[] chars = new char[count];
        for (int i = 0; i < count; i++) {
            chars[i] = c;
        }
        return new String(chars);
    }
}
