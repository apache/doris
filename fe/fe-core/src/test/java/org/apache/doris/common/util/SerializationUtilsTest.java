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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

public class SerializationUtilsTest {


    private final HashMap<String, Object> sampleMap = new HashMap<>();
    private final String sampleString = "sampleString";
    private final Integer sampleInteger = 456;

    public SerializationUtilsTest() {
        sampleMap.put("KEY_ONE", sampleString);
        sampleMap.put("KEY_TWO", sampleInteger);
    }

    @Test
    public void testClone() {
        final Object clonedObject = SerializationUtils.clone(sampleMap);
        Assertions.assertNotNull(clonedObject);
        Assertions.assertTrue(clonedObject instanceof HashMap<?, ?>);
        Assertions.assertNotSame(clonedObject, sampleMap);

        final HashMap<?, ?> clonedMap = (HashMap<?, ?>) clonedObject;
        Assertions.assertEquals(sampleString, clonedMap.get("KEY_ONE"));
        Assertions.assertNotSame(sampleString, clonedMap.get("KEY_ONE"));
        Assertions.assertEquals(sampleInteger, clonedMap.get("KEY_TWO"));
        Assertions.assertNotSame(sampleInteger, clonedMap.get("KEY_TWO"));
        Assertions.assertEquals(sampleMap, clonedMap);
    }

    @Test
    public void testCloneNull() {
        final Object clonedObject = SerializationUtils.clone(null);
        Assertions.assertNull(clonedObject);
    }

    @Test
    public void testCloneWithWriteReplace() {
        Child childObject = new Child(true);
        Parent clonedParent = SerializationUtils.clone(childObject);

        Assertions.assertNotSame(childObject, clonedParent);
        Assertions.assertEquals(new Parent(true), clonedParent);
    }

    static class Parent implements Serializable {
        private static final long serialVersionUID = 1L;
        protected boolean status;

        Parent(boolean status) {
            this.status = status;
        }

        protected Parent(Parent parent) {
            this.status = parent.status;
        }

        protected Object writeReplace() {
            return new Parent(this);
        }

        @Override
        public int hashCode() {
            return Objects.hash(status);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Parent other = (Parent) obj;
            return status == other.status;
        }
    }

    static class Child extends Parent {
        private static final long serialVersionUID = 2L;

        Child(boolean status) {
            super(status);
        }
    }
}
