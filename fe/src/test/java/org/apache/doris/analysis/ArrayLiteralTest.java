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

package org.apache.doris.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.doris.common.io.DataOutputBuffer;
import org.junit.Test;

public class ArrayLiteralTest {

    @Test
    public void testArrayLiteralSerialization() throws IOException {

        DataOutputBuffer dob = new DataOutputBuffer();
        DataOutputStream dos = new DataOutputStream(dob);

        StringLiteral[] list = new StringLiteral[10];

        for (int i = 0; i < 10; i++) {
            list[i] = new StringLiteral("test" + i);
        }

        ArrayLiteral literal = new ArrayLiteral(list);

        literal.write(dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dob.getData()));

        ArrayLiteral l = ArrayLiteral.read(dis);

        for (int i = 0; i < 10; i++) {
            assertTrue(l.getChild(i).isLiteral());
            assertEquals(list[i], l.getChild(i));
        }

    }

}
