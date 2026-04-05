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

package org.apache.doris.filesystem.hdfs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class SimpleHadoopAuthenticatorTest {

    @Test
    void noUserDoAsExecutesDirectly() throws IOException {
        SimpleHadoopAuthenticator auth = new SimpleHadoopAuthenticator();
        String result = auth.doAs(() -> "hello");
        Assertions.assertEquals("hello", result);
    }

    @Test
    void nullUserDoAsExecutesDirectly() throws IOException {
        SimpleHadoopAuthenticator auth = new SimpleHadoopAuthenticator(null);
        String result = auth.doAs(() -> "world");
        Assertions.assertEquals("world", result);
    }

    @Test
    void emptyUserDoAsExecutesDirectly() throws IOException {
        SimpleHadoopAuthenticator auth = new SimpleHadoopAuthenticator("");
        int result = auth.doAs(() -> 42);
        Assertions.assertEquals(42, result);
    }

    @Test
    void withUserDoAsExecutesThroughUgi() throws IOException {
        SimpleHadoopAuthenticator auth = new SimpleHadoopAuthenticator("testuser");
        String result = auth.doAs(() -> "authenticated");
        Assertions.assertEquals("authenticated", result);
    }

    @Test
    void doAsPropagatesIOException() {
        SimpleHadoopAuthenticator auth = new SimpleHadoopAuthenticator();
        Assertions.assertThrows(IOException.class, () -> auth.doAs(() -> {
            throw new IOException("test error");
        }));
    }

    @Test
    void doAsWithUserPropagatesIOException() {
        SimpleHadoopAuthenticator auth = new SimpleHadoopAuthenticator("testuser");
        Assertions.assertThrows(IOException.class, () -> auth.doAs(() -> {
            throw new IOException("test error");
        }));
    }
}
