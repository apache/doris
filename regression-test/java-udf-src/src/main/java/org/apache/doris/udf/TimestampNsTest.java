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

package org.apache.doris.udf;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

public class TimestampNsTest {
    public static class Scalar {
        public LocalDateTime evaluate(LocalDateTime value) {
            return value;
        }
    }

    public static class Array {
        public ArrayList<LocalDateTime> evaluate(ArrayList<LocalDateTime> value) {
            return value;
        }
    }

    public static class Map {
        public HashMap<String, LocalDateTime> evaluate(HashMap<String, LocalDateTime> value) {
            return value;
        }
    }

    public static class Struct {
        public ArrayList<Object> evaluate(ArrayList<Object> value) {
            return value;
        }
    }
}
