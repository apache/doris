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

suite("test_encode_decode") {
    test {
        sql "select encode('text', 'GBK')"
        exception "Unsupported character set"
    }

    test {
        sql "select encode('中', 'US-ASCII')"
        exception "Character conversion using 'US-ASCII' failed"
    }

    test {
        sql "select decode(X'E4B8', 'UTF-8')"
        exception "Character conversion using 'UTF-8' failed"
    }
}
