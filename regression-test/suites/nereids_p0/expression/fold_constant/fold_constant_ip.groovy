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

suite("fold_constant_ip") {
    // cast function
    for (def ipv4 : ["1", "256.256.256.256", "192.168.1.10"]) {
        testFoldConst("SELECT cast('${ipv4}' as ipv4)")
        testFoldConst("SELECT cast(cast('${ipv4}' as ipv4) as string)")
        testFoldConst("SELECT cast(cast(cast('${ipv4}' as ipv4) as string) as ipv4)")
        testFoldConst("SELECT cast(cast(cast('${ipv4}' as ipv4) as string) as ipv4)")
    }
    for (def ipv6 : ["1", "ef8d:3d6a:869b:2582:7200:aa46:4dcd:2bd4"]) {
        testFoldConst("SELECT cast('${ipv6}' as ipv6)")
        testFoldConst("SELECT cast(cast('${ipv6}' as ipv6) as string)")
        testFoldConst("SELECT cast(cast(cast('${ipv6}' as ipv6) as string) as ipv6)")
        testFoldConst("SELECT cast(cast(cast('${ipv6}' as ipv6) as string) as ipv6)")
    }
}
