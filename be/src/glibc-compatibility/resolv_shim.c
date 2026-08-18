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

// glibc >= 2.34 demoted the double-underscore resolver entry points
// (__res_nsearch & friends) to non-default compat versions, so newly linked
// binaries cannot bind them anymore. The prebuilt thirdparty krb5 archive
// (dnsglue.o) still references __res_nsearch, which breaks the doris_be link
// on Ubuntu 22.04 (glibc 2.35). Provide a thin forwarder to the public
// res_nsearch entry point, which is the identical implementation (same
// symbol address in libc).

#include <resolv.h>
#include <sys/types.h>

int __res_nsearch(res_state statp, const char* dname, int class_, int type,
                  unsigned char* answer, int anslen) {
    return res_nsearch(statp, dname, class_, type, answer, anslen);
}
