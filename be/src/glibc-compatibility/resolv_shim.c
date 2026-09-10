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
//
// The shim must only exist where it is needed: on glibc < 2.34,
// __res_nsearch is still a default-versioned libc symbol, and <resolv.h>
// there #defines res_nsearch as __res_nsearch, which would fold the
// forwarder below into infinite self-recursion (clang -Winfinite-recursion
// errors out under -Werror, e.g. on the AlmaLinux 8 / glibc 2.28 CI image).

#include <resolv.h>
#include <sys/types.h>

#if defined(__GLIBC__) && __GLIBC_PREREQ(2, 34)

int __res_nsearch(res_state statp, const char* dname, int class_, int type,
                  unsigned char* answer, int anslen) {
    return res_nsearch(statp, dname, class_, type, answer, anslen);
}

#else

// Keep the translation unit non-empty (-Wpedantic forbids an empty one);
// no shim is required on glibc < 2.34.
typedef int doris_resolv_shim_unused_t;

#endif
