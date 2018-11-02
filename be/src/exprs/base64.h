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

#ifndef DORIS_BE_EXPRS_BASE64_H
#define DORIS_BE_EXPRS_BASE64_H

#include <stdio.h>
#include <stdint.h>

namespace doris {

int64_t base64_decode2(
        const char *data,
        size_t length,
        char *decoded_data);

size_t base64_encode2(const unsigned char *data,
                     size_t length,
                     unsigned char *encoded_data);

}
#endif
