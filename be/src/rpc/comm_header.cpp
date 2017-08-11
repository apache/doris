// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "compat.h"
#include "checksum.h"
#include "error.h"
#include "serialization.h"
#include "common/logging.h"
#include "comm_header.h"

namespace palo {

void CommHeader::encode(uint8_t **bufp) {
    uint8_t *base = *bufp;
    serialization::encode_i8(bufp, version);
    serialization::encode_i8(bufp, header_len);
    serialization::encode_i16(bufp, alignment);
    serialization::encode_i16(bufp, flags);
    serialization::encode_i32(bufp, 0);
    serialization::encode_i32(bufp, id);
    serialization::encode_i32(bufp, gid);
    serialization::encode_i32(bufp, total_len);
    serialization::encode_i32(bufp, timeout_ms);
    serialization::encode_i32(bufp, payload_checksum);
    serialization::encode_i64(bufp, command);
    // compute and serialize header checksum
    header_checksum = fletcher32(base, (*bufp)-base);
    base += 6;
    serialization::encode_i32(&base, header_checksum);
}

void CommHeader::decode(const uint8_t **bufp, size_t *remainp) {
    const uint8_t *base = *bufp;
    if (*remainp < FIXED_LENGTH)
        HT_THROWF(error::COMM_BAD_HEADER,
                  "Header size %d is less than the minumum fixed length %d",
                  (int)*remainp, (int)FIXED_LENGTH);
    HT_TRY("decoding comm header",
           version = serialization::decode_i8(bufp, remainp);
           header_len = serialization::decode_i8(bufp, remainp);
           alignment = serialization::decode_i16(bufp, remainp);
           flags = serialization::decode_i16(bufp, remainp);
           header_checksum = serialization::decode_i32(bufp, remainp);
           id = serialization::decode_i32(bufp, remainp);
           gid = serialization::decode_i32(bufp, remainp);
           total_len = serialization::decode_i32(bufp, remainp);
           timeout_ms = serialization::decode_i32(bufp, remainp);
           payload_checksum = serialization::decode_i32(bufp, remainp);
           command = serialization::decode_i64(bufp, remainp));
    memset((void *)(base+6), 0, 4);
    uint32_t checksum = fletcher32(base, *bufp-base);
    if (checksum != header_checksum)
        HT_THROWF(error::COMM_HEADER_CHECKSUM_MISMATCH, "%u != %u", checksum,
                  header_checksum);
}

} //namespace palo
