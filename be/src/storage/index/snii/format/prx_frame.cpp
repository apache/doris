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

#include "storage/index/snii/format/prx_frame.h"

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/prx_pod.h"

namespace doris::snii::format {

Status read_prx_frame(ByteSource* source, PrxFrameView* frame) {
    const size_t start = source->position();
    uint8_t codec = 0;
    RETURN_IF_ERROR(source->get_u8(&codec));
    if (codec != static_cast<uint8_t>(PrxCodec::kRaw) &&
        codec != static_cast<uint8_t>(PrxCodec::kZstd) &&
        codec != static_cast<uint8_t>(PrxCodec::kPfor)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("prx: unknown codec");
    }
    frame->codec = static_cast<PrxCodec>(codec);
    RETURN_IF_ERROR(source->get_varint32(&frame->uncompressed_length));
    if (frame->uncompressed_length > kReaderPrxWindowLimits.max_uncomp_bytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: uncomp_len exceeds sane window cap");
    }
    size_t payload_length = frame->uncompressed_length;
    if (frame->codec == PrxCodec::kZstd) {
        uint32_t compressed_length = 0;
        RETURN_IF_ERROR(source->get_varint32(&compressed_length));
        payload_length = compressed_length;
    }
    RETURN_IF_ERROR(source->get_bytes(payload_length, &frame->payload));
    const size_t framed_length = source->position() - start;
    uint32_t stored_crc = 0;
    RETURN_IF_ERROR(source->get_fixed32(&stored_crc));
    if (crc32c(source->slice_from(start, framed_length)) != stored_crc) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: window crc mismatch");
    }
    return Status::OK();
}

} // namespace doris::snii::format
