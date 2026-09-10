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

#pragma once

#include <cstdint>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii {

class ByteSource;

namespace format {

struct PrxFrameView {
    PrxCodec codec = PrxCodec::kRaw;
    uint32_t uncompressed_length = 0;
    Slice payload;
};

// verify_crc=false 只解析帧头并切出 payload、跳过 crc 比对：给"估算位置工作量"这类
// 只读元数据的路径用，避免在真正解码之前对整个 prx 窗口多算一遍 crc32c；坏帧仍会在
// 解码时被 crc 拦下。
Status read_prx_frame(ByteSource* source, PrxFrameView* frame, bool verify_crc = true);

} // namespace format
} // namespace doris::snii
