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

#include "byte_stream_split.h"

#include <glog/logging.h>

#include <array>
#include <cstring>
#include <vector>

#include "gutil/port.h"

namespace doris {

inline void do_merge_streams(const uint8_t** src_streams, int width, int64_t nvalues,
                             uint8_t* dest) {
    // Value empirically chosen to provide the best performance on the author's machine
    constexpr int kBlockSize = 128;

    while (nvalues >= kBlockSize) {
        for (int stream = 0; stream < width; ++stream) {
            // Take kBlockSize bytes from the given stream and spread them
            // to their logical places in destination.
            const uint8_t* src = src_streams[stream];
            for (int i = 0; i < kBlockSize; i += 8) {
                uint64_t v;
                std::memcpy(&v, src + i, sizeof(v));
#ifdef IS_LITTLE_ENDIAN
                dest[stream + i * width] = static_cast<uint8_t>(v);
                dest[stream + (i + 1) * width] = static_cast<uint8_t>(v >> 8);
                dest[stream + (i + 2) * width] = static_cast<uint8_t>(v >> 16);
                dest[stream + (i + 3) * width] = static_cast<uint8_t>(v >> 24);
                dest[stream + (i + 4) * width] = static_cast<uint8_t>(v >> 32);
                dest[stream + (i + 5) * width] = static_cast<uint8_t>(v >> 40);
                dest[stream + (i + 6) * width] = static_cast<uint8_t>(v >> 48);
                dest[stream + (i + 7) * width] = static_cast<uint8_t>(v >> 56);
#elif defined IS_BIG_ENDIAN
                dest[stream + i * width] = static_cast<uint8_t>(v >> 56);
                dest[stream + (i + 1) * width] = static_cast<uint8_t>(v >> 48);
                dest[stream + (i + 2) * width] = static_cast<uint8_t>(v >> 40);
                dest[stream + (i + 3) * width] = static_cast<uint8_t>(v >> 32);
                dest[stream + (i + 4) * width] = static_cast<uint8_t>(v >> 24);
                dest[stream + (i + 5) * width] = static_cast<uint8_t>(v >> 16);
                dest[stream + (i + 6) * width] = static_cast<uint8_t>(v >> 8);
                dest[stream + (i + 7) * width] = static_cast<uint8_t>(v);
#endif
            }
            src_streams[stream] += kBlockSize;
        }
        dest += width * kBlockSize;
        nvalues -= kBlockSize;
    }

    // Epilog
    for (int stream = 0; stream < width; ++stream) {
        const uint8_t* src = src_streams[stream];
        for (int64_t i = 0; i < nvalues; ++i) {
            dest[stream + i * width] = src[i];
        }
    }
}

template <int kNumStreams>
void byte_stream_split_decode_scalar(const uint8_t* src, int width, int64_t offset,
                                     int64_t num_values, int64_t stride, uint8_t* dest) {
    DCHECK(width == kNumStreams);
    std::array<const uint8_t*, kNumStreams> src_streams;
    for (int stream = 0; stream < kNumStreams; ++stream) {
        src_streams[stream] = &src[stream * stride + offset];
    }
    do_merge_streams(src_streams.data(), kNumStreams, num_values, dest);
}

inline void byte_stream_split_decode_scalar_dynamic(const uint8_t* src, int width, int64_t offset,
                                                    int64_t num_values, int64_t stride,
                                                    uint8_t* dest) {
    std::vector<const uint8_t*> src_streams;
    src_streams.resize(width);
    for (int stream = 0; stream < width; ++stream) {
        src_streams[stream] = &src[stream * stride + offset];
    }
    do_merge_streams(src_streams.data(), width, num_values, dest);
}

// TODO: optimize using simd: https://github.com/apache/arrow/pull/38529
void byte_stream_split_decode(const uint8_t* src, int width, int64_t offset, int64_t num_values,
                              int64_t stride, uint8_t* dest) {
    switch (width) {
    case 1:
        memcpy(dest, src + offset * width, num_values);
        return;
    case 2:
        return byte_stream_split_decode_scalar<2>(src, width, offset, num_values, stride, dest);
    case 4:
        return byte_stream_split_decode_scalar<4>(src, width, offset, num_values, stride, dest);
    case 8:
        return byte_stream_split_decode_scalar<8>(src, width, offset, num_values, stride, dest);
    case 16:
        return byte_stream_split_decode_scalar<16>(src, width, offset, num_values, stride, dest);
    }
    return byte_stream_split_decode_scalar_dynamic(src, width, offset, num_values, stride, dest);
}

} // namespace doris
