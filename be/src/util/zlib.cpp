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

#include "util/zlib.h"

#include <zconf.h>
#include <zlib.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "gutil/macros.h"
#include "gutil/strings/substitute.h"

using std::ostream;
using std::string;

#define ZRETURN_NOT_OK(call) RETURN_IF_ERROR(ZlibResultToStatus(call))

namespace doris {
namespace zlib {

namespace {
Status ZlibResultToStatus(int rc) {
    switch (rc) {
    case Z_OK:
        return Status::OK();
    case Z_STREAM_END:
        return Status::EndOfFile("zlib EOF");
    case Z_NEED_DICT:
        return Status::Corruption("zlib error: NEED_DICT");
    case Z_ERRNO:
        return Status::IOError("zlib error: Z_ERRNO");
    case Z_STREAM_ERROR:
        return Status::Corruption("zlib error: STREAM_ERROR");
    case Z_DATA_ERROR:
        return Status::Corruption("zlib error: DATA_ERROR");
    case Z_MEM_ERROR:
        return Status::RuntimeError("zlib error: MEM_ERROR");
    case Z_BUF_ERROR:
        return Status::RuntimeError("zlib error: BUF_ERROR");
    case Z_VERSION_ERROR:
        return Status::RuntimeError("zlib error: VERSION_ERROR");
    default:
        return Status::RuntimeError(strings::Substitute("zlib error: unknown error $0", rc));
    }
}
} // anonymous namespace

Status Compress(Slice input, ostream* out) {
    return CompressLevel(input, Z_DEFAULT_COMPRESSION, out);
}

Status CompressLevel(Slice input, int level, ostream* out) {
    z_stream zs;
    memset(&zs, 0, sizeof(zs));
    ZRETURN_NOT_OK(deflateInit2(&zs, level, Z_DEFLATED, 15 + 16 /* 15 window bits, enable gzip */,
                                8 /* memory level, max is 9 */, Z_DEFAULT_STRATEGY));

    zs.avail_in = input.get_size();
    zs.next_in = (unsigned char*)(input.mutable_data());
    const int kChunkSize = 256 * 1024;
    std::unique_ptr<unsigned char[]> chunk(new unsigned char[kChunkSize]);
    int flush;
    do {
        zs.avail_out = kChunkSize;
        zs.next_out = chunk.get();
        flush = (zs.avail_in == 0) ? Z_FINISH : Z_NO_FLUSH;
        Status s = ZlibResultToStatus(deflate(&zs, flush));
        if (!s.ok() && !s.is_end_of_file()) {
            return s;
        }
        int out_size = zs.next_out - chunk.get();
        if (out_size > 0) {
            out->write(reinterpret_cast<char*>(chunk.get()), out_size);
        }
    } while (flush != Z_FINISH);
    ZRETURN_NOT_OK(deflateEnd(&zs));
    return Status::OK();
}

Status Uncompress(Slice compressed, std::ostream* out) {
    z_stream zs;
    memset(&zs, 0, sizeof(zs));
    zs.next_in = (unsigned char*)(compressed.mutable_data());
    zs.avail_in = compressed.get_size();
    ZRETURN_NOT_OK(inflateInit2(&zs, 15 + 16 /* 15 window bits, enable zlib */));
    int flush;
    Status s;
    do {
        unsigned char buf[4096];
        zs.next_out = buf;
        zs.avail_out = arraysize(buf);
        flush = zs.avail_in > 0 ? Z_NO_FLUSH : Z_FINISH;
        s = ZlibResultToStatus(inflate(&zs, flush));
        if (!s.ok() && !s.is_end_of_file()) {
            return s;
        }
        out->write(reinterpret_cast<char*>(buf), zs.next_out - buf);
    } while (flush == Z_NO_FLUSH);
    ZRETURN_NOT_OK(inflateEnd(&zs));

    return Status::OK();
}

} // namespace zlib
} // namespace doris
