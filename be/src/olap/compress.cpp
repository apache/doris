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

#include "compress.h"

#include "olap/byte_buffer.h"
#include "olap/utils.h"

namespace doris {

#ifdef DORIS_WITH_LZO
Status lzo_compress(StorageByteBuffer* in, StorageByteBuffer* out, bool* smaller) {
    size_t out_length = 0;
    Status res = Status::OK();
    *smaller = false;
    res = olap_compress(&(in->array()[in->position()]), in->remaining(),
                        &(out->array()[out->position()]), out->remaining(), &out_length,
                        OLAP_COMP_STORAGE);

    if (res.ok()) {
        if (out_length < in->remaining()) {
            *smaller = true;
            out->set_position(out->position() + out_length);
        }
    }

    return res;
}

Status lzo_decompress(StorageByteBuffer* in, StorageByteBuffer* out) {
    size_t out_length = 0;
    Status res = Status::OK();
    res = olap_decompress(&(in->array()[in->position()]), in->remaining(),
                          &(out->array()[out->position()]), out->remaining(), &out_length,
                          OLAP_COMP_STORAGE);

    if (res.ok()) {
        out->set_limit(out_length);
    }

    return res;
}
#endif

Status lz4_compress(StorageByteBuffer* in, StorageByteBuffer* out, bool* smaller) {
    size_t out_length = 0;
    Status res = Status::OK();
    *smaller = false;
    res = olap_compress(&(in->array()[in->position()]), in->remaining(),
                        &(out->array()[out->position()]), out->remaining(), &out_length,
                        OLAP_COMP_LZ4);

    if (res.ok()) {
        if (out_length < in->remaining()) {
            *smaller = true;
            out->set_position(out->position() + out_length);
        }
    }

    return res;
}

Status lz4_decompress(StorageByteBuffer* in, StorageByteBuffer* out) {
    size_t out_length = 0;
    Status res = Status::OK();
    res = olap_decompress(&(in->array()[in->position()]), in->remaining(),
                          &(out->array()[out->position()]), out->remaining(), &out_length,
                          OLAP_COMP_LZ4);

    if (res.ok()) {
        out->set_limit(out_length);
    }

    return res;
}

} // namespace doris
