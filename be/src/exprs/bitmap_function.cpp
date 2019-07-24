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

#include "exprs/bitmap_function.h"

#include "exprs/anyval_util.h"
#include <util/bitmap.h>

namespace doris {
void BitmapFunctions::init() {
}

StringVal BitmapFunctions::bitmap_init(doris_udf::FunctionContext* ctx, const doris_udf::StringVal& src) {
    std::unique_ptr<RoaringBitmap> bitmap {new RoaringBitmap()};
    if (!src.is_null) {
        std::string tmp_str = std::string(reinterpret_cast<char*>(src.ptr), src.len) ;
        bitmap->update(std::stoi(tmp_str));
    }
    size_t size = bitmap->size();
    char buf[size];
    bitmap->serialize(buf);
    return AnyValUtil::from_buffer_temp(ctx, buf, sizeof(buf));;
}

}
