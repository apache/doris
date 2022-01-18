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

#ifndef DORIS_BE_SRC_QUERY_EXEC_TEXT_CONVERTER_H
#define DORIS_BE_SRC_QUERY_EXEC_TEXT_CONVERTER_H

#include "runtime/runtime_state.h"
#include "vec/core/block.h"
namespace doris {

class MemPool;
class SlotDescriptor;
class Status;
struct StringValue;
class Tuple;
class TupleDescriptor;

// Helper class for dealing with text data, e.g., converting text data to
// numeric types, etc.
class TextConverter {
public:
    TextConverter(char escape_char);

    // Converts slot data, of length 'len',  into type of slot_desc,
    // and writes the result into the tuples's slot.
    // copy_string indicates whether we need to make a separate copy of the string data:
    // For regular unescaped strings, we point to the original data in the _file_buf.
    // For regular escaped strings, we copy an its unescaped string into a separate buffer
    // and point to it.
    // If the string needs to be copied, the memory is allocated from 'pool', otherwise
    // 'pool' is unused.
    // Unsuccessful conversions are turned into NULLs.
    // Returns true if the value was written successfully.
    bool write_slot(const SlotDescriptor* slot_desc, Tuple* tuple, const char* data, int len,
                    bool copy_string, bool need_escape, MemPool* pool);

    bool write_column(const SlotDescriptor* slot_desc, vectorized::MutableColumnPtr* column_ptr,
                      const char* data, size_t len, bool copy_string, bool need_escape);

    // Removes escape characters from len characters of the null-terminated string src,
    // and copies the unescaped string into dest, changing *len to the unescaped length.
    // No null-terminator is added to dest.
    void unescape_string(const char* src, char* dest, size_t* len);
    void unescape_string_on_spot(const char* src, size_t* len);
    // Removes escape characters from 'str', allocating a new string from pool.
    // 'str' is updated with the new ptr and length.
    void unescape_string(StringValue* str, MemPool* pool);

private:
    char _escape_char;
};

} // namespace doris

#endif
