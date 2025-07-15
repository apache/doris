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

#include <gtest/gtest.h>

#include <memory>

#include "orc/ColumnPrinter.hh"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/orc/vorc_reader.h"

namespace doris {
namespace vectorized {

class MemoryOutputStream : public orc::OutputStream {
public:
    MemoryOutputStream(size_t capacity) : name("MemoryOutputStream") {
        data = new char[capacity];
        length = 0;
        naturalWriteSize = 2048;
    }

    virtual ~MemoryOutputStream() override { delete[] data; };

    virtual uint64_t getLength() const override { return length; }

    virtual uint64_t getNaturalWriteSize() const override { return naturalWriteSize; }

    virtual void write(const void* buf, size_t size) override {
        memcpy(data + length, buf, size);
        length += size;
    }

    virtual const std::string& getName() const override { return name; }

    const char* getData() const { return data; }

    void close() override {}

private:
    char* data;
    std::string name;
    uint64_t length, naturalWriteSize;
};

class MemoryInputStream : public orc::InputStream {
public:
    MemoryInputStream(const char* _buffer, size_t _size)
            : buffer(_buffer), size(_size), naturalReadSize(1024), name("MemoryInputStream") {}

    ~MemoryInputStream() override {}

    virtual uint64_t getLength() const override { return size; }

    virtual uint64_t getNaturalReadSize() const override { return naturalReadSize; }

    virtual void read(void* buf, uint64_t length, uint64_t offset) override {
        memcpy(buf, buffer + offset, length);
    }

    virtual const std::string& getName() const override { return name; }

    //    const char* getData() const {
    //        return buffer;
    //    }

private:
    const char* buffer;
    uint64_t size, naturalReadSize;
    std::string name;
};
} // namespace vectorized
} // namespace doris