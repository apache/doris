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

#include "common/status.h"
#include "util/byte_buffer.h"

namespace doris {

class MessageBodySink {
public:
    virtual ~MessageBodySink() {}
    virtual Status append(const char* data, size_t size) = 0;
    virtual Status append(const ByteBufferPtr& buf) { return append(buf->ptr, buf->remaining()); }
    // called when all data has been append
    virtual Status finish() { return Status::OK(); }
    // called when read HTTP failed
    virtual void cancel() {}
};

// write message to a local file
class MessageBodyFileSink : public MessageBodySink {
public:
    MessageBodyFileSink(const std::string& path) : _path(path) {}
    virtual ~MessageBodyFileSink();

    Status open();

    Status append(const char* data, size_t size) override;
    Status finish() override;
    void cancel() override;

private:
    std::string _path;
    int _fd = -1;
};

} // namespace doris
