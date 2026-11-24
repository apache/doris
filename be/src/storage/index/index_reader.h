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

#include <gen_cpp/olap_file.pb.h>

#include <memory>

#include "common/exception.h"
#include "common/status.h"
#include "storage/metadata_adder.h"

namespace doris {
class RuntimeState;
}

namespace doris::segment_v2 {

class IndexIterator;

class InvertedIndexReader;
using InvertedIndexReaderPtr = std::shared_ptr<InvertedIndexReader>;

class IndexReader : public std::enable_shared_from_this<IndexReader>,
                    public MetadataAdder<IndexReader> {
public:
    IndexReader() = default;
    ~IndexReader() override = default;

    virtual Status healthy_status() const { return _healthy_status.status(); }

    virtual IndexType index_type() = 0;
    virtual uint64_t get_index_id() const = 0;

    virtual Status new_iterator(std::unique_ptr<IndexIterator>* iterator) = 0;

protected:
    // Only wrap initialization of cached state. Query-local failures must not
    // poison this reader. Preserve exceptions after recording them.
    template <typename Fn>
    Status _load_with_health_check(Fn&& load) {
        try {
            auto status = load();
            _healthy_status.update(status);
            return status;
        } catch (const doris::Exception& e) {
            _healthy_status.update(e.to_status());
            throw;
        } catch (const std::exception& e) {
            _healthy_status.update(Status::InternalError(
                    "Exception while initializing cached index reader: {}", e.what()));
            throw;
        } catch (...) {
            _healthy_status.update(Status::InternalError(
                    "Unknown exception while initializing cached index reader"));
            throw;
        }
    }

private:
    AtomicStatus _healthy_status;
};
using IndexReaderPtr = std::shared_ptr<IndexReader>;

} // namespace doris::segment_v2
