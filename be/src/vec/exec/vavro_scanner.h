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

#include <sstream>

#include "avro/Compiler.hh"
#include "avro/Decoder.hh"
#include "avro/Exception.hh"
#include "avro/Node.hh"
#include "avro/NodeConcepts.hh"
#include "avro/NodeImpl.hh"
#include "avro/Schema.hh"
#include "avro/Specific.hh"
#include "avro/ValidSchema.hh"
#include "common/status.h"
#include "exec/avro_scanner.h"
#include "exec/base_scanner.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace doris {
class ExprContext;

namespace vectorized {
class VAvroReader;

class VAvroScanner : public AvroScanner {
public:
    VAvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                 const std::vector<TBrokerRangeDesc>& ranges,
                 const std::vector<TNetworkAddress>& broker_addresses,
                 const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    Status get_next(doris::Tuple* tuple, MemPool* tuple_pool, bool* eof,
                    bool* fill_tuple) override {
        return Status::NotSupported("Not Implemented get tuple");
    }
    Status get_next(vectorized::Block* output_block, bool* eof) override;

private:
    Status open_vavro_reader();
    Status open_next_reader();

private:
    std::unique_ptr<VAvroReader> _cur_vavro_reader;
};


class VAvroReader : public AvroReader {
public:
    VAvroReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                FileReader* file_reader = nullptr, LineReader* line_reader = nullptr);

    ~VAvroReader();

    // Status init(std::string avro_schema_name);

    Status read_avro_column(std::vector<MutableColumnPtr>& columns,
                            const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                            bool* eof);
                        
private:
    Status deserialize_column(std::vector<MutableColumnPtr>& columns, 
                              const std::vector<SlotDescriptor*>& slot_descs,
                              bool* is_empty_row, bool* eof);

    using DeserializeColumnFn = 
            std::function<void(vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                               avro::Decoder& decoder, int nullcount)>;
    using SkipColumnFn = std::function<void(avro::Decoder& decoder)>;
    DeserializeColumnFn createDeserializeColumnFn(avro::NodePtr root_node, SlotDescriptor* slot_desc);
    SkipColumnFn createSkipColumnFn(avro::NodePtr root_node);

    Status _get_field_mapping_column(const std::vector<SlotDescriptor*>& slot_descs);

private:
    std::vector<SkipColumnFn> _skip_fns_column;
    std::vector<DeserializeColumnFn> _deserialize_fns_column;
};   






} // namespace vectorized
} // namespace doris