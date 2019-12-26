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

#ifndef ORC_SCANNER_H
#define ORC_SCANNER_H

#include <memory>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <orc/OrcFile.hh>

#include "exec/base_scanner.h"
#include "exec/file_reader.h"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/tuple.h"
#include "util/slice.h"
#include "util/runtime_profile.h"

namespace doris {

class FileReader;
class Tuple;
class SlotDescriptor;
class Slice;
class ParquetReaderWrap;
class RuntimeState;
class ExprContext;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class MemTracker;
class RuntimeProfile;
class StreamLoadPipe;

class ORCFileStream : public orc::InputStream {
public:
    ORCFileStream(FileReader *file, std::string filename) : _file(file), _filename(filename) {
    }

    ~ORCFileStream();

    /**
     * Get the total length of the file in bytes.
     */
    uint64_t getLength() const;

    /**
     * Get the natural size for reads.
     * @return the number of bytes that should be read at once
     */
    uint64_t getNaturalReadSize() const;

    /**
     * Read length bytes from the file starting at offset into
     * the buffer starting at buf.
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to read.
     * @param offset the position in the stream to read from.
     */
    void read(void *buf, uint64_t length, uint64_t offset);

    /**
     * Get the name of the stream for error messages.
     */
    const std::string& getName() const;

private:
    FileReader *_file;
    std::string _filename;
};

// Broker scanner convert the data read from broker to doris's tuple.
class ORCScanner : public BaseScanner {
public:
    ORCScanner(RuntimeState *state,
               RuntimeProfile *profile,
               const TBrokerScanRangeParams& params,
               const std::vector<TBrokerRangeDesc>& ranges,
               const std::vector<TNetworkAddress>& broker_addresses, ScannerCounter *counter);

    ~ORCScanner();

    // Open this scanner, will initialize information need to
    virtual Status open();

    // Get next tuple
    virtual Status get_next(Tuple *tuple, MemPool *tuple_pool, bool *eof);

    // Close this scanner
    virtual void close();

private:
    // Read next buffer from reader
    Status open_next_reader();

private:
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;

    // Reader
    int _next_range;
    bool _cur_file_eof;
    bool _scanner_eof;

    // orc file reader object
    orc::ReaderOptions _options;
    orc::RowReaderOptions _rowReaderOptions;
    std::shared_ptr<orc::ColumnVectorBatch> _batch;
    std::unique_ptr<orc::Reader> _reader;
    std::unique_ptr<orc::RowReader> _row_reader;
    std::list<std::string> _includes; // include columns in orc file
    // The batch after reading from orc data is arranged in the original order,
    // so we need to record the index in the original order to correspond the column names to the order
    std::map<std::string, int> _column_name_map_orc_index;
    int _num_of_columns_from_file;

    int _total_groups; // groups in a orc file
    int _current_group;
    int64_t _rows_of_group; // rows in a group.
    int64_t _current_line_of_group;
};

}
#endif //ORC_SCANNER_H
