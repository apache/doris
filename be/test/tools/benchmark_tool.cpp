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

#include <benchmark/benchmark.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "olap/comparison_predicate.h"
#include "olap/fs/block_manager.h"
#include "olap/fs/fs_util.h"
#include "olap/in_list_predicate.h"
#include "olap/olap_common.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/binary_dict_page.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "test_util/test_util.h"
#include "util/debug_util.h"
#include "util/file_utils.h"

DEFINE_string(operation, "Custom",
              "valid operation: Custom, BinaryDictPageEncode, BinaryDictPageDecode, SegmentScan, "
              "SegmentWrite, "
              "SegmentScanByFile, SegmentWriteByFile");
DEFINE_string(input_file, "./sample.dat", "input file directory");
DEFINE_string(column_type, "int,varchar", "valid type: int, char, varchar, string");
DEFINE_string(rows_number, "10000", "rows number");
DEFINE_string(iterations, "10",
              "run times, this is set to 0 means the number of iterations is automatically set ");

const std::string kSegmentDir = "./segment_benchmark";

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE benchmark tool.\n";
    ss << "Stop BE first before use this tool.\n";

    ss << "Usage:\n";
    ss << "./benchmark_tool --operation=Custom\n";
    ss << "./benchmark_tool --operation=BinaryDictPageEncode "
          "--rows_number=10000 --iterations=40\n";
    ss << "./benchmark_tool --operation=BinaryDictPageDecode "
          "--rows_number=10000 --iterations=40\n";
    ss << "./benchmark_tool --operation=SegmentScan --column_type=int,varchar "
          "--rows_number=10000 --iterations=0\n";
    ss << "./benchmark_tool --operation=SegmentWrite --column_type=int "
          "--rows_number=10000 --iterations=10\n";
    ss << "./benchmark_tool --operation=SegmentScanByFile --input_file=./sample.dat "
          "--iterations=10\n";
    ss << "./benchmark_tool --operation=SegmentWriteByFile --input_file=./sample.dat "
          "--iterations=10\n";

    ss << "Sampe data file format: \n"
       << "The first line defines Shcema\n"
       << "The rest of the content is DataSet\n"
       << "For example: \n"
       << "int,char,varchar\n"
       << "123,hello,world\n"
       << "321,good,bye\n";
    return ss.str();
}

static int seg_id = 0;

namespace doris {
class BaseBenchmark {
public:
    BaseBenchmark(const std::string& name, int iterations) : _name(name), _iterations(iterations) {}
    virtual ~BaseBenchmark() {}

    void add_name(const std::string& str) { _name += str; }

    virtual void init() {}
    virtual void run() {}

    void register_bm() {
        auto bm = benchmark::RegisterBenchmark(_name.c_str(), [&](benchmark::State& state) {
            //first turn will use more time
            this->init();
            this->run();
            for (auto _ : state) {
                state.PauseTiming();
                this->init();
                state.ResumeTiming();
                this->run();
            }
        });
        if (_iterations != 0) {
            bm->Iterations(_iterations);
        }
        bm->Unit(benchmark::kMillisecond);
    }

private:
    std::string _name;
    int _iterations;
};

class BinaryDictPageBenchmark : public BaseBenchmark {
public:
    BinaryDictPageBenchmark(const std::string& name, int iterations)
            : BaseBenchmark(name, iterations) {}
    virtual ~BinaryDictPageBenchmark() override {}

    virtual void init() override {}
    virtual void run() override {}

    void encode_pages(const std::vector<Slice>& contents) {
        PageBuilderOptions options;
        BinaryDictPageBuilder page_builder(options);

        results.clear();
        page_start_ids.clear();
        page_start_ids.push_back(0);
        for (size_t i = 0; i < contents.size(); i++) {
            const Slice* ptr = &contents[i];
            size_t add_num = 1;
            page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &add_num);
            if (page_builder.is_page_full()) {
                OwnedSlice s = page_builder.finish();
                results.emplace_back(std::move(s));
                page_start_ids.push_back(i + 1);
                page_builder.reset();
            }
        }
        OwnedSlice s = page_builder.finish();
        results.emplace_back(std::move(s));
        page_start_ids.push_back(contents.size());

        page_builder.get_dictionary_page(&dict_slice);
    }

    void decode_pages() {
        int slice_index = 0;
        for (auto& src : results) {
            PageDecoderOptions dict_decoder_options;
            std::unique_ptr<BinaryPlainPageDecoder> dict_page_decoder(
                    new BinaryPlainPageDecoder(dict_slice.slice(), dict_decoder_options));
            dict_page_decoder->init();

            StringRef dict_word_info[dict_page_decoder->_num_elems];
            dict_page_decoder->get_dict_word_info(dict_word_info);

            // decode
            PageDecoderOptions decoder_options;
            BinaryDictPageDecoder page_decoder(src.slice(), decoder_options);
            page_decoder.init();

            page_decoder.set_dict_decoder(dict_page_decoder.get(), dict_word_info);

            //check values
            size_t num = page_start_ids[slice_index + 1] - page_start_ids[slice_index];

            auto tracker = std::make_shared<MemTracker>();
            MemPool pool(tracker.get());
            const auto* type_info = get_scalar_type_info<OLAP_FIELD_TYPE_VARCHAR>();
            std::unique_ptr<ColumnVectorBatch> cvb;
            ColumnVectorBatch::create(num, false, type_info, nullptr, &cvb);
            ColumnBlock column_block(cvb.get(), &pool);
            ColumnBlockView block_view(&column_block);

            page_decoder.next_batch(&num, &block_view);

            slice_index++;
        }
    }

private:
    std::vector<OwnedSlice> results;
    OwnedSlice dict_slice;
    std::vector<size_t> page_start_ids;
};

class BinaryDictPageEncodeBenchmark : public BinaryDictPageBenchmark {
public:
    BinaryDictPageEncodeBenchmark(const std::string& name, int iterations, int rows_number)
            : BinaryDictPageBenchmark(name + "/rows_number:" + std::to_string(rows_number),
                                      iterations),
              _rows_number(rows_number) {}
    virtual ~BinaryDictPageEncodeBenchmark() override {}

    virtual void init() override {
        src_strings.clear();
        for (int i = 0; i < _rows_number; i++) {
            src_strings.emplace_back(rand_rng_string(rand_rng_int(1, 8)));
        }

        slices.clear();
        for (auto s : src_strings) {
            slices.emplace_back(s.c_str());
        }
    }
    virtual void run() override { encode_pages(slices); }

private:
    std::vector<Slice> slices;
    std::vector<std::string> src_strings;
    int _rows_number;
};

class BinaryDictPageDecodeBenchmark : public BinaryDictPageBenchmark {
public:
    BinaryDictPageDecodeBenchmark(const std::string& name, int iterations, int rows_number)
            : BinaryDictPageBenchmark(name + "/rows_number:" + std::to_string(rows_number),
                                      iterations),
              _rows_number(rows_number) {}
    virtual ~BinaryDictPageDecodeBenchmark() override {}

    virtual void init() override {
        src_strings.clear();
        for (int i = 0; i < _rows_number; i++) {
            src_strings.emplace_back(rand_rng_string(rand_rng_int(1, 8)));
        }

        slices.clear();
        for (auto s : src_strings) {
            slices.emplace_back(s.c_str());
        }

        encode_pages(slices);
    }
    virtual void run() override { decode_pages(); }

private:
    std::vector<Slice> slices;
    std::vector<std::string> src_strings;
    int _rows_number;
}; // namespace doris

class SegmentBenchmark : public BaseBenchmark {
public:
    SegmentBenchmark(const std::string& name, int iterations, const std::string& column_type)
            : BaseBenchmark(name, iterations),
              _tracker(std::make_shared<MemTracker>()),
              _pool(_tracker.get()) {
        if (FileUtils::check_exist(kSegmentDir)) {
            FileUtils::remove_all(kSegmentDir);
        }
        FileUtils::create_dir(kSegmentDir);

        init_schema(column_type);
    }
    SegmentBenchmark(const std::string& name, int iterations)
            : BaseBenchmark(name, iterations),
              _tracker(std::make_shared<MemTracker>()),
              _pool(_tracker.get()) {
        if (FileUtils::check_exist(kSegmentDir)) {
            FileUtils::remove_all(kSegmentDir);
        }
        FileUtils::create_dir(kSegmentDir);
    }
    virtual ~SegmentBenchmark() override {
        if (FileUtils::check_exist(kSegmentDir)) {
            FileUtils::remove_all(kSegmentDir);
        }
    }

    const Schema& get_schema() { return *_schema.get(); }

    virtual void init() override {}
    virtual void run() override {}

    void init_schema(const std::string& column_type) {
        std::string column_valid = "/column_type:";

        std::vector<std::string> tokens = strings::Split(column_type, ",");
        std::vector<TabletColumn> columns;

        bool first_column = true;
        for (auto token : tokens) {
            bool valid = true;

            if (equal_ignore_case(token, "int")) {
                columns.emplace_back(create_int_key(columns.size() + 1));
            } else if (equal_ignore_case(token, "char")) {
                columns.emplace_back(create_char_key(columns.size() + 1));
            } else if (equal_ignore_case(token, "varchar")) {
                columns.emplace_back(create_varchar_key(columns.size() + 1));
            } else if (equal_ignore_case(token, "string")) {
                columns.emplace_back(create_string_key(columns.size() + 1));
            } else {
                valid = false;
            }

            if (valid) {
                if (first_column) {
                    first_column = false;
                } else {
                    column_valid += ',';
                }
                column_valid += token;
            }
        }

        _tablet_schema = _create_schema(columns);
        _schema = std::make_shared<Schema>(_tablet_schema);

        add_name(column_valid);
    }

    void build_segment(std::vector<std::vector<std::string>> dataset,
                       std::shared_ptr<Segment>* res) {
        // must use unique filename for each segment, otherwise page cache kicks in and produces
        // the wrong answer (it use (filename,offset) as cache key)
        std::string filename = strings::Substitute("$0/seg_$1.dat", kSegmentDir, ++seg_id);
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions block_opts({filename});
        fs::fs_util::block_manager(TStorageMedium::HDD)->create_block(block_opts, &wblock);
        SegmentWriterOptions opts;
        SegmentWriter writer(wblock.get(), 0, &_tablet_schema, opts);
        writer.init(1024);

        RowCursor row;
        row.init(_tablet_schema);

        for (auto tokens : dataset) {
            for (int cid = 0; cid < _tablet_schema.num_columns(); ++cid) {
                RowCursorCell cell = row.cell(cid);
                set_column_value_by_type(_tablet_schema._cols[cid]._type, tokens[cid],
                                         (char*)cell.mutable_cell_ptr(), &_pool,
                                         _tablet_schema._cols[cid]._length);
            }
            writer.append_row(row);
        }

        uint64_t file_size, index_size;
        writer.finalize(&file_size, &index_size);
        wblock->close();

        Segment::open(filename, seg_id, &_tablet_schema, res);
    }

    std::vector<std::vector<std::string>> generate_dataset(int rows_number) {
        std::vector<std::vector<std::string>> dataset;
        while (rows_number--) {
            std::vector<std::string> row_data;
            for (int cid = 0; cid < _tablet_schema.num_columns(); ++cid) {
                row_data.emplace_back(rand_rng_by_type(_tablet_schema._cols[cid]._type));
            }
            dataset.emplace_back(row_data);
        }
        return dataset;
    }

private:
    TabletSchema _create_schema(const std::vector<TabletColumn>& columns,
                                int num_short_key_columns = -1) {
        TabletSchema res;
        int num_key_columns = 0;
        for (auto& col : columns) {
            if (col.is_key()) {
                num_key_columns++;
            }
            res._cols.push_back(col);
        }
        res._num_columns = columns.size();
        res._num_key_columns = num_key_columns;
        res._num_short_key_columns =
                num_short_key_columns != -1 ? num_short_key_columns : num_key_columns;
        res.init_field_index_for_test();
        return res;
    }

    std::shared_ptr<MemTracker> _tracker;
    MemPool _pool;
    TabletSchema _tablet_schema;
    std::shared_ptr<Schema> _schema;
}; // namespace doris

class SegmentWriteBenchmark : public SegmentBenchmark {
public:
    SegmentWriteBenchmark(const std::string& name, int iterations, const std::string& column_type,
                          int rows_number)
            : SegmentBenchmark(name + "/rows_number:" + std::to_string(rows_number), iterations,
                               column_type),
              _dataset(generate_dataset(rows_number)) {}
    virtual ~SegmentWriteBenchmark() override {}

    virtual void init() override {}
    virtual void run() override { build_segment(_dataset, &_segment); }

private:
    std::vector<std::vector<std::string>> _dataset;
    std::shared_ptr<Segment> _segment;
};

class SegmentWriteByFileBenchmark : public SegmentBenchmark {
public:
    SegmentWriteByFileBenchmark(const std::string& name, int iterations,
                                const std::string& file_str)
            : SegmentBenchmark(name + "/file_path:" + file_str, iterations) {
        std::ifstream file(file_str);
        assert(file.is_open());

        std::string column_type;
        std::getline(file, column_type);
        init_schema(column_type);
        while (file.peek() != EOF) {
            std::string row_str;
            std::getline(file, row_str);
            std::vector<std::string> tokens = strings::Split(row_str, ",");
            assert(tokens.size() == _tablet_schema.num_columns());
            _dataset.push_back(tokens);
        }

        add_name("/rows_number:" + std::to_string(_dataset.size()));
    }
    virtual ~SegmentWriteByFileBenchmark() override {}

    virtual void init() override {}
    virtual void run() override { build_segment(_dataset, &_segment); }

private:
    std::vector<std::vector<std::string>> _dataset;
    std::shared_ptr<Segment> _segment;
};

class SegmentScanBenchmark : public SegmentBenchmark {
public:
    SegmentScanBenchmark(const std::string& name, int iterations, const std::string& column_type,
                         int rows_number)
            : SegmentBenchmark(name + "/rows_number:" + std::to_string(rows_number), iterations,
                               column_type),
              _dataset(generate_dataset(rows_number)) {}
    virtual ~SegmentScanBenchmark() override {}

    virtual void init() override { build_segment(_dataset, &_segment); }
    virtual void run() override {
        StorageReadOptions read_opts;
        read_opts.stats = &stats;
        std::unique_ptr<RowwiseIterator> iter;
        _segment->new_iterator(get_schema(), read_opts, nullptr, &iter);
        RowBlockV2 block(get_schema(), 1024);

        int left = _dataset.size();
        while (left > 0) {
            int rows_read = std::min(left, 1024);
            block.clear();
            iter->next_batch(&block);
            left -= rows_read;
        }
    }

private:
    std::vector<std::vector<std::string>> _dataset;
    std::shared_ptr<Segment> _segment;
    OlapReaderStatistics stats;
};

class SegmentScanByFileBenchmark : public SegmentBenchmark {
public:
    SegmentScanByFileBenchmark(const std::string& name, int iterations, const std::string& file_str)
            : SegmentBenchmark(name, iterations) {
        std::ifstream file(file_str);
        assert(file.is_open());

        std::string column_type;
        std::getline(file, column_type);
        init_schema(column_type);
        while (file.peek() != EOF) {
            std::string row_str;
            std::getline(file, row_str);
            std::vector<std::string> tokens = strings::Split(row_str, ",");
            assert(tokens.size() == _tablet_schema.num_columns());
            _dataset.push_back(tokens);
        }

        add_name("/rows_number:" + std::to_string(_dataset.size()));
    }
    virtual ~SegmentScanByFileBenchmark() override {}

    virtual void init() override { build_segment(_dataset, &_segment); }
    virtual void run() override {
        StorageReadOptions read_opts;
        read_opts.stats = &stats;
        std::unique_ptr<RowwiseIterator> iter;
        _segment->new_iterator(get_schema(), read_opts, nullptr, &iter);
        RowBlockV2 block(get_schema(), 1024);

        int left = _dataset.size();
        while (left > 0) {
            int rows_read = std::min(left, 1024);
            block.clear();
            iter->next_batch(&block);
            left -= rows_read;
        }
    }

private:
    std::vector<std::vector<std::string>> _dataset;
    std::shared_ptr<Segment> _segment;
    OlapReaderStatistics stats;
};

// This is sample custom test. User can write custom test code at custom_init()&custom_run().
// Call method: ./benchmark_tool --operation=Custom
class CustomBenchmark : public BaseBenchmark {
public:
    CustomBenchmark(const std::string& name, int iterations, std::function<void()> init_func,
                    std::function<void()> run_func)
            : BaseBenchmark(name, iterations), _init_func(init_func), _run_func(run_func) {}
    virtual ~CustomBenchmark() override {}

    virtual void init() override { _init_func(); }
    virtual void run() override { _run_func(); }

private:
    std::function<void()> _init_func;
    std::function<void()> _run_func;
};
void custom_init() {}
void custom_run_plus() {
    int p = 100000;
    int q = 0;
    while (p--) {
        q++;
        if (UNLIKELY(q == 1024)) q = 0;
    }
}
void custom_run_mod() {
    int p = 100000;
    int q = 0;
    while (p--) {
        q++;
        if (q %= 1024) q = 0;
    }
}

class MultiBenchmark {
public:
    MultiBenchmark() {}
    ~MultiBenchmark() {
        for (auto bm : benchmarks) {
            delete bm;
        }
    }

    void add_bm() {
        if (equal_ignore_case(FLAGS_operation, "Custom")) {
            benchmarks.emplace_back(
                    new doris::CustomBenchmark("custom_run_plus", std::stoi(FLAGS_iterations),
                                               doris::custom_init, doris::custom_run_plus));
            benchmarks.emplace_back(
                    new doris::CustomBenchmark("custom_run_mod", std::stoi(FLAGS_iterations),
                                               doris::custom_init, doris::custom_run_mod));
        } else if (equal_ignore_case(FLAGS_operation, "BinaryDictPageEncode")) {
            benchmarks.emplace_back(new doris::BinaryDictPageEncodeBenchmark(
                    FLAGS_operation, std::stoi(FLAGS_iterations), std::stoi(FLAGS_rows_number)));
        } else if (equal_ignore_case(FLAGS_operation, "BinaryDictPageDecode")) {
            benchmarks.emplace_back(new doris::BinaryDictPageDecodeBenchmark(
                    FLAGS_operation, std::stoi(FLAGS_iterations), std::stoi(FLAGS_rows_number)));
        } else if (equal_ignore_case(FLAGS_operation, "SegmentScan")) {
            benchmarks.emplace_back(new doris::SegmentScanBenchmark(
                    FLAGS_operation, std::stoi(FLAGS_iterations), FLAGS_column_type,
                    std::stoi(FLAGS_rows_number)));
        } else if (equal_ignore_case(FLAGS_operation, "SegmentWrite")) {
            benchmarks.emplace_back(new doris::SegmentWriteBenchmark(
                    FLAGS_operation, std::stoi(FLAGS_iterations), FLAGS_column_type,
                    std::stoi(FLAGS_rows_number)));
        } else if (equal_ignore_case(FLAGS_operation, "SegmentScanByFile")) {
            benchmarks.emplace_back(new doris::SegmentScanByFileBenchmark(
                    FLAGS_operation, std::stoi(FLAGS_iterations), FLAGS_input_file));
        } else if (equal_ignore_case(FLAGS_operation, "SegmentWriteByFile")) {
            benchmarks.emplace_back(new doris::SegmentWriteByFileBenchmark(
                    FLAGS_operation, std::stoi(FLAGS_iterations), FLAGS_input_file));
        } else {
            std::cout << "operation invalid!" << std::endl;
        }
    }
    void register_bm() {
        for (auto bm : benchmarks) {
            bm->register_bm();
        }
    }

private:
    std::vector<doris::BaseBenchmark*> benchmarks;
};

} //namespace doris

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    doris::StoragePageCache::create_global_cache(1 << 30, 10);

    doris::MultiBenchmark multi_bm;
    multi_bm.add_bm();
    multi_bm.register_bm();

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    return 0;
}
