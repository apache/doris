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
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/comparison_predicate.h"
#include "olap/data_dir.h"
#include "olap/in_list_predicate.h"
#include "olap/olap_common.h"
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
#include "testutil/test_util.h"
#include "util/debug_util.h"

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

namespace doris {
class BaseBenchmark {
public:
    BaseBenchmark(const std::string& name, int iterations) : _name(name), _iterations(iterations) {}
    virtual ~BaseBenchmark() = default;

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
            (void)page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &add_num);
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

        (void)page_builder.get_dictionary_page(&dict_slice);
    }

    void decode_pages() {
        // TODO should rewrite this method by using vectorized next batch method
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

    doris::StoragePageCache::create_global_cache(1 << 30, 10, 0);

    doris::MultiBenchmark multi_bm;
    multi_bm.add_bm();
    multi_bm.register_bm();

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    return 0;
}
