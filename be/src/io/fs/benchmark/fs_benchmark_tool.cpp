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

#include <gflags/gflags.h>

#include <fstream>

#include "io/fs/benchmark/benchmark_factory.hpp"
#include "io/fs/s3_file_bufferpool.h"
#include "util/cpu_info.h"
#include "util/threadpool.h"

DEFINE_string(fs_type, "hdfs", "Supported File System: s3, hdfs");
DEFINE_string(operation, "create_write",
              "Supported Operations: create_write, open_read, open, rename, delete, exists");
DEFINE_string(threads, "1", "Number of threads");
DEFINE_string(iterations, "1", "Number of runs of each thread");
DEFINE_string(repetitions, "1", "Number of iterations");
DEFINE_string(file_size, "0", "File size for read/write opertions");
DEFINE_string(conf, "", "config file");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE benchmark tool for testing file system.\n";

    ss << "Usage:\n";
    ss << progname
       << " --fs_type=[fs_type] --operation=[op_type] --threads=[num] --iterations=[num] "
          "--repetitions=[num] "
          "--file_size=[num]\n";
    ss << "\nfs_type:\n";
    ss << "     hdfs\n";
    ss << "     s3\n";
    ss << "\nop_type:\n";
    ss << "     read\n";
    ss << "     write\n";
    ss << "\nthreads:\n";
    ss << "     num of threads\n";
    ss << "\niterations:\n";
    ss << "     Number of runs of each thread\n";
    ss << "\nrepetitions:\n";
    ss << "     Number of iterations\n";
    ss << "\nfile_size:\n";
    ss << "     File size for read/write opertions\n";
    ss << "\nExample:\n";
    ss << progname
       << " --conf my.conf --fs_type=hdfs --operation=create_write --threads=2 --iterations=100 "
          "--file_size=1048576\n";
    return ss.str();
}

int read_conf(const std::string& conf, std::map<std::string, std::string>* conf_map) {
    bool ok = true;
    std::ifstream fin(conf);
    if (fin.is_open()) {
        std::string line;
        while (getline(fin, line)) {
            if (line.empty() || line.rfind("#", 0) == 0) {
                // skip empty line and line starts with #
                continue;
            }
            size_t pos = line.find('=');
            if (pos != std::string::npos) {
                std::string key = line.substr(0, pos);
                std::string val = line.substr(pos + 1);
                (*conf_map)[key] = val;
            } else {
                std::cout << "invalid config item: " << line << std::endl;
                ok = false;
                break;
            }
        }
        fin.close();

        std::cout << "read config from file \"" << conf << "\":\n";
        for (auto it = conf_map->begin(); it != conf_map->end(); it++) {
            std::cout << it->first << " = " << it->second << std::endl;
        }
    } else {
        std::cout << "failed to open conf file: " << conf << std::endl;
        return 1;
    }
    return ok ? 0 : 1;
}

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    std::string conf_file = FLAGS_conf;
    std::map<std::string, std::string> conf_map;
    int res = read_conf(conf_file, &conf_map);
    if (res != 0) {
        std::cout << "failed to read conf from file \"conf_file\"" << std::endl;
        return 1;
    }

    doris::CpuInfo::init();
    int num_cores = doris::CpuInfo::num_cores();

    // init s3 write buffer pool
    std::unique_ptr<doris::ThreadPool> s3_file_upload_thread_pool;
    static_cast<void>(doris::ThreadPoolBuilder("S3FileUploadThreadPool")
                              .set_min_threads(num_cores)
                              .set_max_threads(num_cores)
                              .build(&s3_file_upload_thread_pool));
    doris::io::S3FileBufferPool* s3_buffer_pool = doris::io::S3FileBufferPool::GetInstance();
    s3_buffer_pool->init(524288000, 5242880, s3_file_upload_thread_pool.get());

    try {
        doris::io::MultiBenchmark multi_bm(FLAGS_fs_type, FLAGS_operation, std::stoi(FLAGS_threads),
                                           std::stoi(FLAGS_iterations), std::stol(FLAGS_file_size),
                                           conf_map);
        doris::Status st = multi_bm.init_env();
        if (!st) {
            std::cout << "init env failed: " << st << std::endl;
            return 1;
        }
        st = multi_bm.init_bms();
        if (!st) {
            std::cout << "init bms failed: " << st << std::endl;
            return 1;
        }

        benchmark::Initialize(&argc, argv);
        benchmark::RunSpecifiedBenchmarks();
        benchmark::Shutdown();

    } catch (std::invalid_argument const& ex) {
        std::cout << "std::invalid_argument::what(): " << ex.what() << std::endl;
        return 1;
    } catch (std::out_of_range const& ex) {
        std::cout << "std::out_of_range::what(): " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}
