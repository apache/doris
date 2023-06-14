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

DEFINE_string(fs_type, "hdfs", "Supported File System: s3, hdfs, local");
DEFINE_string(operation, "read", "Supported Operations: read, write, open, size, list, connect");
DEFINE_string(iterations, "10", "Number of runs");
DEFINE_string(conf, "", "config file");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE benchmark tool for testing file system.\n";

    ss << "Usage:\n";
    ss << progname << " --fs_type=[fs_type] --operation=[op_type] --iterations=10\n";
    ss << "\nfs_type:\n";
    ss << "     hdfs\n";
    ss << "     s3\n";
    ss << "\nop_type:\n";
    ss << "     read\n";
    ss << "     write\n";
    ss << "\niterations:\n";
    ss << "     num of run\n";
    ss << "\nExample:\n";
    ss << progname << " --conf my.conf --fs_type=s3 --operation=read --iterations=100\n";
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

    try {
        doris::io::MultiBenchmark multi_bm(FLAGS_fs_type, FLAGS_operation,
                                           std::stoi(FLAGS_iterations), conf_map);
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
