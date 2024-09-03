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

#include "util/pprof_utils.h"

#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream> // IWYU pragma: keep
#include <memory>

#include "agent/utils.h"
#include "gutil/strings/substitute.h"
#include "io/fs/local_file_system.h"

namespace doris {
namespace config {
extern std::string pprof_profile_dir;
}

Status PprofUtils::get_pprof_cmd(std::string* cmd) {
    AgentUtils util;
    // check if pprof cmd exist
    const static std::string tools_path = std::string(std::getenv("DORIS_HOME")) + "/tools/bin/";
    std::string pprof_cmd = tools_path + "pprof";
    std::string msg;
    bool rc = util.exec_cmd(pprof_cmd + " --version", &msg);
    if (!rc) {
        // not found in BE tools dir, found in system
        pprof_cmd = "pprof";
        rc = util.exec_cmd(pprof_cmd + " --version", &msg);
        if (!rc) {
            return Status::NotSupported(
                    "pprof: command not found in systemp PATH or be/tools/bin/. Install gperftools "
                    "first.");
        }
    }
    *cmd = pprof_cmd;
    return Status::OK();
}

Status PprofUtils::get_perf_cmd(std::string* cmd) {
    AgentUtils util;
    // check if perf cmd exist
    std::string perf_cmd = "perf";
    std::string msg;
    bool rc = util.exec_cmd(perf_cmd + " --version", &msg);
    if (!rc) {
        return Status::NotSupported("perf: command not found in systemp PATH");
    }
    *cmd = perf_cmd;
    return Status::OK();
}

Status PprofUtils::get_self_cmdline(std::string* cmd) {
    // get cmdline
    FILE* fp = fopen("/proc/self/cmdline", "r");
    if (fp == nullptr) {
        return Status::InternalError("Unable to open file: /proc/self/cmdline");
    }
    char buf[1024];

    Status res = Status::OK();

    if (fscanf(fp, "%1023s ", buf) != 1) {
        res = Status::InternalError("get_self_cmdline read buffer failed");
    }
    fclose(fp);
    *cmd = buf;
    return res;
}

Status PprofUtils::get_readable_profile(const std::string& file_or_content, bool is_file,
                                        std::stringstream* output) {
    // get pprof cmd
    std::string pprof_cmd;
    RETURN_IF_ERROR(PprofUtils::get_pprof_cmd(&pprof_cmd));

    // get self cmdline
    std::string self_cmdline;
    RETURN_IF_ERROR(PprofUtils::get_self_cmdline(&self_cmdline));

    // save file if necessary
    std::string final_file;
    if (!is_file) {
        std::stringstream tmp_file;
        tmp_file << config::pprof_profile_dir << "/pprof_profile." << getpid() << "." << rand();
        std::ofstream outfile;
        outfile.open(tmp_file.str().c_str());
        outfile << file_or_content;
        outfile.close();
        final_file = tmp_file.str();
    } else {
        final_file = file_or_content;
    }

    // parse raw with "pprof --text cmdline raw_file"
    std::string cmd_output;
    std::string final_cmd =
            pprof_cmd + strings::Substitute(" --text $0 $1", self_cmdline, final_file);
    AgentUtils util;
    LOG(INFO) << "begin to run command: " << final_cmd;
    bool rc = util.exec_cmd(final_cmd, &cmd_output, false);

    // delete raw file
    static_cast<void>(io::global_local_filesystem()->delete_file(file_or_content));

    if (!rc) {
        return Status::InternalError("Failed to execute command: {}", cmd_output);
    }

    (*output) << "Profile(Sample 30 seconds)" << std::endl;
    (*output) << cmd_output << std::endl;
    return Status::OK();
}

Status PprofUtils::generate_flamegraph(int32_t sample_seconds,
                                       const std::string& flame_graph_tool_dir, bool return_file,
                                       std::string* svg_file_or_content) {
    // get perf cmd
    std::string perf_cmd;
    RETURN_IF_ERROR(PprofUtils::get_perf_cmd(&perf_cmd));

    // check if FlameGraph has been installed
    // check stackcollapse-perf.pl and flamegraph.pl exist
    std::string stackcollapse_perf_pl = flame_graph_tool_dir + "/stackcollapse-perf.pl";
    std::string flamegraph_pl = flame_graph_tool_dir + "/flamegraph.pl";
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(stackcollapse_perf_pl, &exists));
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(flamegraph_pl, &exists));
    if (!exists) {
        return Status::InternalError(
                "Missing stackcollapse-perf.pl or flamegraph.pl in FlameGraph");
    }

    // tmp output profile file
    std::stringstream tmp_file;
    tmp_file << config::pprof_profile_dir << "/cpu_perf." << getpid() << "." << rand();

    // sample
    std::stringstream cmd;
    cmd << perf_cmd << " record -m 2 -g -p " << getpid() << " -o " << tmp_file.str() << " -- sleep "
        << sample_seconds;

    AgentUtils util;
    std::string cmd_output;
    LOG(INFO) << "begin to run command: " << cmd.str();
    bool rc = util.exec_cmd(cmd.str(), &cmd_output);
    if (!rc) {
        static_cast<void>(io::global_local_filesystem()->delete_file(tmp_file.str()));
        return Status::InternalError("Failed to execute perf command: {}", cmd_output);
    }

    // generate flamegraph

    std::string res_content;
    if (return_file) {
        std::stringstream graph_file;
        graph_file << config::pprof_profile_dir << "/flamegraph." << getpid() << "." << rand()
                   << ".svg";
        std::stringstream gen_cmd;
        gen_cmd << perf_cmd << " script -i " << tmp_file.str() << " | " << stackcollapse_perf_pl
                << " | " << flamegraph_pl << " > " << graph_file.str();
        LOG(INFO) << "begin to run command: " << gen_cmd.str();
        rc = util.exec_cmd(gen_cmd.str(), &res_content);
        if (!rc) {
            static_cast<void>(io::global_local_filesystem()->delete_file(tmp_file.str()));
            static_cast<void>(io::global_local_filesystem()->delete_file(graph_file.str()));
            return Status::InternalError("Failed to execute perf script command: {}", res_content);
        }
        *svg_file_or_content = graph_file.str();
    } else {
        std::stringstream gen_cmd;
        gen_cmd << perf_cmd << " script -i " << tmp_file.str() << " | " << stackcollapse_perf_pl
                << " | " << flamegraph_pl;
        LOG(INFO) << "begin to run command: " << gen_cmd.str();
        rc = util.exec_cmd(gen_cmd.str(), &res_content, false);
        if (!rc) {
            static_cast<void>(io::global_local_filesystem()->delete_file(tmp_file.str()));
            return Status::InternalError("Failed to execute perf script command: {}", res_content);
        }
        *svg_file_or_content = res_content;
    }
    return Status::OK();
}

} // namespace doris
