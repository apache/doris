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

#include "runtime/minidump.h"

#include <sys/stat.h>
#include <unistd.h>

#include "common/config.h"
#include "env/env.h"
#include "util/file_utils.h"
#include "util/string_util.h"

#include "client/linux/handler/exception_handler.h"

namespace doris {

int Minidump::_signo = SIGUSR1;
std::unique_ptr<google_breakpad::ExceptionHandler> Minidump::_error_handler = nullptr;

// Save the absolute path and create_time of a minidump file
struct FileStat {
    std::string abs_path;
    time_t create_time;

    FileStat(const std::string& path_, time_t ctime) : abs_path(path_), create_time(ctime) {}
};

Status Minidump::init() {
    if (config::disable_minidump) {
        LOG(INFO) << "minidump is disabled";
        return Status::OK();
    }

    // 1. create minidump dir
    RETURN_IF_ERROR(FileUtils::create_dir(config::minidump_dir));

    // 2. create ExceptionHandler
    google_breakpad::MinidumpDescriptor minidump_descriptor(config::minidump_dir);
    if (config::max_minidump_file_size_mb > 0) {
        minidump_descriptor.set_size_limit(config::max_minidump_file_size_mb * 1024 * 1024);
    }
    _error_handler.reset(new google_breakpad::ExceptionHandler(minidump_descriptor, nullptr,
                                                               _minidump_cb, nullptr, true, -1));

    // 3. setup sig handler
    _setup_sig_handler();

    RETURN_IF_ERROR(Thread::create(
            "Minidump", "minidump_clean_thread", [this]() { this->_clean_old_minidump(); },
            &_clean_thread));

    LOG(INFO) << "Minidump is enabled. dump file will be saved at " << config::minidump_dir;
    return Status::OK();
}

Status Minidump::_setup_sig_handler() {
    struct sigaction sig_action;
    memset(&sig_action, 0, sizeof(sig_action));
    sigemptyset(&sig_action.sa_mask);

    sig_action.sa_flags = SA_SIGINFO; // use sa_sigaction instead of sa_handler
    sig_action.sa_sigaction = &(this->_usr1_sigaction);
    if (sigaction(_signo, &sig_action, nullptr) == -1) {
        return Status::InternalError("failed to install signal handler for " +
                                     std::to_string(_signo));
    }
    return Status::OK();
}

void Minidump::_usr1_sigaction(int signum, siginfo_t* info, void* context) {
    const char* msg = "Receive signal: SIGUSR1\n";
    sys_write(STDOUT_FILENO, msg, strlen(msg));
    _error_handler->WriteMinidump();
}

bool Minidump::_minidump_cb(const google_breakpad::MinidumpDescriptor& descriptor, void* context,
                            bool succeeded) {
    // use sys_write supported by `linux syscall`, recommended by breakpad doc.
    const char* msg = "Minidump created at: ";
    sys_write(STDOUT_FILENO, msg, strlen(msg));
    msg = descriptor.path();
    sys_write(STDOUT_FILENO, msg, strlen(msg));
    sys_write(STDOUT_FILENO, "\n", 1);

    // Reference from kudu, return false so that breakpad will invoke any
    // previously-installed signal handler of glog.
    // So that we can get the error stack trace directly in be.out without
    // anlayzing minidump file, which is more friendly for debugging.
    return false;
}

void Minidump::stop() {
    if (_stop) {
        return;
    }
    _stop = true;
    _clean_thread->join();
}

void Minidump::_clean_old_minidump() {
    while (!_stop) {
        sleep(10);
        if (config::max_minidump_file_number <= 0) {
            continue;
        }

        // list all files
        std::vector<std::string> files;
        FileUtils::list_files(Env::Default(), config::minidump_dir, &files);
        for (auto it = files.begin(); it != files.end();) {
            if (!ends_with(*it, ".dmp")) {
                it = files.erase(it);
            } else {
                it++;
            }
        }
        if (files.size() <= config::max_minidump_file_number) {
            continue;
        }

        // check file create time and sort and save in stats
        int ret = 0;
        std::vector<FileStat> stats;
        for (auto it = files.begin(); it != files.end(); ++it) {
            std::string path = config::minidump_dir + "/" + *it;

            struct stat buf;
            if ((ret = stat(path.c_str(), &buf)) != 0) {
                LOG(WARNING) << "Failed to stat minidump file: " << path
                             << ", remote it. errno: " << ret;
                FileUtils::remove(path);
                continue;
            }

            stats.emplace_back(path, buf.st_ctime);
        }

        // sort file by ctime ascending
        std::sort(stats.begin(), stats.end(), [](const FileStat& f1, const FileStat& f2) {
            if (f1.create_time > f2.create_time) {
                return false;
            } else {
                return true;
            }
        });

        int to_delete = stats.size() - config::max_minidump_file_number;
        int deleted = 0;
        for (auto it = stats.begin(); it != stats.end() && deleted < to_delete; it++, deleted++) {
            FileUtils::remove(it->abs_path);
        }
        LOG(INFO) << "delete " << deleted << " minidump files";
    }
}

} // namespace doris
