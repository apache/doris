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

#include "common/config.h"
#include "util/file_utils.h"

#include "client/linux/handler/exception_handler.h"

namespace doris {

int Minidump::_signo = SIGUSR1;
std::unique_ptr<google_breakpad::ExceptionHandler> Minidump::_error_handler = nullptr;

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
    _error_handler.reset(new google_breakpad::ExceptionHandler(minidump_descriptor, nullptr, _minidump_cb, nullptr, true, -1));

    // 3. setup sig handler
    _setup_sig_handler(); 

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
        return Status::InternalError("failed to install signal handler for " + std::to_string(_signo));
    }
    return Status::OK();
}

void Minidump::_usr1_sigaction(int signum, siginfo_t* info, void* context) {
    const char* msg = "Receive signal: SIGUSR1\n";
    sys_write(STDOUT_FILENO, msg, strlen(msg));
    _error_handler->WriteMinidump();
}

bool Minidump::_minidump_cb(const google_breakpad::MinidumpDescriptor& descriptor,
                        void* context, bool succeeded) {
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

} // namespace doris
