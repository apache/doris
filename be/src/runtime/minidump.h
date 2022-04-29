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

#include <signal.h>

#include "client/linux/handler/exception_handler.h"
#include "common/status.h"
#include "util/thread.h"

namespace doris {

// A wrapper of minidump from breakpad.
// Used to write minidump file to config::minidump_dir when BE crashes.
// And user can also trigger to write a minidump by sending SIGUSR1 to BE, eg:
//      kill -s SIGUSR1 be_pid
class Minidump {
public:
    Minidump() {};
    ~Minidump() {};

    Status init();

    // stop and join the minidump clean thread;
    void stop();

private:
    // The callback after writing the minidump file
    static bool _minidump_cb(const google_breakpad::MinidumpDescriptor& descriptor, void* context,
                             bool succeeded);
    // The handle function when receiving SIGUSR1 signal.
    static void _usr1_sigaction(int signum, siginfo_t* info, void* context);

    // try clean old minidump files periodically.
    // To keep at most config::max_minidump_number files.
    void _clean_old_minidump();

    // Setup hanlder for SIGUSR1
    Status _setup_sig_handler();

private:
    static int _signo;
    static std::unique_ptr<google_breakpad::ExceptionHandler> _error_handler;

    std::atomic<bool> _stop = false;
    scoped_refptr<Thread> _clean_thread;
};

} // namespace doris
