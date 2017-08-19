// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "compat.h"
#include "handler_map.h"
#include "reactor_factory.h"
#include "reactor_runner.h"

#include <cassert>

extern "C" {
#include <signal.h>
}

namespace palo {

std::vector<ReactorPtr> ReactorFactory::ms_reactors;
boost::thread_group ReactorFactory::ms_threads;
std::default_random_engine ReactorFactory::rng {1};
std::mutex ReactorFactory::ms_mutex;
std::atomic<int> ReactorFactory::ms_next_reactor(0);
bool ReactorFactory::ms_epollet = true;
bool ReactorFactory::proxy_master = false;

void ReactorFactory::initialize(uint16_t reactor_count) {
    std::lock_guard<std::mutex> lock(ms_mutex);
    if (!ms_reactors.empty())
        return;
    ReactorPtr reactor;
    ReactorRunner rrunner;
    ReactorRunner::handler_map = std::make_shared<HandlerMap>();
    signal(SIGPIPE, SIG_IGN);
    assert(reactor_count > 0);
    ms_reactors.reserve(reactor_count+2);
    for (uint16_t i=0; i<reactor_count+2; i++) {
        reactor = std::make_shared<Reactor>();
        ms_reactors.push_back(reactor);
        rrunner.set_reactor(reactor);
        ms_threads.create_thread(rrunner);
    }
}

void ReactorFactory::destroy() {
    ReactorRunner::shutdown = true;
    for (size_t i=0; i<ms_reactors.size(); i++) {
        ms_reactors[i]->poll_loop_interrupt();
    }
    ms_threads.join_all();
    ms_reactors.clear();
    ReactorRunner::handler_map = 0;
}

void ReactorFactory::join() {
    ms_threads.join_all();
}

} //namespace palo
