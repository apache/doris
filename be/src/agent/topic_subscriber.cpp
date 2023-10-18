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

#include "agent/topic_subscriber.h"

#include <pthread.h>

#include <mutex>
#include <utility>

#include "agent/topic_listener.h"

namespace doris {

TopicSubscriber::TopicSubscriber() {}

TopicSubscriber::~TopicSubscriber() {
    for (auto iter = _registered_listeners.begin(); iter != _registered_listeners.end(); iter++) {
        delete iter->second;
    }
}

void TopicSubscriber::register_listener(TTopicInfoType::type topic_type,
                                        TopicListener* topic_listener) {
    // Unique lock here to prevent access to listeners
    std::lock_guard<std::shared_mutex> lock(_listener_mtx);
    this->_registered_listeners.emplace(topic_type, topic_listener);
}

void TopicSubscriber::handle_topic_info(const TPublishTopicRequest& topic_request) {
    // Shared lock here in order to avoid updates in listeners' map
    std::shared_lock lock(_listener_mtx);
    pthread_t tids[_registered_listeners.size()];
    int i = 0;
    for (auto& listener_pair : _registered_listeners) {
        ThreadArgsPair* arg_pair = new ThreadArgsPair();
        arg_pair->first = listener_pair.second;
        arg_pair->second = new TPublishTopicRequest(topic_request);
        int ret = pthread_create(
                &tids[i], NULL,
                [](void* arg) {
                    auto* local_pair = (ThreadArgsPair*)(arg);
                    local_pair->first->handle_topic_info(*(local_pair->second));
                    delete local_pair->second;
                    delete local_pair;
                    return (void*)0;
                },
                (void*)(arg_pair));
        if (ret != 0) {
            LOG(WARNING) << "create thread failed when handle topic info";
        }
        i++;
    }
}
} // namespace doris
