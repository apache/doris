// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

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

#include "util/rpc_channel.h"

#include <gtest/gtest.h>

#include "rpc/connection_handler_factory.h"
#include "rpc/connection_manager.h"
#include "rpc/dispatch_handler.h"
#include "rpc/error.h"
#include "rpc/event.h"
#include "rpc/reactor_factory.h"

namespace palo {

class TestFactory : public ConnectionHandlerFactory {
public:
    TestFactory(DispatchHandlerPtr handler) : _handler(handler) { }
    virtual ~TestFactory() { }
    void get_instance(DispatchHandlerPtr &dhp) override {
        dhp = _handler;
    }
private:
    DispatchHandlerPtr _handler;
};

class TestDispacher : public DispatchHandler {
public:
    TestDispacher(Comm* comm) : _comm(comm) { }
    virtual ~TestDispacher() { }

    void handle(EventPtr& event) override {
        if (event->type == Event::CONNECTION_ESTABLISHED) {
            LOG(INFO) << "Connection Established.";
        } else if (event->type == Event::DISCONNECT) {
            if (event->error != 0) {
                LOG(INFO) << "Disconnect : " << error::get_text(event->error);
            } else {
                LOG(INFO) << "Disconnect";
            }
        } else if (event->type == Event::ERROR) {
            LOG(ERROR) << "Error: " << error::get_text(event->error);
        } else if (event->type == Event::MESSAGE) {
            const uint8_t *buf_ptr = (const uint8_t*)event->payload;
            if (buf_ptr[0] == 123) {
                // ignore this packet
                return;
            }
            CommHeader header;
            header.initialize_from_request_header(event->header);
            CommBufPtr response(new CommBuf(header, event->payload_len));
            response->append_bytes(event->payload, event->payload_len);
            int error = _comm->send_response(event->addr, response);
            if (error != error::OK) {
                LOG(ERROR) << "Comm::send_response returned" << error::get_text(error);
            }
        }
    }
private:
    Comm* _comm;
};

class RpcChannelTest : public testing::Test {
public:
    RpcChannelTest() { }
    virtual ~RpcChannelTest() {
    }
    static void SetUpTestCase() {
        ReactorFactory::initialize(1);
        _comm.reset(new Comm("127.0.0.1"));
        _conn_mgr = std::make_shared<ConnectionManager>(_comm.get());
        DispatchHandlerPtr dhp = std::make_shared<TestDispacher>(_comm.get());
        ConnectionHandlerFactoryPtr factory = std::make_shared<TestFactory>(dhp);

        struct sockaddr_in addr;
        InetAddr::initialize(&addr, "127.0.0.1", 25437);
        _comm->listen(addr, factory, dhp);
    }
    void SetUp() override { }

private:
    static std::unique_ptr<Comm> _comm;
    static ConnectionManagerPtr _conn_mgr;
};

std::unique_ptr<Comm> RpcChannelTest::_comm;
ConnectionManagerPtr RpcChannelTest::_conn_mgr;

TEST_F(RpcChannelTest, normal) {
    RpcChannelPtr channel = std::make_shared<RpcChannel>(_comm.get(), _conn_mgr, 0);
    auto st = channel->init("127.0.0.1", 25437, 500, 1000);
    ASSERT_TRUE(st.ok());

    uint8_t buf[16];
    for (int i = 0; i < 16; ++i) {
        buf[i] = i;
    }
    st = channel->send_message(buf, 16);
    ASSERT_TRUE(st.ok());
    const uint8_t* rep_buf = nullptr;
    uint32_t rep_size = 0;
    st = channel->get_response(&rep_buf, &rep_size);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(16, rep_size);
    for (int i = 0; i < rep_size; ++i) {
        ASSERT_EQ(i, rep_buf[i]);
    }
}

TEST_F(RpcChannelTest, send_fail) {
    RpcChannelPtr channel = std::make_shared<RpcChannel>(_comm.get(), _conn_mgr, 0);
    auto st = channel->init("127.0.0.1", 25437, 100, 100);
    ASSERT_TRUE(st.ok());

    uint8_t buf[16];
    memset(buf, 0, 16);
    // make reponse ignore this packet
    buf[0] = 123;
    st = channel->send_message(buf, 16);
    ASSERT_TRUE(st.ok());
    const uint8_t* rep_buf = nullptr;
    uint32_t rep_size = 0;
    st = channel->get_response(&rep_buf, &rep_size);
    ASSERT_FALSE(st.ok());
}

TEST_F(RpcChannelTest, disconnect) {
    RpcChannelPtr channel = std::make_shared<RpcChannel>(_comm.get(), _conn_mgr, 0);
    auto st = channel->init("127.0.0.1", 25437, 100, 100);
    ASSERT_TRUE(st.ok());

    EventPtr event = std::make_shared<Event>(Event::DISCONNECT);
    channel->handle(event);
    uint8_t buf[16];
    for (int i = 0; i < 16; ++i) {
        buf[i] = i;
    }
    st = channel->send_message(buf, 16);
    ASSERT_TRUE(st.ok());
    const uint8_t* rep_buf = nullptr;
    uint32_t rep_size = 0;
    st = channel->get_response(&rep_buf, &rep_size);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(16, rep_size);
    for (int i = 0; i < rep_size; ++i) {
        ASSERT_EQ(i, rep_buf[i]);
    }
}

TEST_F(RpcChannelTest, unknown_port) {
    RpcChannelPtr channel = std::make_shared<RpcChannel>(_comm.get(), _conn_mgr, 0);
    auto st = channel->init("127.0.0.1", 25438, 100, 100);
    ASSERT_TRUE(st.ok());

    EventPtr event = std::make_shared<Event>(Event::DISCONNECT);
    channel->handle(event);
    uint8_t buf[16];
    memset(buf, 0, 16);
    st = channel->send_message(buf, 16);
    ASSERT_TRUE(st.ok());
    const uint8_t* rep_buf = nullptr;
    uint32_t rep_size = 0;
    st = channel->get_response(&rep_buf, &rep_size);
    ASSERT_FALSE(st.ok());
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
