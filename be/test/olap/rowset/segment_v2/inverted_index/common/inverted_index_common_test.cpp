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

#include "olap/rowset/segment_v2/inverted_index_common.h"

#include <gtest/gtest.h>

#include "common/status.h"

namespace doris::segment_v2 {

class InvertedIndexCommonTest : public testing::Test {
public:
    void SetUp() override {}

    void TearDown() override {}

    InvertedIndexCommonTest() = default;
    ~InvertedIndexCommonTest() override = default;
};

TEST_F(InvertedIndexCommonTest, TestFinallyClose) {
    class InvertedIndexBase {
    public:
        InvertedIndexBase(int32_t& count) : count_(count) {}

        void close() { count_++; }
        void clear() { count_++; }

        int32_t& count_;
    };
    {
        int32_t count = 0;
        {
            ErrorContext error_context;
            auto ptr = std::make_shared<InvertedIndexBase>(count);
            finally_close(ptr, error_context);
        }
        EXPECT_EQ(count, 1);
    }
    {
        int32_t count = 0;
        {
            ErrorContext error_context;
            auto ptr = std::shared_ptr<InvertedIndexBase>(new InvertedIndexBase(count),
                                                          [](InvertedIndexBase* p) {
                                                              if (p) {
                                                                  p->clear();
                                                                  delete p;
                                                                  p = nullptr;
                                                              }
                                                          });
            finally_close(ptr, error_context);
        }
        EXPECT_EQ(count, 2);
    }
    {
        int32_t count = 0;
        {
            ErrorContext error_context;
            auto ptr = std::make_unique<InvertedIndexBase>(count);
            finally_close(ptr, error_context);
        }
        EXPECT_EQ(count, 1);
    }
    {
        struct Deleter {
            void operator()(InvertedIndexBase* p) const {
                if (p) {
                    p->clear();
                    delete p;
                    p = nullptr;
                }
            }
        };

        int32_t count = 0;
        {
            ErrorContext error_context;
            auto ptr = std::unique_ptr<InvertedIndexBase, Deleter>(new InvertedIndexBase(count));
            finally_close(ptr, error_context);
        }
        EXPECT_EQ(count, 2);
    }
}

TEST_F(InvertedIndexCommonTest, TestTryBlockException) {
    class InvertedIndexBase {
    public:
        void add() { _CLTHROWA(CL_ERR_IO, "test add error"); }
        void close() {}
    };

    // return error
    {
        auto func = []() -> Status {
            auto base_ptr = std::make_unique<InvertedIndexBase>();
            ErrorContext error_context;
            try {
                base_ptr->add();
            } catch (CLuceneError& e) {
                error_context.eptr = std::current_exception();
                error_context.err_msg.append("error: ");
                error_context.err_msg.append(e.what());
            }
            FINALLY({
                EXPECT_TRUE(error_context.eptr);
                FINALLY_CLOSE(base_ptr);
            })
            return Status::OK();
        };
        auto ret = func();
        EXPECT_EQ(ret.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    }

    // throw exception
    {
        auto func = []() {
            auto base_ptr = std::make_unique<InvertedIndexBase>();
            ErrorContext error_context;
            try {
                base_ptr->add();
            } catch (CLuceneError& e) {
                error_context.eptr = std::current_exception();
                error_context.err_msg.append("error: ");
                error_context.err_msg.append(e.what());
            }
            FINALLY_EXCEPTION({
                EXPECT_TRUE(error_context.eptr);
                FINALLY_CLOSE(base_ptr);
            })
        };
        bool is_exception = false;
        try {
            func();
        } catch (CLuceneError& e) {
            EXPECT_EQ(e.number(), CL_ERR_IO);
            is_exception = true;
        }
        EXPECT_TRUE(is_exception);
    }
}

TEST_F(InvertedIndexCommonTest, TestFinallyBlockException) {
    class InvertedIndexBase {
    public:
        void add() {}
        void close() { _CLTHROWA(CL_ERR_Runtime, "test close error"); }
    };

    // return error
    {
        auto func = []() -> Status {
            auto base_ptr = std::make_unique<InvertedIndexBase>();
            ErrorContext error_context;
            try {
                base_ptr->add();
            } catch (CLuceneError& e) {
                error_context.eptr = std::current_exception();
                error_context.err_msg.append("error: ");
                error_context.err_msg.append(e.what());
            }
            FINALLY({
                EXPECT_FALSE(error_context.eptr);
                FINALLY_CLOSE(base_ptr);
                EXPECT_TRUE(error_context.eptr);
            })
            return Status::OK();
        };
        auto ret = func();
        EXPECT_EQ(ret.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    }

    // throw exception
    {
        auto func = []() {
            auto base_ptr = std::make_unique<InvertedIndexBase>();
            ErrorContext error_context;
            try {
                base_ptr->add();
            } catch (CLuceneError& e) {
                error_context.eptr = std::current_exception();
                error_context.err_msg.append("error: ");
                error_context.err_msg.append(e.what());
            }
            FINALLY_EXCEPTION({
                EXPECT_FALSE(error_context.eptr);
                FINALLY_CLOSE(base_ptr);
                EXPECT_TRUE(error_context.eptr);
            })
        };
        bool is_exception = false;
        try {
            func();
        } catch (CLuceneError& e) {
            EXPECT_EQ(e.number(), CL_ERR_Runtime);
            is_exception = true;
        }
        EXPECT_TRUE(is_exception);
    }
}

TEST_F(InvertedIndexCommonTest, TestTryAndFinallyBlockException) {
    class InvertedIndexBase {
    public:
        void add() { _CLTHROWA(CL_ERR_IO, "test add error"); }
        void close() { _CLTHROWA(CL_ERR_Runtime, "test close error"); }
    };

    // return error
    {
        auto func = []() -> Status {
            auto base_ptr = std::make_unique<InvertedIndexBase>();
            ErrorContext error_context;
            try {
                base_ptr->add();
            } catch (CLuceneError& e) {
                error_context.eptr = std::current_exception();
                error_context.err_msg.append("error: ");
                error_context.err_msg.append(e.what());
            }
            FINALLY({
                EXPECT_TRUE(error_context.eptr);
                FINALLY_CLOSE(base_ptr);
                EXPECT_TRUE(error_context.eptr);
            })
            return Status::OK();
        };
        auto ret = func();
        EXPECT_EQ(ret.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    }

    // throw exception
    {
        auto func = []() {
            auto base_ptr = std::make_unique<InvertedIndexBase>();
            ErrorContext error_context;
            try {
                base_ptr->add();
            } catch (CLuceneError& e) {
                error_context.eptr = std::current_exception();
                error_context.err_msg.append("error: ");
                error_context.err_msg.append(e.what());
            }
            FINALLY_EXCEPTION({
                EXPECT_TRUE(error_context.eptr);
                FINALLY_CLOSE(base_ptr);
                EXPECT_TRUE(error_context.eptr);
            })
        };
        bool is_exception = false;
        try {
            func();
        } catch (CLuceneError& e) {
            EXPECT_EQ(e.number(), CL_ERR_Runtime);
            is_exception = true;
        }
        EXPECT_TRUE(is_exception);
    }
}

TEST_F(InvertedIndexCommonTest, TestRawPointerException) {
    class InvertedIndexBase {
    public:
        void add() { _CLTHROWA(CL_ERR_IO, "test add error"); }
        void close() { _CLTHROWA(CL_ERR_Runtime, "test close error"); }
    };

    // return error
    {
        auto func = []() -> Status {
            auto* base_ptr = new InvertedIndexBase();
            ErrorContext error_context;
            try {
                base_ptr->add();
            } catch (CLuceneError& e) {
                error_context.eptr = std::current_exception();
                error_context.err_msg.append("error: ");
                error_context.err_msg.append(e.what());
            }
            FINALLY({
                EXPECT_TRUE(error_context.eptr);
                FINALLY_CLOSE(base_ptr);
                if (base_ptr) {
                    delete base_ptr;
                    base_ptr = nullptr;
                }
                EXPECT_TRUE(error_context.eptr);
            })
            return Status::OK();
        };
        auto ret = func();
        EXPECT_EQ(ret.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    }

    // throw exception
    {
        auto func = []() {
            auto* base_ptr = new InvertedIndexBase();
            ErrorContext error_context;
            try {
                base_ptr->add();
            } catch (CLuceneError& e) {
                error_context.eptr = std::current_exception();
                error_context.err_msg.append("error: ");
                error_context.err_msg.append(e.what());
            }
            FINALLY_EXCEPTION({
                EXPECT_TRUE(error_context.eptr);
                FINALLY_CLOSE(base_ptr);
                if (base_ptr) {
                    delete base_ptr;
                    base_ptr = nullptr;
                }
                EXPECT_TRUE(error_context.eptr);
            })
        };
        bool is_exception = false;
        try {
            func();
        } catch (CLuceneError& e) {
            EXPECT_EQ(e.number(), CL_ERR_Runtime);
            is_exception = true;
        }
        EXPECT_TRUE(is_exception);
    }
}

} // namespace doris::segment_v2