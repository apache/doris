#include <gtest/gtest.h>
#include <rate-limiter/s3_rate_limiter.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "common/configbase.h"
#include "common/logging.h"

using namespace doris::cloud;

int main(int argc, char** argv) {
    auto conf_file = "doris_cloud.conf";
    if (!doris::cloud::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!doris::cloud::init_glog("util")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(S3RateLimiterTest, normal) {
    auto rate_limiter = S3RateLimiter(1, 5, 10);
    std::atomic_int64_t failed;
    std::atomic_int64_t succ;
    std::atomic_int64_t sleep_thread_num;
    std::atomic_int64_t sleep;
    auto request_thread = [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        auto ms = rate_limiter.add(1);
        if (ms < 0) {
            failed++;
        } else if (ms == 0) {
            succ++;
        } else {
            sleep += ms;
            sleep_thread_num++;
        }
    };
    {
        std::vector<std::thread> threads;
        for (size_t i = 0; i < 20; i++) {
            threads.emplace_back(request_thread);
        }
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
    ASSERT_EQ(failed, 10);
    ASSERT_EQ(succ, 6);
    ASSERT_EQ(sleep_thread_num, 4);
}