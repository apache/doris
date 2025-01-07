#include <gtest/gtest.h>
#include <unistd.h> // For sleep() function

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "util/threadpool.h"

// External global variables defined in run_all_tests.cpp
extern int mode;
extern int thread_pool_size;
extern int thread_pool_queue_size;
extern int producer_size;
extern int duration;

std::mutex mtx;                     // Mutex for locking
std::atomic<int> shared_counter(0); // Shared atomic counter for locks
std::ofstream fake_io_file;         // Fake IO file for simulation
std::atomic<bool> global_stop = false;     // Atomic flag for stopping threads
std::atomic_int64_t finished_producers(0); // Atomic counter for finished producers

void simulate_io(int id) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // 模拟延时
    std::lock_guard<std::mutex> lock(mtx);
    fake_io_file << "Thread " << id << " performed I/O operation\n";
}

class ThreadPoolCustom {
public:
    ThreadPoolCustom(size_t pool_size);
    ~ThreadPoolCustom();

    using EnqueueSucceed = bool;

    template<class F, class... Args>
    using EnqueueResult = std::variant<EnqueueSucceed, std::future<typename std::invoke_result<F, Args...>::type>>;

    template<class F, class... Args>
    EnqueueResult<F, Args...> enqueue(F&& f, Args&&... args);
    
    void stop();

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop_flag;
};

ThreadPoolCustom::ThreadPoolCustom(size_t pool_size) : stop_flag(false) {
    for (size_t i = 0; i < pool_size; ++i) {
        workers.emplace_back([this] {
            for (;;) {
                std::function<void()> task;

                {
                    std::unique_lock<std::mutex> lock(this->queue_mutex);
                    this->condition.wait(lock, [this] { return this->stop_flag || !this->tasks.empty(); });
                    

                    // Finish all tasks before stopping
                    if (!this->tasks.empty()) {
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    } else {
                        if (stop_flag) {
                            return;
                        }               
                    }
                }

                task();
            }
        });
    }
}

ThreadPoolCustom::~ThreadPoolCustom() {
    stop();
}

template<class F, class... Args>
auto ThreadPoolCustom::enqueue(F&& f, Args&&... args) -> std::variant<bool, std::future<typename std::invoke_result<F, Args...>::type>> {
    using return_type = typename std::invoke_result<F, Args...>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        if (stop_flag) {
            return std::variant<bool, std::future<return_type>>(false);
        }

        tasks.emplace([task]() { (*task)(); });
    }
    condition.notify_one();
    return std::variant<bool, std::future<return_type>>(std::move(res));
}

void ThreadPoolCustom::stop() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if (stop_flag) {
            return;
        }
        std::cout << "ThreadPool is set stopped...\n";
        stop_flag = true;
    }
    condition.notify_all();
    std::cout << "Waiting for "<< workers.size() << " workers to join, existing tasks " << tasks.size() << "...\n";
    for (std::thread &worker : workers) {
        worker.join();
    }
}

class TaskProducerCustom {
public:
    TaskProducerCustom(ThreadPoolCustom& pool, int id, bool io_flag = true)
        : pool(pool), id(id), io_flag(io_flag), rand_i32(std::rand()){}

    void produce_tasks() {
        auto task = [this] {
            double result = (rand_i32 * 3.14) / 2.71;
            for (int i = 0; i < 1000000; ++i) {
                 result += (rand_i32 * 3.14) / 2.71;
            }
            if (io_flag) {
                simulate_io(id);
            }
            return result;
        };

        while (true) {
            auto res = pool.enqueue(task);

            if (std::holds_alternative<bool>(res) && !std::get<bool>(res)) {
                break; // Stop if enqueue failed
            } else {
                // Get result from task.
                std::future<double> future = std::move(std::get<std::future<double>>(res));
                future.wait();
                result += future.get();
                finished_producers.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

private:
    ThreadPoolCustom& pool;
    int id;
    bool io_flag;
    // std::rand 有性能问题，std::rand() 内部是需要加锁的
    int rand_i32;
    std::atomic<double> result = 0;
};

void thread_pool_with_timed_task_producer_mode_custom(std::chrono::seconds duration, int thread_count, int producer_count, bool io_flag) {
    std::cerr << "Starting ThreadPool with " << thread_count << " threads and " << producer_count << " producers...\n";
    ThreadPoolCustom pool(thread_count);

    std::vector<std::thread> producers;
    for (int i = 0; i < producer_count; ++i) {
        producers.emplace_back([&pool, i, io_flag] {
            TaskProducerCustom producer(pool, i, io_flag);
            producer.produce_tasks();
        });
    }

    std::this_thread::sleep_for(duration);
    std::cerr << "Stopping ThreadPool...\n";
    pool.stop(); // Signal all producers to stop
    for (auto& producer : producers) {
        producer.join();
    }
    std::cerr << "ThreadPool stopped\n";
    std::cout << "Finished " << finished_producers.load() << " producers, qps is " << finished_producers.load() / duration.count() << "\n";
}


class TaskProducer {
public:
    TaskProducer(doris::ThreadPool& pool, int id, bool io_flag = true)
            : pool(pool), id(id), io_flag(io_flag), rand_i32(std::rand()) {}

    void produce_tasks() {
        auto task = [this] {
            double result = 0;
            for (int i = 0; i < 1000000; ++i) {
                 result += (rand_i32 * 3.14) / 2.71;
            }
            // if (io_flag) {
            //     simulate_io(id);
            // }
            std::unique_lock<std::mutex> lg(this->mtx);
            task_finised = true;
            finished_producers.fetch_add(1);
            // notidy producer threads.
            cv.notify_one();
            return result;
        };

        while (!global_stop) {
            task_finised = false;
            auto res = pool.submit_func(task);
            if (!res.ok()) {
                if (global_stop) {
                    return;
                }
                sleep(1);
                continue;
            }

            std::unique_lock<std::mutex> lk(mtx);
            cv.wait_for(lk, std::chrono::seconds(1), [this] { return task_finised;});
            if (task_finised) {
                finished_producers.fetch_add(1);
            }
        }
    }

private:
    doris::ThreadPool& pool;
    [[maybe_unused]] int id;
    [[maybe_unused]] bool io_flag;
    int rand_i32;
    bool task_finised = false;
    std::mutex mtx;             // Mutex for locking
    std::condition_variable cv; // Condition variable for synchronization
};

class ThreadPoolPerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize thread pool based on command line arguments
        doris::ThreadPoolBuilder builder("PerformanceTestPool");
        builder.set_min_threads(thread_pool_size)
                .set_max_threads(thread_pool_size)
                .set_max_queue_size(thread_pool_queue_size);
        std::ignore = builder.build(&_pool);
    }

    void TearDown() override {
        // Clean up thread pool
        _pool.reset();
    }

    void parallel_mode(std::chrono::seconds duration, int thread_count) {
        std::vector<std::thread> threads;
        std::atomic<bool> stop_flag(false);
        // std::rand 有性能问题，std::rand() 内部是需要加锁的

        for (int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&stop_flag, this]() {
                volatile double result; // Use volatile to prevent optimization
                while (!stop_flag.load()) {
                    auto rand_i32 = std::rand();
                    result = (rand_i32 * 3.14) / 2.71;
                    finished_producers.fetch_add(1);
                }
                results.fetch_add(result);
            });
        }

        std::this_thread::sleep_for(duration);
        stop_flag.store(true);

        for (auto& t : threads) {
            t.join();
        }

        std::cout << "Finished " << finished_producers.load() << " producers, qps is "
                  << finished_producers.load() / duration.count() << "\n";
    }

    void thread_pool_with_timed_task_producer_mode(std::chrono::seconds duration, int thread_count,
                                                   int producer_count) {
        std::cerr << "Starting ThreadPool with " << thread_count << " threads and "
                  << producer_count << " producers...\n";

        std::vector<std::thread> producers;
        doris::ThreadPool* pool = _pool.get();
        for (int i = 0; i < producer_count; ++i) {
            producers.emplace_back([pool, i] {
                TaskProducer producer(*pool, i, false);
                producer.produce_tasks();
            });
        }

        std::this_thread::sleep_for(duration);
        std::cerr << "Stopping ThreadPool...\n";
        global_stop = true;
        _pool->shutdown();
        for (auto& producer : producers) {
            producer.join();
        }
        std::cerr << "ThreadPool stopped\n";
        std::cout << "Finished " << finished_producers.load()
                << " producers, qps is " << finished_producers.load() / duration.count() << "\n";
    }

private:
    std::unique_ptr<doris::ThreadPool> _pool;
    std::mutex mtx;                             // Mutex for locking
    std::atomic<int> shared_counter = 0;        // Shared atomic counter for locks
    std::ofstream fake_io_file;                 // Fake IO file for simulation
    std::atomic<double> results = 0;            // Atomic counter for finished producers
};

TEST_F(ThreadPoolPerformanceTest, TestSimpleTasks) {
    switch (mode) {
    case 1:
        std::cout << "Running parallel mode with " << producer_size << " producers\n";
        parallel_mode(std::chrono::seconds(duration), producer_size);
        break;
    case 2:
        std::cout << "Running Thread Pool with Timed Task Producer mode...\n";
        thread_pool_with_timed_task_producer_mode(std::chrono::seconds(duration), thread_pool_size,
                                                  producer_size);
        break;
    case 3:
        std::cout << "Running ThreadPoolCustom with Timed Task Producer mode...\n";
        thread_pool_with_timed_task_producer_mode_custom(std::chrono::seconds(duration), thread_pool_size,
                                                  producer_size, false);
        break;
    default:
        std::cout << "Invalid mode: " << mode << "\n";
        break;
    }
}