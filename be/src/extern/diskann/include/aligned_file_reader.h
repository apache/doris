// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#define MAX_IO_DEPTH 128

#include <vector>
#include <atomic>

#include <fcntl.h>
#include <libaio.h>
#include <unistd.h>
typedef io_context_t IOContext;

#include <malloc.h>
#include <cstdio>
#include <mutex>
#include <thread>
#include "tsl/robin_map.h"
#include "utils.h"

// NOTE :: all 3 fields must be 512-aligned
struct AlignedRead
{
    uint64_t offset; // where to read from
    uint64_t len;    // how much to read
    void *buf;       // where to read into

    AlignedRead() : offset(0), len(0), buf(nullptr)
    {
    }

    AlignedRead(uint64_t offset, uint64_t len, void *buf) : offset(offset), len(len), buf(buf)
    {
        assert(IS_512_ALIGNED(offset));
        assert(IS_512_ALIGNED(len));
        assert(IS_512_ALIGNED(buf));
        // assert(malloc_usable_size(buf) >= len);
    }
};

class AlignedFileReader
{
  protected:
    tsl::robin_map<std::thread::id, IOContext> ctx_map;
    std::mutex ctx_mut;

  public:
    // returns the thread-specific context
    // returns (io_context_t)(-1) if thread is not registered
    virtual IOContext &get_ctx() = 0;

    virtual ~AlignedFileReader(){};

    // register thread-id for a context
    virtual void register_thread() = 0;
    // de-register thread-id for a context
    virtual void deregister_thread() = 0;
    virtual void deregister_all_threads() = 0;

    // Open & close ops
    // Blocking calls
    virtual void open(const std::string &fname) = 0;
    virtual void close() = 0;

    // process batch of aligned requests in parallel
    // NOTE :: blocking call
    virtual void read(std::vector<AlignedRead> &read_reqs, IOContext &ctx, bool async = false) = 0;
};