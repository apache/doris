// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include "Windows.h"

namespace diskann
{
// A thin C++ wrapper around Windows exclusive functionality of Windows
// SlimReaderWriterLock.
//
// The SlimReaderWriterLock is simpler/more lightweight than std::mutex
// (8 bytes vs 80 bytes), which is useful in the scenario where DiskANN has
// one lock per vector in the index. It does not support recursive locking and
// requires Windows Vista or later.
//
// Full documentation can be found at.
// https://msdn.microsoft.com/en-us/library/windows/desktop/aa904937(v=vs.85).aspx
class windows_exclusive_slim_lock
{
  public:
    windows_exclusive_slim_lock() : _lock(SRWLOCK_INIT)
    {
    }

    // The lock is non-copyable. This also disables move constructor/operator=.
    windows_exclusive_slim_lock(const windows_exclusive_slim_lock &) = delete;
    windows_exclusive_slim_lock &operator=(const windows_exclusive_slim_lock &) = delete;

    void lock()
    {
        return AcquireSRWLockExclusive(&_lock);
    }

    bool try_lock()
    {
        return TryAcquireSRWLockExclusive(&_lock) != FALSE;
    }

    void unlock()
    {
        return ReleaseSRWLockExclusive(&_lock);
    }

  private:
    SRWLOCK _lock;
};

// An exclusive lock over a SlimReaderWriterLock.
class windows_exclusive_slim_lock_guard
{
  public:
    windows_exclusive_slim_lock_guard(windows_exclusive_slim_lock &p_lock) : _lock(p_lock)
    {
        _lock.lock();
    }

    // The lock is non-copyable. This also disables move constructor/operator=.
    windows_exclusive_slim_lock_guard(const windows_exclusive_slim_lock_guard &) = delete;
    windows_exclusive_slim_lock_guard &operator=(const windows_exclusive_slim_lock_guard &) = delete;

    ~windows_exclusive_slim_lock_guard()
    {
        _lock.unlock();
    }

  private:
    windows_exclusive_slim_lock &_lock;
};
} // namespace diskann
