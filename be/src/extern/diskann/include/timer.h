// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
#pragma once

#include <chrono>

namespace diskann
{
class Timer
{
    typedef std::chrono::high_resolution_clock _clock;
    std::chrono::time_point<_clock> check_point;

  public:
    Timer() : check_point(_clock::now())
    {
    }

    void reset()
    {
        check_point = _clock::now();
    }

    long long elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(_clock::now() - check_point).count();
    }

    float elapsed_seconds() const
    {
        return (float)elapsed() / 1000000.0f;
    }

    std::string elapsed_seconds_for_step(const std::string &step) const
    {
        return std::string("Time for ") + step + std::string(": ") + std::to_string(elapsed_seconds()) +
               std::string(" seconds");
    }
};
} // namespace diskann
