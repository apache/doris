// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <any>
#include "tsl/robin_set.h"

namespace AnyWrapper
{

/*
 * Base Struct to hold refrence to the data.
 * Note: No memory mamagement, caller need to keep object alive.
 */
struct AnyReference
{
    template <typename Ty> AnyReference(Ty &reference) : _data(&reference)
    {
    }

    template <typename Ty> Ty &get()
    {
        auto ptr = std::any_cast<Ty *>(_data);
        return *ptr;
    }

  private:
    std::any _data;
};
struct AnyRobinSet : public AnyReference
{
    template <typename T> AnyRobinSet(const tsl::robin_set<T> &robin_set) : AnyReference(robin_set)
    {
    }
    template <typename T> AnyRobinSet(tsl::robin_set<T> &robin_set) : AnyReference(robin_set)
    {
    }
};

struct AnyVector : public AnyReference
{
    template <typename T> AnyVector(const std::vector<T> &vector) : AnyReference(vector)
    {
    }
    template <typename T> AnyVector(std::vector<T> &vector) : AnyReference(vector)
    {
    }
};
} // namespace AnyWrapper
