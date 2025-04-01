// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <stdexcept>

namespace diskann
{

class NotImplementedException : public std::logic_error
{
  public:
    NotImplementedException() : std::logic_error("Function not yet implemented.")
    {
    }
};
} // namespace diskann
