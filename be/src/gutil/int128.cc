// Copyright 2004 Google Inc.
// All Rights Reserved.
//
//

#include <iostream>
using std::cout;
using std::endl;
#include "gutil/int128.h"
#include "gutil/integral_types.h"

const uint128_pod kuint128max = {static_cast<uint64>(GG_LONGLONG(0xFFFFFFFFFFFFFFFF)),
                                 static_cast<uint64>(GG_LONGLONG(0xFFFFFFFFFFFFFFFF))};

std::ostream& operator<<(std::ostream& o, const uint128& b) {
    return (o << b.hi_ << "::" << b.lo_);
}
