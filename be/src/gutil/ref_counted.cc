// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gutil/ref_counted.h"

#include "gutil/atomic_refcount.h"

namespace doris {

namespace subtle {

bool RefCountedThreadSafeBase::HasOneRef() const {
    return base::RefCountIsOne(&const_cast<RefCountedThreadSafeBase*>(this)->ref_count_);
}

RefCountedThreadSafeBase::RefCountedThreadSafeBase() : ref_count_(0) {
#ifndef NDEBUG
    in_dtor_ = false;
#endif
}

RefCountedThreadSafeBase::~RefCountedThreadSafeBase() {
#ifndef NDEBUG
    DCHECK(in_dtor_) << "RefCountedThreadSafe object deleted without "
                        "calling Release()";
#endif
}

void RefCountedThreadSafeBase::AddRef() const {
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    base::RefCountInc(&ref_count_);
}

bool RefCountedThreadSafeBase::Release() const {
#ifndef NDEBUG
    DCHECK(!in_dtor_);
    DCHECK(!base::RefCountIsZero(&ref_count_));
#endif
    if (!base::RefCountDec(&ref_count_)) {
#ifndef NDEBUG
        in_dtor_ = true;
#endif
        return true;
    }
    return false;
}

} // namespace subtle

} // namespace doris
