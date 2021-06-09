// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gutil/ref_counted.h"

#include <common/logging.h>

#include "gutil/atomic_refcount.h"

namespace doris {

namespace subtle {

RefCountedBase::RefCountedBase()
        : ref_count_(0)
#ifndef NDEBUG
          ,
          in_dtor_(false)
#endif
{
}

RefCountedBase::~RefCountedBase() {
#ifndef NDEBUG
    DCHECK(in_dtor_) << "RefCounted object deleted without calling Release()";
#endif
}

void RefCountedBase::AddRef() const {
    // TODO(maruel): Add back once it doesn't assert 500 times/sec.
    // Current thread books the critical section "AddRelease" without release it.
    // DFAKE_SCOPED_LOCK_THREAD_LOCKED(add_release_);
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    ++ref_count_;
}

bool RefCountedBase::Release() const {
    // TODO(maruel): Add back once it doesn't assert 500 times/sec.
    // Current thread books the critical section "AddRelease" without release it.
    // DFAKE_SCOPED_LOCK_THREAD_LOCKED(add_release_);
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    if (--ref_count_ == 0) {
#ifndef NDEBUG
        in_dtor_ = true;
#endif
        return true;
    }
    return false;
}

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
