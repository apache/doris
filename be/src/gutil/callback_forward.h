// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef GUTIL_CALLBACK_FORWARD_H_
#define GUTIL_CALLBACK_FORWARD_H_

namespace kudu {

template <typename Sig>
class Callback;

typedef Callback<void(void)> Closure;

}  // namespace kudu

#endif  // GUTIL_CALLBACK_FORWARD_H
