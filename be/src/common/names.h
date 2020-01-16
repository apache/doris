// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <boost/version.hpp>

#ifdef _GLIBCXX_VECTOR
using std::vector;
#endif

#ifdef _GLIBCXX_MAP
using std::map;
using std::multimap;
#endif

#ifdef _GLIBCXX_LIST
using std::list;
#endif

#ifdef _GLIBCXX_SET
using std::set;
using std::multiset;
#endif

#ifdef _GLIBCXX_STACK
using std::stack;
#endif

#ifdef _GLIBCXX_QUEUE
using std::queue;
#endif

#ifdef _GLIBCXX_DEQUE
using std::deque;
#endif

#ifdef _GLIBCXX_STRING
using std::string;
#endif

#ifdef _GLIBCXX_IOSTREAM
using std::cout;
using std::cin;
using std::cerr;
#endif

#ifdef _GLIBCXX_OSTREAM
using std::ostream;
using std::endl;
#endif

#ifdef _GLIBCXX_IOS
using std::fixed;
using std::hex;
using std::oct;
using std::dec;
using std::left;
using std::ios;
#endif

#ifdef _GLIBCXX_IOMANIP
using std::setprecision;
using std::setfill;
using std::setw;
#endif

#ifdef _GLIBCXX_FSTREAM
using std::fstream;
using std::ifstream;
using std::ofstream;
#endif

#ifdef _GLIBCXX_SSTREAM
using std::stringstream;
using std::istringstream;
using std::ostringstream;
#endif

#ifdef _GLIBCXX_ALGORITHM
using std::swap;
using std::min;
using std::max;
using std::sort;
#endif

#ifdef _GLIBCXX_MEMORY
using std::make_shared;
using std::shared_ptr;
using std::unique_ptr;
#endif

#ifdef _GLIBCXX_UTILITY
using std::move;
#endif

#ifdef _NEW
using std::nothrow;
#endif

#ifdef BOOST_THREAD_THREAD_COMMON_HPP
using boost::thread;
#endif

#ifdef BOOST_THREAD_DETAIL_THREAD_GROUP_HPP
using boost::thread_group;
#endif

#ifdef BOOST_THREAD_MUTEX_HPP
using boost::mutex;
using boost::try_mutex;
#endif

#ifdef BOOST_LEXICAL_CAST_INCLUDED
using boost::lexical_cast;
#endif

#ifdef BOOST_THREAD_PTHREAD_SHARED_MUTEX_HPP
using boost::shared_mutex;
#endif

/// In older versions of boost, when including mutex.hpp, it would include locks.hpp that
/// would in turn provide lock_guard<>. In more recent versions, including mutex.hpp would
/// include lock_types.hpp that does not provide lock_guard<>. This check verifies if boost
/// locks have been included and makes sure to only include lock_guard if the provided lock
/// implementations were not included using lock_types.hpp (for older boost versions) or if
/// lock_guard.hpp was explicitly included.
#if (defined(BOOST_THREAD_LOCKS_HPP) && BOOST_VERSION < 105300) || \
        defined(BOOST_THREAD_LOCK_GUARD_HPP)
using boost::lock_guard;
#endif

#if defined(BOOST_THREAD_LOCKS_HPP) || defined(BOOST_THREAD_LOCK_TYPES_HPP)
using boost::unique_lock;
using boost::shared_lock;
using boost::upgrade_lock;
#endif

#ifdef BOOST_SMART_PTR_SCOPED_PTR_HPP_INCLUDED
using boost::scoped_ptr;
#endif

#ifdef BOOST_UNORDERED_MAP_HPP_INCLUDED
using boost::unordered_map;
#endif

#ifdef BOOST_UNORDERED_SET_HPP_INCLUDED
using boost::unordered_set;
#endif

#ifdef BOOST_FUNCTION_PROLOGUE_HPP
using boost::function;
#endif

#ifdef BOOST_BIND_HPP_INCLUDED
using boost::bind;
using boost::mem_fn;
#endif

#ifdef STRINGS_SUBSTITUTE_H_
using strings::Substitute;
#endif
