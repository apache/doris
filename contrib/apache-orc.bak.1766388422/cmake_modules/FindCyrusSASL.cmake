# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# - Find Cyrus SASL (sasl.h, libsasl2.so)
#
# This module defines
#  CYRUS_SASL_INCLUDE_DIR, directory containing headers
#  CYRUS_SASL_SHARED_LIB, path to Cyrus SASL's shared library
#  CYRUS_SASL_FOUND, whether Cyrus SASL and its plugins have been found
#
# N.B: we do _not_ include sasl in thirdparty, for a fairly subtle reason. The
# TLDR version is that newer versions of cyrus-sasl (>=2.1.26) have a bug fix
# for https://bugzilla.cyrusimap.org/show_bug.cgi?id=3590, but that bug fix
# relied on a change both on the plugin side and on the library side. If you
# then try to run the new version of sasl (e.g from our thirdparty tree) with
# an older version of a plugin (eg from RHEL6 install), you'll get a SASL_NOMECH
# error due to this bug.
#
# In practice, Cyrus-SASL is so commonly used and generally non-ABI-breaking that
# we should be OK to depend on the host installation.

# Note that this is modified from the version that was copied from our
# friends at the Kudu project.  The original version implicitly required
# the Cyrus SASL.  This version will only complain if REQUIRED is added.


find_path(CYRUS_SASL_INCLUDE_DIR sasl/sasl.h)
find_library(CYRUS_SASL_SHARED_LIB sasl2)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CyrusSASL DEFAULT_MSG
  CYRUS_SASL_SHARED_LIB CYRUS_SASL_INCLUDE_DIR)

MARK_AS_ADVANCED(CYRUS_SASL_INCLUDE_DIR CYRUS_SASL_SHARED_LIB)
