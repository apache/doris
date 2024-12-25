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

import org.junit.Assert;

suite("test_system_user","p0,auth") {
    test {
          sql """
              create user `root`;
          """
          exception "root"
    }
    test {
          sql """
              drop user `root`;
          """
          exception "system"
    }
    test {
          sql """
              drop user `admin`;
          """
          exception "system"
    }
    test {
          sql """
              revoke "operator" from root;
          """
          exception "Can not revoke role"
    }
    test {
          sql """
              revoke 'admin' from `admin`;
          """
          exception "Unsupported operation"
    }

    sql """
        grant select_priv on *.*.* to  `root`;
    """
    sql """
        revoke select_priv on *.*.* from  `root`;
    """
    sql """
        grant select_priv on *.*.* to  `admin`;
    """
    sql """
        revoke select_priv on *.*.* from  `admin`;
    """

}
