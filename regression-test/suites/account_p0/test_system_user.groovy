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

     sql """
          create user `root`@'8.8.8.8';
      """
     sql """
         grant select_priv on *.*.* to  `root`@'8.8.8.8';
     """
     sql """
         revoke select_priv on *.*.* from  `root`@'8.8.8.8';
     """
     test {
               sql """
                   grant 'operator' to `root`@'8.8.8.8';
               """
               exception "Can not grant role: operator"
         }
    sql """
            drop user `root`@'8.8.8.8';
        """

    sql """
          create user `admin`@'8.8.8.8';
      """
     sql """
         grant select_priv on *.*.* to  `admin`@'8.8.8.8';
     """
     sql """
         revoke select_priv on *.*.* from  `admin`@'8.8.8.8';
     """

   sql """
       grant 'admin' to `admin`@'8.8.8.8';
   """
    sql """
           revoke 'admin' from `admin`@'8.8.8.8';
       """
    sql """
            drop user `admin`@'8.8.8.8';
        """

    String grantUser = "test_system_user_grant"
    String pwd = 'C123_567p'
    try_sql("DROP USER 'root'@'8.8.20.39'")
    try_sql("DROP USER '${grantUser}'")
    sql """CREATE USER 'root'@'8.8.20.39' IDENTIFIED BY 'Pwd_a1'"""
    sql """SET PASSWORD FOR 'root'@'8.8.20.39' = PASSWORD('Pwd_b2')"""
    test {
        sql """ALTER USER 'root'@'8.8.20.39' ACCOUNT_LOCK"""
        exception "Not support lock account now"
    }
    sql """CREATE USER '${grantUser}' IDENTIFIED BY '${pwd}'"""
    sql """GRANT GRANT_PRIV ON *.*.* TO ${grantUser}"""
    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${grantUser}"""
    }
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"
    connect(grantUser, "${pwd}", url) {
        sql """SET PASSWORD FOR 'root'@'8.8.20.39' = PASSWORD('Pwd_c3')"""
        sql """ALTER USER 'root'@'8.8.20.39' IDENTIFIED BY 'Pwd_d4'"""
        test {
            sql """SET PASSWORD FOR 'root'@'%' = PASSWORD('Pwd_e5')"""
            exception "Can not set password for root user"
        }
        test {
            sql """ALTER USER 'root'@'%' IDENTIFIED BY 'Pwd_e5'"""
            exception "Only root user can modify root user"
        }
    }
}
