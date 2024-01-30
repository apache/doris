---
{
    "title": "LDAP",
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# LDAP

Access to third-party LDAP services to provide authentication login and group authorization services for Doris.

LDAP authentication login complements Doris authentication login by accessing the LDAP service for password authentication; Doris uses LDAP to authenticate the user's password first; if the user does not exist in the LDAP service, it continues to use Doris to authenticate the password; if the LDAP password is correct but there is no corresponding account in Doris, a temporary user is created to log in to Doris.

LDAP group authorization, is to map the group in LDAP to the Role in Doris, if the user belongs to multiple user groups in LDAP, after logging into Doris the user will get the permission of all groups corresponding to the Role, requiring the group name to be the same as the Role name.

## Noun Interpretation

* LDAP: Lightweight directory access protocol that enables centralized management of account passwords.
* Privilege: Permissions act on nodes, databases or tables. Different permissions represent different permission to operate.
* Role: Doris can create custom named roles. A role can be thought of as a collection of permissions.

## LDAP related concepts

In LDAP, data is organized in a tree structure.

### Example (the following introduction will be expanded based on this example)
- dc=example,dc=com
- ou = ou1
  - cn = group1
  - cn = user1
- ou = ou2
  - cn = group2
    - cn = user2
- cn = user3

### Explanation of LDAP Terms
- dc(Domain Component): It can be understood as the domain name of an organization, serving as the root node of a tree
- dn(Distinguished Name): Equivalent to a unique name, for example, the dn of user1 is cn=user1,ou=ou1,dc=example,dc=com the dn of user2 is cn=user2,cn=group2,ou=ou2,dc=example,dc=com
- rdn(Relative Distinguished Name): As part of dn, the four rdns of user1 are cn=user1 ou=ou1 dc=example and dc=com
- ou(Organization Unit): It can be understood as a sub organization, where users can be placed in ou or directly in the example.com domain
- cn(common name):name
- group: Group, which can be understood as the role of Doris
- user: User, equivalent to Doris' user
- objectClass：It can be understood as the type of data in each row, such as how to distinguish whether group1 is a group or a user. Each type of data requires different attributes below, such as CN and member (user list) for group, CN, password, uid, etc. for user

## Enable LDAP Authentication
### Server-side Configuration

You need to configure the LDAP basic information in the fe/conf/ldap.conf file, and the LDAP administrator password needs to be set using sql statements.

#### Configure the fe/conf/ldap.conf file：
* ldap_authentication_enabled = false  
  Set the value to "true" to enable LDAP authentication; when the value is "false", LDAP authentication is not enabled and all other configuration items of this profile are invalid.Set the value to "true" to enable LDAP authentication; when the value is "false", LDAP authentication is not enabled and all other configuration items of this profile are invalid.

* ldap_host = 127.0.0.1  
  LDAP service ip.
  
* ldap_port = 389  
  LDAP service port, the default plaintext transfer port is 389, currently Doris' LDAP function only supports plaintext password transfer.
  
* ldap_admin_name = cn=admin,dc=domain,dc=com  
  LDAP administrator account "Distinguished Name". When a user logs into Doris using LDAP authentication, Doris will bind the administrator account to search for user information in LDAP.
  
* ldap_user_basedn = ou=people,dc=domain,dc=com
  Doris base dn when searching for user information in LDAP,For example, only user2 in the above example is allowed to log in to Doris, which is configured as ou=ou2, dc=example, dc=com. If user1, user2, and user3 in the above example are allowed to log in to Doris, which is configured as dc=example, dc=com
  
* ldap_user_filter = (&(uid={login}))

  For Doris' filtering criteria when searching for user information in LDAP, the placeholder "{login}" will be replaced with the login username. You must ensure that the user searched by this filter is unique, otherwise Doris will not be able to verify the password through LDAP and the error message "ERROR 5081 (42000): user is not unique in LDAP server." will appear when logging in.
  
  For example, if you use the LDAP user node uid attribute as the username to log into Doris, you can configure it as:    
  ldap_user_filter = (&(uid={login}))；  
  This item can be configured using the LDAP user mailbox prefix as the user name:   
  ldap_user_filter = (&(mail={login}@baidu.com))。

* ldap_group_basedn = ou=group,dc=domain,dc=com
  base dn when Doris searches for group information in LDAP. if this item is not configured, LDAP group authorization will not be enabled. Same as ldap_ User_ Similar to basedn, it limits the scope of Doris searching for groups.

#### Set the LDAP administrator password:
After configuring the ldap.conf file, start fe, log in to Doris with the root or admin account, and execute sql:  
```sql
set ldap_admin_password = password('ldap_admin_password');
```

### Client-side configuration  

#### MySQL Client
Client-side LDAP authentication requires the mysql client-side explicit authentication plugin to be enabled. Logging into Doris using the command line enables the mysql explicit authentication plugin in one of two ways.

* Set the environment variable LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN to value 1.
  For example, in a linux or max environment you can use the command:
  ```bash
  echo "export LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN=1" >> ～/.bash_profile && source ～/.bash_profile
  ```

* Add the parameter "--enable-cleartext-plugin" each time you log in to Doris.
  ```sql
  mysql -hDORIS_HOST -PDORIS_PORT -u user -p --enable-cleartext-plugin
  
  Enter ldap password
  ```
#### Jdbc Client

When using Jdbc Client to connect in to Doris, you need to customize the plugin.

First, create a class called `MysqlClearPasswordPluginWithoutSSL` that inherits from `MysqlClearPasswordPlugin`. In this class, override the `requiresConfidentiality()` method and return false.

``` java
public class MysqlClearPasswordPluginWithoutSSL extends MysqlClearPasswordPlugin {
@Override  
public boolean requiresConfidentiality() {
    return false;
  }
}
```
When obtaining a database connection, you need to configure the custom plugin into the properties

That is (xxx is the package name of the custom class)
- authenticationPlugins=xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL
- defaultAuthenticationPlugin=xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL
- disabledAuthenticationPlugins=com.mysql.jdbc.authentication.MysqlClearPasswordPlugin

eg:
```sql
 jdbcUrl = "jdbc:mysql://localhost:9030/mydatabase?authenticationPlugins=xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL&defaultAuthenticationPlugin=xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL&disabledAuthenticationPlugins=com.mysql.jdbc.authentication.MysqlClearPasswordPlugin";
```
## LDAP authentication detailed explanation
LDAP password authentication and group authorization are complementary to Doris password authentication and authorization. Enabling LDAP functionality does not completely replace Doris password authentication and authorization, but coexists with Doris password authentication and authorization.

### LDAP authentication login details
When LDAP is enabled, users have the following in Doris and LDAP:  

|LDAP User|Doris User|Password|Login Status|Login to Doris users|
|--|--|--|--|--|
|Existent|Existent|LDAP Password|Login successful|Doris User|
|Existent|Existent|Doris Password|Login failure|None|
|Non-Existent|Existent|Doris Password|Login successful|Doris User|
|Existent|Non-Existent|LDAP Password|Login successful|Ldap Temporary user|

After LDAP is enabled, when a user logs in using mysql client, Doris will first verify the user's password through the LDAP service, and if the LDAP user exists and the password is correct, Doris will use the user to log in; at this time, if the corresponding account exists, Doris will directly log in to the account, and if the corresponding account does not exist, it will create a temporary account for the user and log in to the account. The temporary account has the appropriate pair of permissions (see LDAP Group Authorization) and is only valid for the current connection. doris does not create the user and does not generate metadata for creating the user pair.  
If no login user exists in the LDAP service, Doris is used for password authentication.

The following assumes that LDAP authentication is enabled, ldap_user_filter = (&(uid={login})) is configured, and all other configuration items are correct, and the client sets the environment variable LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN=1  

For example:

#### 1:Accounts exist in both Doris and LDAP.

Doris account exists: jack@'172.10.1.10', password: 123456  
LDAP user node presence attribute: uid: jack user password: abcdef  
The jack@'172.10.1.10' account can be logged into by logging into Doris using the following command:
```bash
mysql -hDoris_HOST -PDoris_PORT -ujack -p abcdef
```

Login will fail with the following command:  
```bash
mysql -hDoris_HOST -PDoris_PORT -ujack -p 123456
```

#### 2:The user exists in LDAP and the corresponding account does not exist in Doris.

LDAP user node presence attribute: uid: jack User password: abcdef  
Use the following command to create a temporary user and log in to jack@'%', the temporary user has basic privileges DatabasePrivs: Select_priv, Doris will delete the temporary user after the user logs out and logs in:  
```bash
mysql -hDoris_HOST -PDoris_PORT -ujack -p abcdef
```

#### 3:LDAP does not exist for the user.

Doris account exists: jack@'172.10.1.10', password: 123456  
Login to the account using the Doris password, successfully:  
```bash
mysql -hDoris_HOST -PDoris_PORT -ujack -p 123456
```

### LDAP group authorization details

If a DLAP user dn is the "member" attribute of an LDAP group node, Doris assumes that the user belongs to the group. Doris will revoke the corresponding role privileges after the user logs out. Before using LDAP group authorization, you should create the corresponding role pairs in Doris and authorize the roles.

Login user Privileges are related to Doris user and group Privileges, as shown in the following table:  
|LDAP Users|Doris Users|Login User Privileges|
|--|--|--|
|exist|exist|LDAP group Privileges + Doris user Privileges|
|Does not exist|Exists|Doris user Privileges|
|exist|non-exist|LDAP group Privileges|

If the logged-in user is a temporary user and no group permission exists, the user has the select_priv permission of the information_schema by default

Example:  
LDAP user dn is the "member" attribute of the LDAP group node then the user is considered to belong to the group, Doris will intercept the first Rdn of group dn as the group name.  
For example, if user dn is "uid=jack,ou=aidp,dc=domain,dc=com", the group information is as follows:  
```text
dn: cn=doris_rd,ou=group,dc=domain,dc=com  
objectClass: groupOfNames  
member: uid=jack,ou=aidp,dc=domain,dc=com  
```
Then the group name is doris_rd.

If jack also belongs to the LDAP groups doris_qa, doris_pm; Doris exists roles: doris_rd, doris_qa, doris_pm, after logging in using LDAP authentication, the user will not only have the original permissions of the account, but will also get the roles doris_rd, doris_qa and doris _pm privileges.

>Attention:
>
>The group to which user belongs is not related to the organizational structure of the LDAP tree, and user2 in the example section may not necessarily belong to group2
> If you want user2 to belong to group2, you need to add user2 to the member attribute of group2

### LDAP information cache
To avoid frequent access to LDAP service, Doris will cache LDAP information into memory, you can specify the cache time for LDAP users through the `ldap_user_cache_timeout_s` configuration item in ldap.conf, the default is 12 hours; after modifying the information in LDAP service or modifying the After modifying the information in the LDAP service or modifying the Role permissions of the LDAP user group, the cache may not take effect in time because of the cache, so you can refresh the cache with the refresh ldap statement, see [REFRESH-LDAP](... /... /sql-manual/sql-reference/Utility-Statements/REFRESH-LDAP.md).

## Limitations of LDAP authentication

* The current LDAP feature of Doris only supports plaintext password authentication, that is, when a user logs in, the password is transmitted in plaintext between client and fe and between fe and LDAP service.

## FAQ

- How to determine which roles an LDAP user has in Doris?

  Log in to Doris using an LDAP user, ` show grants` Can view which roles the current user has. Among them, ldapDefaultRole is the default role that every ldap user has in Doris.
- How to troubleshoot when the roles of LDAP users in Doris are less than expected?

  1. Through 'show roles` Check if the expected role exists in Doris. If it does not exist, you need to use the 'CREATE ROLE role'_ Name` Create a character.
  2. Check if the expected group is in 'ldap'_ Group_ Based on the corresponding organizational structure.
  3. Check if the expected group contains the member attribute.
  4. Check if the member attribute of the expected group contains the current user.