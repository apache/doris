---
{
    "title": "System settings",
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

# System settings

The super administrator can mainly perform the following operations under the platform module:

- Perform relevant operations on platform users
- Have the highest level of authority on the platform

User permission description

## users

### User management under local authentication

Click the Add User button to create a new user with username and email information.

 Doris Manger will assign a temporary password to the new user. The new user needs to log in with the set username/email and temporary password. After logging in, you can create a new password in "Account Settings".

![](/images/doris-manager/systemsettings-1.png)

![](/images/doris-manager/systemsettings-2.png)


### Edit User

Super administrators can manage users, including editing user information, resetting user passwords, and deactivating users.

#### Edit user information

Click to select and select "Edit" to modify the user name and email address. If the user mailbox is updated, the user needs to log in with the updated mailbox, and the password will not be updated.

![](/images/doris-manager/systemsettings-3.png)

#### reset user password

Click to select "Reset Password", and after confirming this operation, Doris Manger will reassign a temporary password for the user. The user needs to log in with the set email address and the new temporary password. After logging in, you can go to "Account Settings" Create a new password.


#### Deactivate/Activate User

Click Opt-out user, and after confirming to deactivate the user, the user's status will be changed from active to inactive. Deactivated users will not be able to log in to Doris Manger.

Click Activate User on the right side of the user to reactivate the user. The user's status will be changed back to enabled and will be able to log in to Doris Manger again.

Note that super administrators cannot deactivate their own user accounts, and there must be at least one non-deactivated super administrator user in the system.

![](/images/doris-manager/systemsettings-4.png)


## User permission description

### Super administrator privileges

|       | Create | Edit | Delete | View |
| :---- | :----- | :--- | :----- | :--- |
| User  | ✓      | ✓    | ✓      | ✓    |
| Roles | ✓      | ✓    | ✓      | ✓    |
| Space | ✓      | ✓    | ✓      | ✓    |

### Space administrator permissions

|       | Create | Edit | Delete | View |
| :---- | :----- | :--- | :----- | :--- |
| User  | X      | X    | X      | X    |
| Roles | X      | X    | X      | ✓    |
| Space | X      | ✓    | X      | ✓    |
