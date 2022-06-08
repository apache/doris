---
{
    "title": "Initialize",
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

# Initialize

After the deployment is complete, the super administrator needs to complete the local initialization.

## Manage users

The first step of initialization is to manage users, which mainly completes the selection and configuration of authentication methods. Currently Doris Manger supports local user authentication.

![](/images/doris-manager/initializing-1.png)

### Local user authentication

Local user authentication is the user system that comes with Doris Manger. User registration can be completed by filling in the user name, email address and password. User addition, information modification, deletion and permission relationship are all completed locally.

![](/images/doris-manager/initializing-2.png)

At this point, the local initialization process has been completed. Super administrators can create spaces, space administrators can enter the space, manage the space, add and invite users to enter the space for data analysis, etc.