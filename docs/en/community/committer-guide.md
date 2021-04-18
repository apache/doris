---
{
    "title": "Committer Guide",
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

# Committer Guide

## New Committer Guidelines

### Subscribe to the public mailing lists

If you haven’t yet, subscribe to {dev,commits}@doris.apache.org mailing lists.
Commits mailing list is especially important because all of Github Issue, Pull Request and build notifications are sent there.

### Subscribe to the private mailing list

Subscribe to private@doris.apache.org by sending an email to private-subscribe@doris.apache.org.
Keep in mind that this list is private and your subscription needs to be approved by a moderator.
If you are PPMC, you can subscribe the mailing list through [Subscription Helper](https://whimsy.apache.org/committers/subscribe)

### Link your ASF and Github account

We use Github for managing issues and user contributions (pull requests).
As such, you need to link your Github.com account with your ASF account using [Gitbox](https://gitbox.apache.org/setup/).
This way you will get write access to [Doris](https://github.com/apache/incubator-doris) repository
and you will be able to manage issues and pull request directly through our Github repository.

## Code Review Guidelines

1. Always maintain a high standard of review so that the quality of the entire product can be better guaranteed.

2. Changes to the architecture or the user interface need to be fully discussed by the community. This can be either in a mail group or in a GitHub issue. 

3. Test coverage. The added logic needs to be covered by a corresponding test. When the old code for which there is no test is changed, this requirement can be appropriately relaxed.

4. Documentation. Newly added features must be documented, otherwise such code is not allowed.

5. Readability of the code. If reviewers are not very clear about the logic of the code, then you can ask the contributor to explain the logic. And writing sufficient comments in the code to explain the logic is encouraged.

6. Try to give a clear conclusion at the end of your comments, "approve" or "change request". If it's a minor issue, you can just leave a comment.

7. Recommend to submit a "+1 Comment" rather than a "+1 Approve" to indicate that it looks good to me but I am not sure whether this part of the function is correct. It needs someone else's approve.

8. Respect each other and learn from each other. Maintain a polite tone when commenting, try to give reasons for the suggestions.

## Pull Request Guidelines

1. During a PR, there are three types of roles. Contributor: the PR submitter; Reviewer: the person who needs to make code-level review on the PR; and Moderator: the person responsible for coordinating the entire PR process. For example, the moderator should assign reviewers, push the author to make changes according to the comments, set different tags for the PR, and merge the PR etc. For a specific PR, a person may act in different roles, such as when a committer submitted one PR, he may himself be both the contributor and the moderator of that PR.

2. Committers can assign a PR to itself as a moderator. After assigning it to themselves, the other committer will know that this PR has been in charge.

3. The committer is encouraged to act as a moderator for its own PR.

4. The reviewer needs to perform a code-level review, refer Code Review Guidelines

5. Once a reviewer has commented on a PR, they need to keep following up on subsequent changes to that PR.

6. A PR must get at least a +1 approved from committer who is not the author.

7. After the first +1 to the PR, wait at least one working day before merging. The main purpose is to wait for the rest of the community to come to review.

8. For architecture or user interface changes, a PR needs to get at least 3 +1's to merge.

9. Regression cases should pass before merging.

10. Moderator needs to make sure all comments are resolved before merging.

11. Select "squash and merge" to merge.

12. When there is a disagreement about a modification, try to discuss the resolution. If the discussion doesn't work out, it can be resolved by a vote in private@doris.apache.org by the majority rules.

