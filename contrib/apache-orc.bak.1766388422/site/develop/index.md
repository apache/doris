---
layout: page
title: Developing
---

Information about the ORC project that is most important for
developers working on the project.

The [ORC format specification](/specification) defines the format
to promote compatibility between implementations.

## Development community

We have committers from many different companies. The full
list of [ORC committers](committers) is available.

## Mailing Lists

The most important communication mechanism for the project are its
mailing lists.  The mailing lists have the advantage that they are
publicly archived and work well asynchronously across timezones.

Beside the user mailing list, there are several development mailing
lists for ORC:

* [dev@orc.apache.org](mailto:dev@orc.apache.org) - Development discussions
  with archive [here](https://mail-archives.apache.org/mod_mbox/orc-dev/)
* [issues@orc.apache.org](mailto:issues@orc.apache.org) - Bug tracking
  with archive [here](https://mail-archives.apache.org/mod_mbox/orc-issues/)
* [commits@orc.apache.org](mailto:commits@orc.apache.org) - Git tracking
  with archive [here](https://mail-archives.apache.org/mod_mbox/orc-commits/)

You can subscribe to the lists by sending email to
*list*-subscribe@orc.apache.org and unsubscribe by sending email to
*list*-unsubscribe@orc.apache.org.

## Bug reports

Each code change requires a [jira]({{ site.jira }}/ORC) to track the
discussion of the change.

## Design
Some code changes provide [design/additional documentation](design).

## Source code

ORC uses git for version control. Get the source code and configure it
to fetch the pull requests also:

~~~~
% git clone -o apache git@github.com:apache/orc.git
% cd orc
% git config --add remote.apache.fetch '+refs/pull/*/head:refs/remotes/apache/pr/*'
~~~~

Pull requests will be named "apache/pr/999" for pull request 999.

If you are a committer, you need to register your Github id with Apache:

* Visit [id.apache.org](https://id.apache.org) and add your Github id.
* Visit [Apache gitbox](https://gitbox.apache.org/setup/) and get authorization for the projects that use Apache gitbox, which includes ORC.

Once this is done, your Github account will be placed in the ORC github team,
so you'll be able to close PRs, etc.

The important branches are:

* [main](https://github.com/apache/orc/tree/main) -
  The main branch for all development
* branch-X.Y - The release branches
* [asf-site](https://github.com/apache/orc/tree/asf-site) -
  The generated html pages that are deployed as https://orc.apache.org/

Releases are tagged as "rel/release-X.Y.Z". Apache's git repository
guarantees that tags in the "rel/*" namespace are never deleted or
changed.

Please check our [coding guidelines](coding).

## Website shortcuts

We've added several shortcuts to various relevant pages:

From our website, you can use:

* [/bugs](/bugs) to jump to our bug database
* [/downloads](/downloads) to jump to our downloads page
* [/releases](/releases) to jump to our releases page
* [/src](/src) to jump to our source code
* [/web-src](/web-src) to jump to our site source code

## Reviews

ORC uses Commit-Then-Review, so patches can be committed without a
committer's review. However, most changes should be reviewed first.

## Tests

In principle, new pull requests had better provide test coverage for newly proposed code path.
In addition, newly added test cases are supposed to fail without the proposed main code.
In other words, reviewers need to check these two things during the review manually.
The following is useful during Java code review and tests.

### Running Individual Java Test File

~~~~
% cd java
% ./mvnw test -pl core -Dtest=TestRecordReaderImpl
~~~~

### Running Multiple Java Test Files

~~~~
% cd java
% ./mvnw test -pl core -Dtest='Test*Reader*'
~~~~

### Running Individual Java Test Case
~~~~
% cd java
$ ./mvnw test -pl core -Dtest=TestRecordReaderImpl#testFindColumn
~~~~

### Running Multiple Java Test Cases
~~~~
% cd java
% ./mvnw test -pl core -Dtest='TestRecordReaderImpl#testFind*'
~~~~

## Approving a pull request

Fetch the current state of the project:

~~~~
% git fetch apache
~~~~

Switch to the branch:

~~~~
% git checkout apache/pr/999
~~~~

You'll want to rebase it and make it a single commit by squashing
the commits into a single commit.

~~~~
% git rebase -i apache/main
~~~~

Update the commit message to sign it using your GPG key and close the
pull request:

~~~~
% git commit --amend -s -S
~~~~

Ensure the first line of the commit starts with the jira number
(eg. ORC-123) and includes a description of what was changed. Also add
a line such as "Fixes #999", which asks the Apache infrastructure to
close pull request 999. If you wish you close a pull request without
claiming to have fixed the problem, the form "Closes #999" also works.

Finally, push the result to Apache:

~~~~
% git push apache HEAD:main
~~~~

## Creating a GPG key

When you become a committer, you should create a 4096 bit GPG key.

~~~~
% gpg --full-gen-key
~~~~

Use 4096 bits and your Apache email address. Once it is created,
you'll need to get your key fingerprint. Avoid using the short
fingerprint (eg. 3D0C92B9), because it is possible to generate fake
keys that have the same short fingerprint as the real key.

~~~~
% gpg --list-secret-keys --keyid-format LONG
~~~~

Your key fingerprint is the string after "rsa4096/". Example output
for the key with fingerprint 1209E7F13D0C92B9 looks like:

~~~~
/Users/owen/.gnupg/pubring.gpg
------------------------------
sec   rsa4096/1209E7F13D0C92B9 2010-02-23 [SC]
      47660BC98BC433F01E5C90581209E7F13D0C92B9
uid                 [ultimate] Owen O'Malley <omalley@apache.org>
~~~~

Now publish your public key to one of the public keyservers. I usually use
hkp://pgp.mit.edu, although any of them will work.

~~~~
gpg --send-key <your key fingerprint>
~~~~

Next, you need to update the [Apache account
database](https://id.apache.org) with your new key. Login to add your
new key's fingerprint and your github id. It is also good to update
your github profile with your GPG public key as well at
[https://github.com/settings/keys](https://github.com/settings/keys).

After you've created your key, it is good to get someone in the ORC
community to sign it for you. Contact someone directly or send email
to dev@orc.apache.org asking for someone to sign it.

## Making a Release

The release process for ORC is driven by a Release Manager. They should
discuss their intention to start the process on the dev list and then
follow the steps of [how to release ORC](make-release).

## Dist Directory

Apache expects the projects to manage their current release artifact
distribution using subversion. It should be limited to the latest
release in each of the active release branches.

The ORC dist directory is managed via svn in
[https://dist.apache.org/repos/dist/release/orc](https://dist.apache.org/repos/dist/release/orc).
The release artifacts are pushed to many mirrors. Files in the dist
directory are available forever via the [Apache dist
archive](https://archive.apache.org/dist/orc/).

## Bylaws

ORC has a set of [bylaws](bylaws) that describe the rules for the different
votes within our project.
