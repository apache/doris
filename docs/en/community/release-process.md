---
{
    "title": "Publish of Apache Doris",
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

# Publish of Apache Doris

Apache publishing must be at least an IPMC member, a committer with Apache mailboxes, a role called release manager.

The general process of publication is as follows:

1. Preparing your setup
2. Preparing for release candidates
	1. launching DISCUSS in the community
	2. cutting a release branch
	3. clean up issues
	4. merging necessary patch to release branch
3. Verify branch
	1. QA stability test
	2. Verify the correctness of the compiled image
	3. Prepare Release Nodes
4. Running the voting process for a release
	1. singing a tag and upload it to [Apache dev svn repo](https://dist.apache.org/repos/dist/dev/incubator/doris)
	2. calling votes from [Doris community](dev@doris.apache.org)
	3. send result email to [Doris community](dev@doris.apache.org)
	4. calling votes from [Incubator community](general@incubator.apache.org)
	5. send result email to general@incubator.apache.org
5. Finalizing and posting a release
	1. Upload the signature package to [Apache release repo](https://dist.apache.org/repos/dist/release/incubator/doris) and generate relevant links
	2. Publish download links on Doris website and GitHub
	3. Send Announce mail to general@incubator.apache.org



## prepare setup

If you are a new Release Manager, you can read up on the process from the followings:

1. release signing https://www.apache.org/dev/release-signing.html
2. gpg for signing https://www.apache.org/dev/openpgp.html
3. svn https://www.apache.org/dev/version-control.html#https-svn

### preparing gpg key

Release manager needs Mr. A to sign his own public key before publishing and upload it to the public key 
server. Then he can use this public key to sign the package ready for publication.
If your key already exists in [key] (https://dist.apache.org/repos/dist/dev/initiator/doris/keys), you can skip this step.


#### Installation and configuration of signature software GnuPG
##### GnuPG

In 1991, programmer Phil Zimmermann developed the encryption software PGP to avoid government surveillance. This software is very useful, spread quickly, has become a necessary tool for many programmers. However, it is commercial software and cannot be used freely. So the Free Software Foundation decided to develop a replacement for PGP, called GnuPG. This is the origin of GPG.

##### Installation Configuration

CentOS installation command:

```
yum install gnupg
```
After installation, the default configuration file gpg.conf will be placed in the home directory.

```
~/.gnupg /gpg.conf
```

If this directory or file does not exist, you can create an empty file directly.
Edit gpg.conf, modify or add KeyServer configuration:

```
keyserver hkp http://keys.gnupg.net
```

Apache signature recommends SHA512, which can be done by configuring gpg.
Edit gpg.conf and add the following three lines:

```
personal-digest-preferences SHA512
cert -digest -something SHA512
default-preference-list SHA512 SHA384 SHA256 SHA224 AES256 AES192 AES CAST5 ZLIB BZIP2 ZIP Uncompressed
```

#### Generating new signatures

##### Prepare to Sign

Recommended settings for generating new signatures:

We must log in to user account directly through SecureCRT and other terminals. We can't transfer it through Su - user or ssh. Otherwise, the password input box will not show up and make an error.

Let's first look at the version of GPG and whether it supports SHA512.

```
$ gpg --version
gpg (GnuPG) 2.0.22
libgcrypt 1.5.3
Copyright (C) 2013 Free Software Foundation, Inc.
License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Home: ~/.gnupg
Supported algorithms:
Pubkey: RSA, ?, ?, ELG, DSA
Cipher: IDEA, 3DES, CAST5, BLOWFISH, AES, AES192, AES256, TWOFISH,
        CAMELLIA128, CAMELLIA192, CAMELLIA256
Hash: MD5, SHA1, RIPEMD160, SHA256, SHA384, SHA512, SHA224
Compression: Uncompressed, ZIP, ZLIB, BZIP2
```

##### Generating new signatures

```
$ gpg --gen-key
gpg (GnuPG) 2.0.22; Copyright (C) 2013 Free Software Foundation, Inc.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Please select what kind of key you want:
   (1) RSA and RSA (default)
   (2) DSA and Elgamal
   (3) DSA (sign only)
   (4) RSA (sign only)
Your selection? 1
RSA keys may be between 1024 and 4096 bits long.
What keysize do you want? (2048) 4096
Requested keysize is 4096 bits
Please specify how long the key should be valid.
         0 = key does not expire
      <n>  = key expires in n days
      <n>w = key expires in n weeks
      <n>m = key expires in n months
      <n>y = key expires in n years
Key is valid for? (0)
Key does not expire at all
Is this correct? (y/N) y

GnuPG needs to construct a user ID to identify your key.

Real name: xxx
Name must be at least 5 characters long
Real name: xxx-yyy
Email address: xxx@apache.org
Comment: xxx's key
You selected this USER-ID:
    "xxx-yyy (xxx's key) <xxx@apache.org>"

Change (N)ame, (C)omment, (E)mail or (O)kay/(Q)uit? o
```

Real name needs to be consistent with the ID shown in ID. apache. org.
Email address is apache's mailbox.

Enter passphrase, you need to enter it twice. The passphrase must more than 8 characters.

**The passphrase here must be remembered, it will be used when signing later**

##### View and Output

The first line displays the public key file name (pubring.gpg), the second line displays the characteristics of the public key (4096 bits, Hash string and the generation time), the third line displays "User ID", comments, email, and the fourth line displays Private key characteristics.

```
$ gpg --list-keys
/home/lide/.gnupg/pubring.gpg
-----------------------------
pub   4096R/33DBF2E0 2018-12-06
uid                  xxx-yyy  (xxx's key) <xxx@apache.org>
sub   4096R/0E8182E6 2018-12-06
```

xxx-yy is the user ID.

```
gpg --armor --output public-key.txt --export [UserID]
```

```
$ gpg --armor --output public-key.txt --export xxx-yyy
文件‘public-key.txt’已存在。 是否覆盖？(y/N)y
$ cat public-key.txt
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG V2.0.22 (GNU /Linux)

mQINBFwJEQ0BEACwqLluHfjBqD/RWZ4uoYxNYHlIzZvbvxAlwS2mn53BirLIU/G3
9opMWNplvmK+3+gNlRlFpiZ7EvHsF/YJOAP59HmI2Z...
```

#### Upload signature public key

Public key servers are servers that store users'public keys exclusively on the network. The send-keys parameter uploads the public key to the server.

```
gpg --send-keys xxxx
```

Where XXX is the last step -- the string after pub in the list-keys result, as shown above: 33DBF2E0

You can also upload the contents of the above public-key.txt through the following website:

```
http://keys.gnupg.net
```

After successful upload, you can query the website and enter 0x33DBF2E0:

http://keys.gnupg.net

Queries on the site are delayed and may take an hour.


#### Generate fingerprint and upload it to Apache user information

Because the public key server has no checking mechanism, anyone can upload the public key in your name, so there is no way to guarantee the reliability of the public key on the server. Usually, you can publish a public key fingerprint on the website and let others check whether the downloaded public key is true or not.

Fingerprint parameter generates public key fingerprints:

```
gpg --fingerprint [UserID]
```

```
$ gpg --fingerprint xxx-yyy
pub   4096R/33DBF2E0 2018-12-06
      Key fingerprint = 07AA E690 B01D 1A4B 469B  0BEF 5E29 CE39 33DB F2E0
uid                  xxx-yyy (xxx's key) <xxx@apache.org>
sub   4096R/0E8182E6 2018-12-06
```

Paste the fingerprint above (i.e. 07AA E690 B01D 1A4B 469B 0BEF 5E29 CE39 33DB F2E0) into your user information:

https://id.apache.org
OpenPGP Public Key Primary Fingerprint:

#### Generating keys

```
svn co //dist.apache.org/repos/dist/dev/incubator/doris/
# edit doris/KEY file
gpg --list-sigs [用户 ID] >> doris/KEYS
gpg --armor --export [用户 ID] >> doris/KEYS
svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Update KEYS"
```

## Prepare for release

### Launching DISCUSS in the Community

If you think you've fixed a lot of bugs and developed more important features, any IPMC member can initiate DISCUSS discussions to release a new version.
An e-mail entitled [DISCUSS] x.y.z release can be launched to discuss within the community what bugs have been fixed and what features have been developed.
If DISCUSS mail is supported, we can proceed to the next step.

### Preparatory Branch

Before publishing, we need to build a new branch, For example:

```
$ git checkout -b branch-0.9

```

This branch needs to be fully tested to make functions available, bug convergence, and important bugs fixed.

This process needs to wait for the community to see if a necessary patch needs to be merged in this version, and if so, it needs to be cherry picked to the release branch.

### clean up issue

Go through all the issues belonging to this version, close those that have been completed, and if they cannot be completed, postpone them to a later version.

### Merge necessary patches

During the release waiting process, there may be more important patch merging. If someone in the community says that there is an important bug to merge, then release manager needs to evaluate and merge the important patches into the release branch.

## Verify branch

### QA stability test

Give the prepared branch to QA students for stability testing. If there is a problem that needs to be fixed during the testing process, if there is a problem that needs to be fixed during the testing process, after the problem is fixed, the PR that fixes the problem needs to be merged into the branch of the release version.

After the entire branch is stable, the release can be prepared.

### Verify the correctness of the compiled image

1. Download the compiled image

         ```
         docker pull apachedoris/doris-dev:build-env-1.2
         ```

2. Use official documents to compile the new branch, see [Docker Development Mirror Compilation](http://doris.apache.org/master/zh-CN/installing/compilation.html)

         After entering the mirror, the compilation may take about 3~4 hours, please be patient.

         If the compilation fails due to the lack of some third-party libraries, the compilation image needs to be updated.

3. Rebuild the image

## Running the voting process for a release

### dozen Tags

When the above branches are stable, tags can be made on them.
Remember to modify the `build_version` variable in `gensrc/script/gen_build_version.sh` when creating tags. For example, `build_version='0.10.0-release'.`

For example:

```
$ git checkout branch-0.9
$ git tag -a 0.9.0-rc01 -m "0.9.0 release candidate 01"
$ git push origin 0.9.0-rc01
Counting objects: 1, done.
Writing objects: 100% (1/1), 165 bytes | 0 bytes/s, done.
Total 1 (delta 0), reused 0 (delta 0)
To git@github.com:apache/incubator-doris.git
 * [new tag]         0.9.0-rc01 -> 0.9.0-rc01

$ git tag
```

### Packing Signature

The following steps also need to log into user accounts directly through terminals such as SecureCRT, and can not be transferred through Su - user or ssh, otherwise the password input box will not show and error will be reported.

```
$ git checkout 0.9.0-rc01

$ git archive --format=tar 0.9.0-rc01 --prefix=apache-doris-0.9.0-incubating-src/ | gzip > apache-doris-0.9.0-incubating-src.tar.gz

$ gpg -u xxx@apache.org --armor --output apache-doris-0.9.0-incubating-src.tar.gz.asc --detach-sign apache-doris-0.9.0-incubating-src.tar.gz

$ gpg --verify apache-doris-0.9.0-incubating-src.tar.gz.asc apache-doris-0.9.0-incubating-src.tar.gz

$ sha512sum apache-doris-0.9.0-incubating-src.tar.gz > apache-doris-0.9.0-incubating-src.tar.gz.sha512

$ sha512sum --check apache-doris-0.9.0-incubating-src.tar.gz.sha512
```

### Upload signature packages and KEYS files to DEV SVN

First, download the SVN library:

```
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
```

Organize all previous files into the following SVN paths

```
./doris/
|-- 0.11.0-rc1
|   |-- apache-doris-0.11.0-incubating-src.tar.gz
|   |-- apache-doris-0.11.0-incubating-src.tar.gz.asc
|   `-- apache-doris-0.11.0-incubating-src.tar.gz.sha512
`-- KEYS
```

Upload these files

```
svn add 0.9.0-rc1
svn commit -m "Release Apache Doris (incubating) 0.9.0 rc1"
```

### Send e-mail to community dev@doris.apache.org about voting

[VOTE] Release Apache Doris 0.9.0-incubating-rc01


```
Hi all,

Please review and vote on Apache Doris 0.9.0-incubating-rc01 release.

The release candidate has been tagged in GitHub as 0.9.0-rc01, available
here:
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01

===== CHANGE LOG =====

New Features:
....

======================

Thanks to everyone who has contributed to this release.

The artifacts (source, signature and checksum) corresponding to this release
candidate can be found here:
https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc1/

This has been signed with PGP key 33DBF2E0, corresponding to
lide@apache.org.
KEYS file is available here:
https://dist.apache.org/repos/dist/dev/incubator/doris/KEYS
It is also listed here:
https://people.apache.org/keys/committer/lide.asc

To verify and build, you can refer to following wiki:
https://github.com/apache/incubator-doris/wiki/How-to-verify-Apache-Release
https://wiki.apache.org/incubator/IncubatorReleaseChecklist

The vote will be open for at least 72 hours.
[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...

Best Regards,
xxx

----
DISCLAIMER-WIP: 
Apache Doris is an effort undergoing incubation at The Apache Software Foundation (ASF), 
sponsored by the Apache Incubator. Incubation is required of all newly accepted projects 
until a further review indicates that the infrastructure, communications, and decision 
making process have stabilized in a manner consistent with other successful ASF projects. 
While incubation status is not necessarily a reflection of the completeness or stability 
of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

Some of the incubating project’s releases may not be fully compliant with ASF policy. For 
example, releases may have incomplete or un-reviewed licensing conditions. What follows is 
a list of known issues the project is currently aware of (note that this list, by definition, 
is likely to be incomplete): 

 * Releases may have incomplete licensing conditions

If you are planning to incorporate this work into your product/project, please be aware that
you will need to conduct a thorough licensing review to determine the overall implications of 
including this work. For the current status of this project through the Apache Incubator 
visit: https://incubator.apache.org/projects/doris.html
```

### Email Result after the vote is passed

[Result][VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Thanks to everyone, and this vote is now closed.

It has passed with 4 +1 (binding) votes and no 0 or -1 votes.

Binding:
Zhao Chun
+1 xxx
+ 1 Li Chaoyong
+1 Mingyu Chen

Best Regards,
xxx

```

### Send an e-mail to general@incubator.apache.org for a vote.

[VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Hi all,

Please review and vote on Apache Doris 0.9.0-incubating-rc01 release.

Apache Doris is an MPP-based interactive SQL data warehousing for reporting and analysis.

The Apache Doris community has voted on and approved this release:
https://lists.apache.org/thread.html/d70f7c8a8ae448bf6680a15914646005c6483564464cfa15f4ddc2fc@%3Cdev.doris.apache.org%3E

The vote result email thread:
https://lists.apache.org/thread.html/64d229f0ba15d66adc83306bc8d7b7ccd5910ecb7e842718ce6a61da@%3Cdev.doris.apache.org%3E

The release candidate has been tagged in GitHub as 0.9.0-rc01, available here:
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01

There is no CHANGE LOG file because this is the first release of Apache Doris.
Thanks to everyone who has contributed to this release, and there is a simple release notes can be found here:
https://github.com/apache/incubator-doris/issues/406

The artifacts (source, signature and checksum) corresponding to this release candidate can be found here:
https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc01/

This has been signed with PGP key 33DBF2E0, corresponding to lide@apache.org.
KEYS file is available here:
https://dist.apache.org/repos/dist/dev/incubator/doris/KEYS
It is also listed here:
https://people.apache.org/keys/committer/lide.asc

The vote will be open for at least 72 hours.
[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...

To verify and build, you can refer to following instruction:

Firstly, you must be install and start docker service, and then you could build Doris as following steps:

Step1: Pull the docker image with Doris building environment
$ docker pull apachedoris/doris-dev:build-env
You can check it by listing images, its size is about 3.28GB.

Step2: Run the Docker image
You can run image directly:
$ docker run -it apachedoris/doris-dev:build-env

Step3: Download Doris source
Now you should in docker environment, and you can download Doris source package.
(If you have downloaded source and it is not in image, you can map its path to image in Step2.)
$ wget https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc01/apache-doris-0.9.0.rc01-incubating-src.tar.gz

Step4: Build Doris
Now you can decompress and enter Doris source path and build Doris.
$ tar zxvf apache-doris-0.9.0.rc01-incubating-src.tar.gz
$ cd apache-doris-0.9.0.rc01-incubating-src
$ sh build.sh

Best Regards,
xxx

----
DISCLAIMER-WIP: 
Apache Doris is an effort undergoing incubation at The Apache Software Foundation (ASF), 
sponsored by the Apache Incubator. Incubation is required of all newly accepted projects 
until a further review indicates that the infrastructure, communications, and decision 
making process have stabilized in a manner consistent with other successful ASF projects. 
While incubation status is not necessarily a reflection of the completeness or stability 
of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

Some of the incubating project’s releases may not be fully compliant with ASF policy. For 
example, releases may have incomplete or un-reviewed licensing conditions. What follows is 
a list of known issues the project is currently aware of (note that this list, by definition, 
is likely to be incomplete): 

 * Releases may have incomplete licensing conditions

If you are planning to incorporate this work into your product/project, please be aware that
you will need to conduct a thorough licensing review to determine the overall implications of 
including this work. For the current status of this project through the Apache Incubator 
visit: https://incubator.apache.org/projects/doris.html
```

The threaded connection for mail can be found here:

`https://lists.apache.org/list.html?dev@doris.apache.org`


### Email Result to general@incubator.apache.org

[RESULT][VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Hi,

Thanks to everyone, and the vote for releasing Apache Doris 0.9.0-incubating-rc01 is now closed.

It has passed with 4 +1 (binding) votes and no 0 or -1 votes.

Binding:
+1 Willem Jiang
+1 Justin Mclean
+1 ShaoFeng Shi
+1 Makoto Yui

The vote thread:
https://lists.apache.org/thread.html/da05fdd8d84e35de527f27200b5690d7811a1e97d419d1ea66562130@%3Cgeneral.incubator.apache.org%3E

Best Regards,
xxx
```

## Finalizing release

### Upload package to release

When the formal voting is successful, email [Result] first, and then prepare the release package.
Copy the source package, signature file and hash file from the corresponding RC folder published under dev to another directory 0.9.0-incubating. Note that the file name does not need rcxx (rename, but do not recalculate signatures, hash can recalculate, the results will not change)

KEYS files also need to be copied if they are first released. Then add to SVN release.

```
After the add is successful, you can see the file you posted on the following URL
https://dist.apache.org/repos/dist/release/incubator/doris/0.9.0-incubating/

After waiting for a while, you can see on the official apache website:
http://www.apache.org/dist/incubator/doris/0.9.0-incubating/

```

### Publish links on Doris website and GitHub

#### Create Download Links

Download link:
http://www.apache.org/dyn/closer.cgi?filename=incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz&action=download

wget --trust-server-names "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz"

Original location:
https://www.apache.org/dist/incubator/doris/0.9.0-incubating/

http://www.apache.org/dyn/closer.cgi/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz

Source package:
http://www.apache.org/dyn/closer.cgi/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz

ASC:
http://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.asc

sha512:
http://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.sha512

KEYS:
http://archive.apache.org /dist /incubator /doris /KEYS

refer to: <http://www.apache.org/dev/release-download-pages#closer>

#### Prepare release note

The following two areas need to be modified:

1. Github's release page

```
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01
```

2. Doris Official Website Download Page

```
http://doris.apache.org /downloads.html
```

### Send Announce e-mail to general@incubator.apache.org

Title:

```
[ANNOUNCE] Apache Doris (incubating) 0.9.0 Release
```

Send mail group:

```
general@incubator.apache.org <general@incubator.apache.org >
dev@doris.apache.org <dev@doris.apache.org >
```

Mail text:

```
Hi All,

We are pleased to announce the release of Apache Doris 0.9.0-incubating.

Apache Doris (incubating) is an MPP-based interactive SQL data warehousing for reporting and analysis.

The release is available at:
http://doris.apache.org/master/en/downloads/downloads.html

Thanks to everyone who has contributed to this release, and the release note can be found here:
https://github.com/apache/incubator-doris/releases

Best Regards,

On behalf of the Doris team,
xxx

----
DISCLAIMER-WIP: 
Apache Doris is an effort undergoing incubation at The Apache Software Foundation (ASF), 
sponsored by the Apache Incubator. Incubation is required of all newly accepted projects 
until a further review indicates that the infrastructure, communications, and decision 
making process have stabilized in a manner consistent with other successful ASF projects. 
While incubation status is not necessarily a reflection of the completeness or stability 
of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

Some of the incubating project’s releases may not be fully compliant with ASF policy. For 
example, releases may have incomplete or un-reviewed licensing conditions. What follows is 
a list of known issues the project is currently aware of (note that this list, by definition, 
is likely to be incomplete): 

 * Releases may have incomplete licensing conditions

If you are planning to incorporate this work into your product/project, please be aware that
you will need to conduct a thorough licensing review to determine the overall implications of 
including this work. For the current status of this project through the Apache Incubator 
visit: https://incubator.apache.org/projects/doris.html

```


