---
{
"title": "Release Preparation",
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

# Release Preparation

Releases of Apache projects must strictly follow the Apache Foundation release process. Related guidance and policies can be found at.

* [Release Creation Process](https://infra.apache.org/release-publishing)
* [Release Policy](https://www.apache.org/legal/release-policy.html)
* [Publishing Maven Releases to Maven Central Repository](https://infra.apache.org/publishing-maven-artifacts.html)

This document describes the main process and prep work for release. For specific Doris component release steps, you can refer to the respective documentation:

* [Doris Core Release](./release-doris-core.md)
* [Doris Connectors Release](./release-doris-connectors.md)
* [Doris Manager Release](./release-doris-manager.md)

There are three main forms of releases for Apache projects.

* Source Release: i.e. source release, this is mandatory.
* Binary Release: e.g., release of a compiled executable. This is optional.
* Convenience Binaries: Release to third-party platforms for user convenience, such as Maven, Docker, etc. This is also optional.

## Release Process

Each project release requires a PMC member or Committer as the **Release Manager**.

The overall release process is as follows.

1. Environment preparation
2. Release preparation
	1. the community initiates DISCUSS and communicates with the community about the specific release plan
	2. create a branch for the release
	3. clean up the issue
	4. merge the necessary patches into the released branch
3. verify the branch
	1. stability testing
	2. verify the compilation flow of the branch code
	3. Prepare Release Nodes
4. prepare release materials
    1. Tagging
    2. upload the content to be released to the [Apache Dev SVN repository](https://dist.apache.org/repos/dist/dev/incubator/doris)
    3. preparation of other Convenience Binaries (e.g. upload to [Maven Staging repository](https://repository.apache.org/#stagingRepositories))
4. Community Release Polling Process
	2. Initiate a VOTE in the [Doris Community Dev Mail Group](dev@doris.apache.org).
	3. After the vote is approved, send a Result email in the Doris community.
	4. If it is an Incubator Project, you also need to
	   1. initiate a VOTE in the [Incubator General Mailing Group](general@incubator.apache.org). 2.
	   2. Send a Result email to general@incubator.apache.org.
5. Complete the work
	1. Upload the signed packages to the [Apache Release repository](https://dist.apache.org/repos/dist/release/incubator/doris) and generate the relevant links.
	2. Post the download links on the Doris website and github, and clean up the old packages on svn.
	3. Send an Announce email to dev@doris.apache.org
	4. If it is an Incuator Project, you also need to
	   1. Send an Announce email to general@incubator.apache.org

## Prepare signatures

If this is your first time as Release Manager, then you need to prepare the following tools in your environment

1. [Release Signing](https://www.apache.org/dev/release-signing.html)
2. [gpg](https://www.apache.org/dev/openpgp.html)
3. [svn](https://www.apache.org/dev/openpgp.html)

### Prepare gpg key

Release manager needs to create its own signature public key before release, and upload it to the public key server, then you can use this public key to sign the package to be released.
If your KEY already exists in [KEYS](https://downloads.apache.org/incubator/doris/KEYS), then you can skip this step.

#### Installation and configuration of the signature software GnuPG

##### GnuPG

In 1991, programmer Phil Zimmermann developed the encryption software PGP in order to avoid government surveillance; it worked so well that it quickly spread and became an essential tool for many programmers. However, it was commercial software and could not be used freely. So, the Free Software Foundation decided to develop a replacement for PGP, named GnuPG, and that's how GPG came to be.

##### installation configuration

CentOS installation command.

```
yum install gnupg
```

After installation, the default configuration file gpg.conf will be placed in the home directory.

```
~/.gnupg/gpg.conf
```

If this directory or file does not exist, you can just create an empty file.

Apache recommends SHA512 for signatures, which can be done by configuring gpg.
Edit gpg.conf, adding the following three lines.

```
personal-digest-preferences SHA512
cert-digest-algo SHA512
default-preference-list SHA512 SHA384 SHA256 SHA224 AES256 AES192 AES CAST5 ZLIB BZIP2 ZIP Uncompressed
```

#### Generate a new signature

##### Preparing a signature

Recommended settings for generating new signatures.

Here you must log in to the user account directly through a terminal such as SecureCRT, not through su - user or ssh, otherwise the password input box will not show up and an error will be reported.

First look at the version of gpg and whether it supports SHA512.

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

##### Generate a new signature

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

Real name should be the same as the id shown in id.apache.org.
Email address is the apache email address.

Enter passphrase, twice, more than 8 characters.

**The secret key here must be remembered, it will be used later when signing. It will also be used for publishing other components**

##### View and output

The first line shows the public key file name (pubring.gpg), the second line shows the public key characteristics (4096 bits, hash string and generation time), the third line shows the "user ID", comments, emails, and the fourth line shows the private key characteristics.

```
$ gpg --list-keys
/home/lide/.gnupg/pubring.gpg
-----------------------------
pub 4096R/33DBF2E0 2018-12-06
uid xxx-yyyy (xxx's key) <xxx@apache.org>
sub 4096R/0E8182E6 2018-12-06
```

where xxx-yyyy is the user ID.

```
gpg --armor --output public-key.txt --export [user-id]
```

```
$ gpg --armor --output public-key.txt --export xxx-yyyy
The file 'public-key.txt' already exists. Is it overwritten? (y/N)y
$ cat public-key.txt
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v2.0.22 (GNU/Linux)

mQINBFwJEQ0BEACwqLluHfjBqD/RWZ4uoYxNYHlIzZvbvxAlwS2mn53BirLIU/G3
9opMWNplvmK+3+gNlRlFpiZ7EvHsF/YJOAP59HmI2Z...
```

#### Uploading Signed Public Keys

A public key server is a server on the network dedicated to storing the user's public key. send-keys parameter can upload the public key to the server.

```
gpg --send-keys xxxx --keyserver https://keyserver.ubuntu.com/

```
where xxxx is the string after pub in the `-list-keys` result of the previous step, e.g., 33DBF2E0

You can also upload the contents of the above public-key.txt via [https://keyserver.ubuntu.com/](https://keyserver.ubuntu.com/).

After successful upload, you can query this [https://keyserver.ubuntu.com/](https://keyserver.ubuntu.com/) by entering 0x33DBF2E0. (Note that it needs to start with 0x)

There is a delay in querying this website, you may need to wait for 1 hour.

#### generates fingerprint and uploads it to apache user information

Since the public key server has no checking mechanism, anyone can upload a public key in your name, so there is no way to guarantee the reliability of the public key on the server. Usually, you can publish a public key fingerprint on your website and let other people check whether the downloaded public key is genuine or not.

The fingerprint parameter generates a public key fingerprint.

```
gpg --fingerprint [user-id]
```

```
$ gpg --fingerprint xxx-yyyy
pub 4096R/33DBF2E0 2018-12-06
      Key fingerprint = 07AA E690 B01D 1A4B 469B 0BEF 5E29 CE39 33DB F2E0
uid xxx-yyyy (xxx's key) <xxx@apache.org>
sub 4096R/0E8182E6 2018-12-06
```

Paste the fingerprint above (i.e. 07AA E690 B01D 1A4B 469B 0BEF 5E29 CE39 33DB F2E0) into your own user information at

https://id.apache.org

`OpenPGP Public Key Primary Fingerprint:`

> Note: Each person can have more than one Public Key.

#### generates keys

**Be careful not to delete existing content in the KEYS file, it can only be added.**

```
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
# edit doris/KEYS file
gpg --list-sigs [user-id] >> doris/KEYS
gpg --armor --export [user ID] >> doris/KEYS
svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m "UPDATE KEYS"
```

Note that the KEYS file should also be published to the following svn library.

```
svn co https://dist.apache.org/repos/dist/release/incubator/doris
# edit doris/KEYS file
svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m "UPDATE KEYS"
```

After that it will automatically sync to.
```
https://downloads.apache.org/incubator/doris/KEYS
```

In subsequent release poll emails, use the KEYS file here in ``https://downloads.apache.org/incubator/doris/KEYS``.

## Maven Release Preparation

For components such as the Doris Connector, you need to use maven for the release.

1. Generate a master password

    `mvn --encrypt-master-password <password>`
    
    This password is only used to encrypt other passwords that follow, and the output is something like `{VSb+6+76djkH/43...} ` Then create the `~/.m2/settings-security.xml` file with the following content

    ```
    <settingsSecurity>
      <master>{VSb+6+76djkH/43...}</master>
    </settingsSecurity>
    ```

2. Encrypt apache passwords


    `mvn --encrypt-password <password>`

    The password is the password for the apache account. The output is similar to `{GRKbCylpwysHfV...}`
    
    Add in `~/.m2/settings.xml`

	```
    <servers>
      <!-- To publish a snapshot of your project -->
      <server>
        <id>apache.snapshots.https</id>
        <username>yangzhg</username>
        <password>{GRKbCylpwysHfV...}</password>
      </server>
      <!-- To stage a release of your project -->
      <server>
        <id>apache.releases.https</id>
        <username>yangzhg</username>
        <password>{GRKbCylpwysHfV...}</password>
      </server>
    </servers>
	```
	
## Initiating DISCUSS in the community

DISCUSS is not a required process before a release, but it is highly recommended to start a discussion in the dev@doris mail group before a major release. Content includes, but is not limited to, descriptions of important features, bug fixes, etc.