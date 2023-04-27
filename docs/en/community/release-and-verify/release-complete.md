---
{
    "title": "Complete Release",
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

# Complete release

The steps in this document follow after the release has been voted on and approved in the dev@doris.

## Upload package to release

When the official release poll is successful, send the [Result] email first, then prepare the release package.
Copy the source package, signature file and hash file from the corresponding folder of the previous release under dev to another directory 1.xx, note that the file name should not be rcxx (you can rename, but do not recalculate the signature, the hash can be recalculated, the result will not change)

> Only PMC members have permission to operate this step.

```
From:
https://dist.apache.org/repos/dist/dev/doris/

To:
https://dist.apache.org/repos/dist/release/doris/

Eg:
svn mv -m "move doris 1.1.0-rc05 to release" https://dist.apache.org/repos/dist/dev/doris/1.1 https://dist.apache.org/repos/dist/release/doris/1.1
```

For the first release, you need to copy the KEYS file as well. Then add it to the svn release.

```
add 成功后就可以在下面网址上看到你发布的文件
https://dist.apache.org/repos/dist/release/doris/1.xx/

稍等一段时间后，能在 apache 官网看到：
http://www.apache.org/dist/doris/1.xx/
```

## Post links on Doris official website and github

We will use Doris Core as an example. For other components, replace the name with the corresponding one.

### Create a download link

Download Link:

```
http://www.apache.org/dyn/closer.cgi?filename=doris/1.xx/apache-doris-1.xx-src.tar.gz&action=download

wget --trust-server-names "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=doris/1.xx/apache-doris-1.xx-src.tar.gz"
```

Original Location:

```
https://www.apache.org/dist/doris/1.xx/

http://www.apache.org/dyn/closer.cgi/doris/1.xx/apache-doris-1.xx-src.tar.gz
```

Ssource package:

```
http://www.apache.org/dyn/closer.cgi/doris/1.xx/apache-doris-1.xx-src.tar.gz

ASC:
http://archive.apache.org/dist/doris/1.xx/apache-doris-1.xx-src.tar.gz.asc

sha512:
http://archive.apache.org/dist/doris/1.xx/apache-doris-1.xx-src.tar.gz.sha512
```

KEYS:

```
http://archive.apache.org/dist/doris/KEYS
```

refer to: <http://www.apache.org/dev/release-download-pages#closer>

### Maven

Find staging repo on [https://repository.apache.org/#stagingRepositories](https://repository.apache.org/#stagingRepositories) and click `Release` to release.

### Prepare the release note

The following two places need to be modified.

1. Github's release page

```
https://github.com/apache/doris/releases/tag/0.9.0-rc01
```

2、Doris official website download page

The download page is a markdown file with the following address.

```
docs/zh-cn/downloads/downloads.md
docs/en/downloads/downloads.md
```

1. you need to change the download package address of the last release to the archive address of apache (see later).
2. Add the download information for the new version.

### Clean up old versions of packages on svn

1. Deleting old packages on svn

Since svn only needs to keep the latest version of packages, old versions of packages should be cleaned from svn when a new version is released.

```
https://dist.apache.org/repos/dist/release/doris/
https://dist.apache.org/repos/dist/dev/doris/
```

Keep these two addresses with only the latest package versions. 2.

2. Change the download address of the older packages on the official Doris website to the address of the archive page 

```
Download page: http://doris.apache.org/downloads.html
Archive page: http://archive.apache.org/dist/doris
```

Apache has a synchronization mechanism to archive the history of releases, see [how to archive](https://www.apache.org/legal/release-policy.html#how-to-archive)
So even if an old package is removed from svn, it can still be found on the archive page.

## Announce

Title:

```
[ANNOUNCE] Apache Doris 1.xx release
```

To mail：

```
dev@doris.apache.org
```

Email body:

```
Hi All,

We are pleased to announce the release of Apache Doris 1.xx.

Apache Doris is an MPP-based interactive SQL data warehousing for reporting and analysis.

The release is available at:
http://doris.apache.org/master/zh-CN/downloads/downloads.html

Thanks to everyone who has contributed to this release, and the release note can be found here:
https://github.com/apache/doris/releases

Best Regards,

On behalf of the Doris team,
xxx

