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

The steps in this document follow after the release has been voted on and approved in the dev@doris or general@incubator mail groups.

## Upload package to release

When the official release poll is successful, send the [Result] email first, then prepare the release package.
Copy the source package, signature file and hash file from the corresponding folder of the previous release under dev to another directory 0.9.0-incubating, note that the file name should not be rcxx (you can rename, but do not recalculate the signature, the hash can be recalculated, the result will not change)

```
From:
https://dist.apache.org/repos/dist/dev/incubator/doris/

To:
https://dist.apache.org/repos/dist/release/incubator/doris/
```

For the first release, you need to copy the KEYS file as well. Then add it to the svn release.

```
add 成功后就可以在下面网址上看到你发布的文件
https://dist.apache.org/repos/dist/release/incubator/doris/0.xx.0-incubating/

稍等一段时间后，能在 apache 官网看到：
http://www.apache.org/dist/incubator/doris/0.9.0-incubating/
```

## Post links on Doris official website and github

We will use Doris Core as an example. For other components, replace the name with the corresponding one.

### Create a download link

Download Link:

```
http://www.apache.org/dyn/closer.cgi?filename=incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz&action=download

wget --trust-server-names "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz"
```

Original Location:

```
https://www.apache.org/dist/incubator/doris/0.9.0-incubating/

http://www.apache.org/dyn/closer.cgi/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz
```

Ssource package:

```
http://www.apache.org/dyn/closer.cgi/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz

ASC:
http://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.asc

sha512:
http://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.sha512
```

KEYS:

```
http://archive.apache.org/dist/incubator/doris/KEYS
```

refer to: <http://www.apache.org/dev/release-download-pages#closer>

### Prepare the release note

The following two places need to be modified.

1. Github's release page

```
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01
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
https://dist.apache.org/repos/dist/release/incubator/doris/
https://dist.apache.org/repos/dist/dev/incubator/doris/
```

Keep these two addresses with only the latest package versions. 2.

2. Change the download address of the older packages on the official Doris website to the address of the archive page 

```
Download page: http://doris.apache.org/downloads.html
Archive page: http://archive.apache.org/dist/incubator/doris
```

Apache has a synchronization mechanism to archive the history of releases, see [how to archive](https://www.apache.org/legal/release-policy.html#how-to-archive)
So even if an old package is removed from svn, it can still be found on the archive page.

## Announce

Title:

```
[ANNOUNCE] Apache Doris (incubating) 0.9.0 Release
```

To mail：

```
dev@doris.apache.org
```

Incubator projects, which also need to be sent to:

```
general@incubator.apache.org
```

Email body:

```
Hi All,

We are pleased to announce the release of Apache Doris 0.9.0-incubating.

Apache Doris (incubating) is an MPP-based interactive SQL data warehousing for reporting and analysis.

The release is available at:
http://doris.apache.org/master/zh-CN/downloads/downloads.html

Thanks to everyone who has contributed to this release, and the release note can be found here:
https://github.com/apache/incubator-doris/releases

Best Regards,

On behalf of the Doris team,
xxx

---
DISCLAIMER: 
Apache Doris (incubating) is an effort undergoing incubation at The
Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted
projects until a further review indicates that the
infrastructure, communications, and decision making process have
stabilized in a manner consistent with other successful ASF
projects.

While incubation status is not necessarily a reflection
of the completeness or stability of the code, it does indicate
that the project has yet to be fully endorsed by the ASF.
```



