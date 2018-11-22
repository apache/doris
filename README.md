# Apache Doris (incubating) Project

Apache Doris is an MPP-based interactive SQL data warehousing for reporting and analysis. It open-sourced by Baidu. Please visit [Doris official site ](http://doris.incubator.apache.org) for more detail information, as well as [Wiki](https://github.com/apache/incubator-doris/wiki) for documents of install, deploy, best pracitices and FAQs.

## Compile and install

### Prerequisites

GCC 5.3.1+，Oracle JDK 1.8+，Python 2.7+, Apache Maven 3.5.4+

For Ubuntu: 

```
sudo apt-get install g++ ant cmake zip byacc flex automake libtool binutils-dev libiberty-dev bison python2.7 libncurses5-dev
sudo updatedb
```

For CentOS:

```
sudo yum install gcc-c++ libstdc++-static ant cmake byacc flex automake libtool binutils-devel bison ncurses-devel
sudo updatedb
```

If your GCC version less than 5.3.1, you can run:

```
sudo yum install devtoolset-4-toolchain -y
```

and then, set the path of gcc (e.g /opt/rh/devtoolset-4/root/usr/bin) to the environment variabl PATH.


### Build

Run following script, it will comiple thirdparty libraries and build whole Doris.

```
sh build.sh
```

After successful build, it will install binary files to the path of output/.

## Links

* Doris official site - <http://doris.incubator.apache.org>
* User Manual (GitHub Wiki) - <https://github.com/apache/incubator-doris/wiki>
* Developer Mailing list - Subscribe to <dev@doris.incubator.apache.org> to discuss with us.
* Gitter channel - <https://gitter.im/apache-doris/Lobby> - Online chat room with Doris developers.
* Overview - <https://github.com/apache/incubator-doris/wiki/Doris-Overview>
* Compile and install - <https://github.com/apache/incubator-doris/wiki/Doris-Install>
* Getting start - <https://github.com/apache/incubator-doris/wiki/Getting-start>
* Deploy and Upgrade - <https://github.com/apache/incubator-doris/wiki/Doris-Deploy-%26-Upgrade>
* User Manual - <https://github.com/apache/incubator-doris/wiki/Doris-Create%2C-Load-and-Delete>
* FAQs - <https://github.com/apache/incubator-doris/wiki/Doris-FAQ>
