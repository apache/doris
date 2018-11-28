# Apache Doris (incubating)

Doris is an MPP-based interactive SQL data warehousing for reporting and analysis. It open-sourced by Baidu. 

## 1. License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## 2. Technology
Doris mainly integrates the technology of Google Mesa and Apache Impala, and it based on a column-oriented storage engine and can communicate by MySQL client.

## 3. User cases
Doris not only provides high concurrent low latency point query performance, but also provides high throughput queries of ad-hoc analysis. 

Doris not only provides batch data loading, but also provides near real-time mini-batch data loading. 

Doris also provides high availability, reliability, fault tolerance, and scalability. 

The simplicity (of developing, deploying and using) and meeting many data serving requirements in single system are the main features of Doris (refer to [Overview](https://github.com/apache/incubator-doris/wiki/Doris-Overview)).

## 4. Compile and install

Currently support Docker environment and Linux OS: 
Docker (Linux / Windows / Mac), Ubuntu and CentOS.

### 4.1 For Docker

#### Step 1: Install Docker

Take CentOS as an example:

```
yum -y install docker-io
service docker start
```

#### Step 2: Create Docker image for complilation

Given your work space is /my/workspace, and you can download Doris docker file as following:

```
wget https://github.com/apache/incubator-doris/blob/master/docker/Dockerfile -O /my/workspace/Dockerfile
```

Now build your image, and it may take a long time (40min to 1 hour)

```
cd /my/workspace && docker build -t doris-dev:v1.0 .
```

#### Step 3: Compile and install Doris

Clone Doris source:

```
git clone https://github.com/apache/incubator-doris.git /path/to/incubator-doris/
```

Start a container named doris-dev-test, and map /path/to/incubator-doris/ to /var/local/incubator-doris/ in container.

```
docker run -it --name doris-dev-test -v /path/to/incubator-doris/:/var/local/incubator-doris/ doris-dev:v1.0
```

Compile Doris source:

```
sh build.sh 
```

After successfully building, it will install binary files in the directory output/.

### 4.2 For Linux

#### Prerequisites

GCC 5.3.1+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.4.3+

* For Ubuntu: 

```
sudo apt-get install g++ ant cmake zip byacc flex automake libtool binutils-dev libiberty-dev bison python2.7 libncurses5-dev
sudo updatedb
```

* For CentOS:

```
sudo yum install gcc-c++ libstdc++-static ant cmake byacc flex automake libtool binutils-devel bison ncurses-devel
sudo updatedb
```

If your GCC version is less than 5.3.1, you can run:

```
sudo yum install devtoolset-4-toolchain -y
```

and then, set the path of gcc (e.g /opt/rh/devtoolset-4/root/usr/bin) to the environment variabl PATH.


#### Compile and install

Run following script, it will compile thirdparty libraries and build whole Doris.

```
sh build.sh
```

After successfully building, it will install binary files in the directory output/.

## 5. Reporting Issues

If you find any bugs, please file a [GitHub issue](https://github.com/apache/incubator-doris/issues).

## 6. Links

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
