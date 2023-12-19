FROM openjdk:8u342-jdk

ARG TARGETARCH

ENV JAVA_HOME="/usr/local/openjdk-8/"
RUN echo "vm.max_map_count=2000000" >> /etc/sysctl.conf

RUN sed -i 's@http://deb.debian.org@http://mirrors.aliyun.com@g' /etc/apt/sources.list &&\
     apt-get clean &&\
     apt-get update &&\
     apt-get install -y default-mysql-client

ARG DORIS_VERSION=2.0.3

RUN arch=$([ "${TARGETARCH:-amd64}" == "arm64" ] && echo "x64" || echo "arm64") &&\
     cd /opt &&\
     wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-${DORIS_VERSION}-bin-${arch}.tar.gz &&\
     tar -xf ./apache-doris-${DORIS_VERSION}-bin-${arch}.tar.gz &&\
     mv /opt/apache-doris-${DORIS_VERSION}-bin-${arch} /opt/apache-doris &&\
     rm -f /opt/apache-doris-${DORIS_VERSION}-bin-${arch}.tar.gz

ENV PATH="/opt/apache-doris/be/bin:$PATH"

ADD ./resource/init.sh /opt/apache-doris/
RUN chmod 755 /opt/apache-doris/init.sh

ENTRYPOINT ["/opt/apache-doris/init.sh"]
