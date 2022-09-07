# Base Images
FROM bash:latest

# add script file
ADD *.sh /root/

# set working directory
WORKDIR /root/

# Set the mirror source and basic environment
RUN /bin/sh -c echo 'http://mirrors.sjtug.sjtu.edu.cn/alpine/v3.15/main' > /etc/apk/repositories && \
    echo 'http://mirrors.sjtug.sjtu.edu.cn/alpine/v3.15/community' >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache mysql-client docker && \
    rm -rf /usr/bin/docker-proxy /usr/bin/docker-init /usr/bin/dockerd && \
    chmod 755 *.sh
