# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM python:3.10.12-bookworm
LABEL authors="davidchen"

USER root

# Define build arguments
ARG DBT_DORIS_HOST
ARG DBT_DORIS_USERNAME
ARG DBT_DORIS_PASSWORD

# set the environment variable of host passwd and username from build args
ENV DBT_DORIS_HOST  ${DBT_DORIS_HOST}
ENV DBT_DORIS_USERNAME  ${DBT_DORIS_USERNAME}
ENV DBT_DORIS_PASSWORD  ${DBT_DORIS_PASSWORD}


# change debian source # if you are in china you can use tsinghua source
RUN sed -i 's@deb.debian.org@mirrors.aliyun.com@g' /etc/apt/sources.list.d/debian.sources

# install git
RUN apt update && apt install git -y

# install dbt, if you are in china you can use aliyun source
RUN pip install --upgrade pip --index https://mirrors.aliyun.com/pypi/simple/
RUN pip install dbt-doris==0.3.4 --index https://mirrors.aliyun.com/pypi/simple/
RUN pip install dbt-core==1.5.11 --index https://mirrors.aliyun.com/pypi/simple/
# otherwise you can use the default source
#RUN pip install --upgrade pip
#RUN pip install dbt-doris==0.3.4
#RUN pip install dbt-core==1.5.11


# copy dbt project
COPY /dbt /dbt
WORKDIR /dbt

# run dbt debug
RUN dbt debug

# run dbt deps
RUN dbt deps

CMD ["echo", "build success"]

# dbt seeds
RUN dbt seed

# run dbt
CMD ["dbt", "run"]

#CMD ["dbt", "test"]

#ENTRYPOINT ["tail", "-f", "/dev/null"]

