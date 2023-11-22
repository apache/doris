#!/bin/env python
# -*- coding: utf-8 -*-
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

"""
This module operates Palo node.
"""

import json
import os
import pexpect
import sys
import threading
import time
import random
import socket

sys.path.append('../deploy')
import env_config
import palo_logger
import palo_client
import util
import palo_job

# 日志 异常 对象
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


class Node(object):
    """
    palo node operation
    """

    def __init__(self):
        self.__master = env_config.master
        self.__follower_list = env_config.follower_list
        self.__observer_list = env_config.observer_list
        self.__be_list = env_config.be_list
        self.__fe_path = env_config.fe_path
        self.__be_path = env_config.be_path
        self.__host_username = env_config.host_username
        self.__host_password = env_config.host_password
        self.__java_home = env_config.JAVA_HOME
        self.__client = None
        self.__query_port = env_config.fe_query_port
        self.__be_ip_list = [socket.gethostbyname(be) for be in self.__be_list]
        self.init_client()

    def init_client(self, host=None, user=None, password=None):
        """get palo client, for get fe master"""
        user = 'root' if user is None else user
        password = '' if password is None else password
        host = random.choice(self.__follower_list + self.__observer_list + [self.__master]) if host is None else host
        try:
            self.__client = palo_client.get_client(host, self.__query_port,
                                                   user=user, password=password)
        except Exception as e:
            LOG.info(L("INIT CLIENT FAILED", host=host, port=self.__query_port))

    def stop_fe(self, host_name):
        """stop fe node"""
        LOG.info(L('STOP FE.'))
        cmd = 'cd %s/fe;sh bin/stop_fe.sh' % self.__fe_path
        status, output = self.__exec_cmd(cmd, host_name=host_name)
        if status == 0:
            LOG.info(L('STOP FE SUCC.', ret=True))
            return True
        else:
            LOG.info(L('STOP FE FAILED.', ret=False))
            return False

    def start_fe(self, host_name):
        """start fe node"""
        LOG.info(L('START FE.'))
        cmd = 'export JAVA_HOME=%s;cd %s/fe;sh bin/start_fe.sh --daemon' % (self.__java_home, self.__fe_path)
        status, output = self.__exec_cmd(cmd, host_name)
        if status == 0:
            LOG.info(L('START FE SUCC.', ret=True))
            return True
        else:
            LOG.info(L('START FE FAILED.', ret=False))
            return False

    def is_fe_alive(self, host_name):
        """check is fe alive
        """
        LOG.info(L('CHECK FE STATUE.'))
        cmd = "cd %s/fe;kill -0 `cat bin/fe.pid` >/dev/null 2>&1" % self.__fe_path
        status, output = self.__exec_cmd(cmd, host_name)
        if status == 0:
            LOG.info(L('CHECK FE STATUE. FE is alive'))
            return True
        else:
            LOG.info(L('CHECK FE STATUE. FE is dead'))
            return False

    def is_be_alive(self, host_name):
        """check is be alive
        """
        LOG.info(L('CHECK BE STATUS.'))
        cmd = "cd %s/be;kill -0 `cat bin/be.pid` >/dev/null 2>&1" % self.__be_path
        status, output = self.__exec_cmd(cmd, host_name)
        if status == 0:
            LOG.info(L('CHECK BE STATUS. BE is alive'))
            return True
        else:
            LOG.info(L('CHECK BE STATUS. BE is dead'))
            return False

    def __exec_cmd(self, cmd, host_name, timeout=120):
        exe_cmd = 'ssh %s@%s "%s"' % (self.__host_username, host_name, cmd)
        output, status = pexpect.run(exe_cmd, timeout=timeout, withexitstatus=True,
                                     events={"continue connecting": "yes\n",
                                             "password:": "%s\n" % self.__host_password})
        LOG.info(L("execute CMD", exe_cmd=exe_cmd, status=status, output=output))
        return status, output

    def stop_be(self, host_name):
        """Stop BE
        """
        LOG.info(L('STOP BE.'))
        cmd_a = 'cd %s/be;sh bin/stop_be.sh' % self.__be_path
        status, output = self.__exec_cmd(cmd_a, host_name)
        if status == 0:
            LOG.info(L('STOP BE SUCC.'))
            return True
        else:
            LOG.info(L('STOP BE FAILED.'))
            return False

    def start_be(self, host_name):
        """Start BE
        """
        LOG.info(L('START BE.'))
        cmd = 'export PATH=$PATH:/sbin; export JAVA_HOME=%s; cd %s/be;sh bin/start_be.sh --daemon' \
              % (self.__java_home, self.__be_path)
        status, output = self.__exec_cmd(cmd, host_name)
        if status == 0:
            LOG.info(L('START BE SUCC.'))
            return True
        else:
            LOG.info(L('START BE FAILED.'))
            return False

    def get_observer(self):
        """get a observer"""
        return random.choice(self.__observer_list)

    def get_master(self):
        """get fe master"""
        retry_times = 10
        if self.__client is not None and self.__client.connection:
            while retry_times > 0:
                retry_times -= 1
                try:
                    self.init_client()
                    self.__master = self.__client.get_master_host()
                    break
                except Exception as e:
                    LOG.info(L("reconnect to fe"))
                time.sleep(3)
            else:
                raise Exception('can not connect to palo and get master')
        return self.__master

    def get_follower(self):
        """get fe follower not master"""
        retry_times = 10
        if self.__client is not None and self.__client.connection:
            while retry_times > 0:
                retry_times -= 1
                try:
                    self.init_client()
                    ret = self.__client.get_fe_list()
                    break
                except Exception as e:
                    LOG.info(L("reconnect to fe"))
                time.sleep(3)
            else:
                raise Exception('can not connect to palo and get master')
        self.__follower_list = util.get_attr_condition_list(ret, palo_job.FrontendInfo.Role,
                                                            'FOLLOWER', palo_job.FrontendInfo.Host)
        return random.choice(self.__follower_list)

    def get_fe_list(self):
        """get fe list"""
        return self.__observer_list + [self.__master] + self.__follower_list

    def get_be_list(self):
        """get be list"""
        return self.__be_list

    def get_be_ip_list(self):
        """get be ip list"""
        return self.__be_ip_list

    def restart_fe(self, host_name, wait_time=10):
        """restart fe"""
        self.stop_fe(host_name)
        time.sleep(wait_time)
        self.start_fe(host_name)

    def restart_be(self, host_name, wait_time=10):
        """retart be"""
        self.stop_be(host_name)
        time.sleep(wait_time)
        self.start_be(host_name)

    def is_be_core(self, host_name):
        """check if be has core file"""
        cmd = "cat %s/be/log/be.out" % self.__be_path
        status, output = self.__exec_cmd(cmd, host_name)
        print(output.replace("'s password", "'s be.out"))
        LOG.info(L('CHECK BE CORE.', be=host_name))
        cmd = "ls -lh %s/be/core.*" % (self.__be_path)
        status, output = self.__exec_cmd(cmd, host_name)
        if status == 0:
            LOG.info(L('BE HAS COREFILE.', be=host_name))
            return True
        else:
            LOG.info(L('BE HAS NO COREFILE.', be=host_name))
            return False

    def get_image_version(self, host_name):
        """
        get fe image version
        may have image.[log-id] & image.ckpt temp file
        """
        cmd = 'ls %s/fe/palo-meta/image/image.[0-9]*' % self.__fe_path
        status, output = self.__exec_cmd(cmd, host_name)
        image_version_list = list()
        if status == 0:
            output_list = output.split('\r\n')
            for output in output_list:
                output = output.strip()
                images = output.split('image.')
                if len(images) != 2:
                    continue
                version = images[-1]
                image_version_list.append(version)
            LOG.info(L('get fe image file version', fe=host_name, version=image_version_list))
            return image_version_list
        else:
            LOG.warning(L('get fe image file version failed', fe=host_name, msg=output, status=status))
            return None

    def check_cluster(self, start_if_dead=True, fe_check=True, be_check=True):
        """check fe status and be status, start if dead"""
        if fe_check:
            for fe in self.get_fe_list():
                if not self.is_fe_alive(fe):
                    self.start_fe(fe)
        if be_check:
            for be in self.get_be_list():
                if not self.is_be_alive(be):
                    self.start_be(be)

    def modify_be_conf(self, hostname, option, value):
        """modify be conf, and restart"""
        if isinstance(value, str):
            value_s = value.replace('/', '\/')
        else:
            value_s = value
        # 获取文件中是否有option，如果有则sed修改配置，如果没有则echo追加到文件末尾
        cmd = "grep -q '^{option}' {filepath} && " \
              "sed -i 's/^{option}.*/{option} = {value_a}/g' {filepath} || " \
              "echo '\n{option} = {value}' >> {filepath}".format(option=option, value=value,
                                                                 filepath=self.__be_path + '/be/conf/be.conf',
                                                                 value_a=value_s)
        status, output = self.__exec_cmd(cmd, hostname)
        if status == 0:
            LOG.info(L("modify be conf succeed, will restart be", be=hostname, config="%s=%s" % (option, value)))
            self.restart_be(hostname)
            return True
        else:
            LOG.warning(L("modify be conf failed", be=hostname, config="%s=%s" % (option, value)))
            return False

    def modify_fe_conf(self, hostname, option, value):
        """modify fe conf, and restart"""
        if isinstance(value, str):
            value_s = value.replace('/', '\/')
        else:
            value_s = value
        # 获取文件中是否有option，如果有则sed修改配置，如果没有则echo追加到文件末尾
        cmd = "grep -q '^{option}' {filepath} && " \
              "sed -i 's/^{option}.*/{option} = {value_a}/g' {filepath} || " \
              "echo '\n{option} = {value}' >> {filepath}".format(option=option, value=value,
                                                                 filepath=self.__fe_path + '/fe/conf/be.conf',
                                                                 value_a=value_s)
        status, output = self.__exec_cmd(cmd, hostname)
        if status == 0:
            LOG.info(L("modify be conf succeed, will restart be", be=hostname, config="%s=%s" % (option, value)))
            self.restart_be(hostname)
            return True
        else:
            LOG.warning(L("modify be conf failed", be=hostname, config="%s=%s" % (option, value), msg=output))
            return False


if __name__ == '__main__':
    env = Node()
    env.check_cluster(fe_check=False)


