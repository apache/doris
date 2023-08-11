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
This module describe environment info about Palo.

Date:    2015/10/07 17:23:06
"""

import os
import pexpect
import threading
import time
import env_config
from lib import palo_client


class PaloEnv(object):
    """
    Palo部署及运行环境信息
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
        self.__fe_query_port = env_config.fe_query_port
        self.__be_heartbeat_port = self.__fe_query_port + 20
        self.__be_data_path_list = env_config.be_data_path_list
        self.__master_lock = threading.Lock()
        self.__follower_lock = threading.Lock()
        self.__observer_lock = threading.Lock()
        self.__be_lock = threading.Lock()
        self.__dynamic_add_fe_list = env_config.dynamic_add_fe_list
        self.__dynamic_add_be_list = env_config.dynamic_add_be_list
        self.__dynamic_add_fe_lock = threading.Lock()
        self.__dynamic_add_be_lock = threading.Lock()

    def init(self):
        """init
        """
        if not self.reset_master():
            return False
        return True

    def get_master(self):
        """返回mster
        """
        with self.__master_lock:
            return self.__master

    def set_master(self, host_name):
        """设置mster
        """
        assert host_name
        with self.__master_lock:
            self.__master = host_name

    def remove_master(self):
        """remove mster
        """
        with self.__master_lock:
            self.__master = None

    def __get_client(self, host_name, port):
        """
        """
        assert host_name
        client = palo_client.PaloClient(host_name, port)
        if client.init():
            time.sleep(10)
            return client
        else:
            return None

    def reset_master(self):
        """reset master
        """
        # 如果没有get到master, 重试，等待重新选了一个新的master
        ix = 0
        while ix < 120:
            if self.reset_master_once():
                return True
            time.sleep(1)
            ix += 1
        else:
            return False

    def reset_master_once(self):
        """reset master once
        """
        cur_master = self.get_master()
        electable_tuple = (self.get_master(),) + self.get_follower_tuple() if \
                self.get_master() else self.get_follower_tuple()
        for host_name in electable_tuple:
            client = self.__get_client(host_name, self.get_fe_query_port())
            if not client:
                return False
            if client.is_master():
                if host_name != cur_master:
                    if cur_master:
                        self.add_follower(cur_master)
                    self.remove_follower(host_name)
                    self.set_master(host_name)
                return True
        return False

    def get_follower_tuple(self):
        """返回follower
        """
        with self.__follower_lock:
            return tuple(self.__follower_list)

    def add_follower(self, host_name):
        """增加follower
        """
        assert host_name
        if host_name not in self.__follower_list:
            with self.__follower_lock:
                self.__follower_list.append(host_name)

    def remove_follower(self, host_name):
        """删除follower
        """
        assert host_name
        if host_name in self.__follower_list:
            with self.__follower_lock:
                self.__follower_list.remove(host_name)

    def get_observer_tuple(self):
        """返回observer
        """
        with self.__follower_lock:
            return tuple(self.__observer_list)

    def add_observer(self, host_name):
        """增加observer
        """
        assert host_name
        if host_name not in self.__observer_list:
            with self.__observer_lock:
                self.__observer_list.append(host_name)

    def remove_observer(self, host_name):
        """删除observer
        """
        assert host_name
        if host_name in self.__observer_list:
            with self.__observer_lock:
                self.__observer_list.remove(host_name)

    def get_fe_tuple(self):
        """返回FE
        """
        return (self.get_master(),) + self.get_follower_tuple() + self.get_observer_tuple()

    def get_dynamic_add_fe_tuple(self):
        """返回dynamic added fe
        """
        with self.__dynamic_add_fe_lock:
            return tuple(self.__dynamic_add_fe_list)

    def add_dynamic_add_fe(self, host_name):
        """增加dynamic added fe
        """
        assert host_name
        if host_name not in self.__dynamic_add_fe_list:
            with self.__dynamic_add_fe_lock:
                self.__dynamic_add_fe_list.append(host_name)

    def remove_dynamic_add_fe(self, host_name):
        """删除dynamic added fe
        """
        assert host_name
        if host_name in self.__dynamic_add_fe_list:
            with self.__dynamic_add_fe_lock:
                self.__dynamic_add_fe_list.remove(host_name)

    def get_be_tuple(self):
        """返回BE
        """
        with self.__be_lock:
            return tuple(self.__be_list)

    def add_be(self, host_name):
        """增加BE
        """
        assert host_name
        if host_name not in self.__be_list:
            with self.__be_lock:
                self.__be_list.append(host_name)

    def remove_be(self, host_name):
        """删除BE
        """
        assert host_name
        if host_name in self.__be_list:
            with self.__be_lock:
                self.__be_list.remove(host_name)

    def get_dynamic_add_be_tuple(self):
        """返回dynamic added be
        """
        with self.__dynamic_add_be_lock:
            return tuple(self.__dynamic_add_be_list)

    def add_dynamic_add_be(self, host_name):
        """增加dynamic added be
        """
        assert host_name
        if host_name not in self.__dynamic_add_be_list:
            with self.__dynamic_add_be_lock:
                self.__dynamic_add_be_list.append(host_name)

    def remove_dynamic_add_be(self, host_name):
        """删除dynamic added be
        """
        assert host_name
        if host_name in self.__dynamic_add_be_list:
            with self.__dynamic_add_be_lock:
                self.__dynamic_add_be_list.remove(host_name)

    def get_fe_query_port(self):
        """fe query port
        """
        return self.__fe_query_port

    def get_be_heartbeat_port(self):
        """be port
        """
        return self.__be_heartbeat_port

    def __exec_cmd(self, cmd, host_name, timeout=120):
        """exec cmd"""
        exe_cmd = 'ssh %s@%s "%s"' % (self.__host_username, host_name, cmd)
        output, status = pexpect.run(exe_cmd, timeout=timeout, withexitstatus=True, \
                events = {"continue connecting":"yes\n", "password:":"%s\n" % self.__host_password})
        if 'PALO_CLIENT_LOG_SQL' in os.environ.keys():
            palo_client.LOG.info(palo_client.L("execute CMD", exe_cmd=exe_cmd, \
                    status=status, output=output))
        return status, output

    def stop_master(self):
        """Stop master
        """
        # 结束当前master进程
        cmd = 'cd %s/fe;sh bin/stop_fe.sh' % (self.__fe_path)
        ix = 0
        while ix < 120:
            self.__exec_cmd(cmd, host_name=self.__master)
            if not self.is_fe_alive(self.get_master()):
                self.remove_master()
                return True
            time.sleep(1)
            ix += 1
        else:
            return False

    def start_electable(self, host_name):
        """Start electable 
        """
        return self.start_follower(host_name)

    def start_follower(self, host_name):
        """Start follower
        """
        if host_name in self.get_follower_tuple():
            return False
        cmd = 'cd %s/fe;sh bin/start_fe.sh' % self.__fe_path
        self.__exec_cmd(cmd, host_name)
        time.sleep(3)
        if self.__get_client(host_name, self.get_fe_query_port()):
            self.add_follower(host_name)
            time.sleep(10)
            return True
        else:
            return False

    def stop_follower(self, host_name):
        """stop follower
        """
        if host_name not in self.get_follower_tuple():
            return False
        cmd = 'cd %s/fe;sh bin/stop_fe.sh' % (self.__fe_path)
        ix = 0
        while ix < 120:
            self.__exec_cmd(cmd, host_name)
            if not self.is_fe_alive(host_name):
                self.remove_follower(host_name)
                return True
            time.sleep(1)
            ix += 1
        else:
            return False

    def start_observer(self, host_name):
        """Start master
        """
        if host_name in self.get_observer_tuple():
            return False
        cmd = 'cd %s/fe;sh bin/start_fe.sh' % (self.__fe_path)
        self.__exec_cmd(cmd, host_name)
        time.sleep(3)
        if self.__get_client(host_name, self.get_fe_query_port()):
            self.add_observer(host_name)
            time.sleep(10)
            return True
        else:
            return False

    def stop_observer(self, host_name):
        """stop observer
        """
        if host_name not in self.get_observer_tuple():
            return False
        cmd = 'cd %s/fe;sh bin/stop_fe.sh' % (self.__fe_path)
        ix = 0
        while ix < 120:
            self.__exec_cmd(cmd, host_name)
            if not self.is_fe_alive(host_name):
                self.remove_observer(host_name)
                return True
            time.sleep(1)
            ix += 1
        else:
            return False

    def is_fe_alive(self, host_name):
        """check is fe alive
        """
        cmd = "cd %s/fe;if [ -f bin/fe.pid ]; then if kill -0 `cat bin/fe.pid` " \
                ">/dev/null 2>&1; then echo 'running'; fi; fi" % (self.__fe_path)
        if self.__exec_cmd(cmd, host_name)[1].rstrip('\r\n') == 'running':
            return True
        else:
            return False

    def is_be_alive(self, host_name):
        """check is be alive
        """
        cmd = "cd %s/be;if [ -f bin/be.pid ]; then if kill -0 `cat bin/be.pid` " \
                ">/dev/null 2>&1; then echo 'running'; fi; fi" % (self.__be_path)
        if self.__exec_cmd(cmd, host_name)[1].rstrip('\r\n') == 'running':
            return True
        else:
            return False

    def restart_master(self):
        """restart master
        """
#       follower_tuple = self.get_follower_tuple()
#       for follower in follower_tuple:
#           if not self.stop_follower(follower):
#               return False
        old_master = self.get_master()
        if not self.stop_master():
            return False
        if not self.start_electable(old_master):
            return False
#       for follower in follow_tuple:
#           if not self.start_follower(follower):
#               return False
#       return True
        if not self.reset_master():
            return False
        return True

    def switch_master(self):
        """主备切换
        """
        cur_master = self.get_master()
        if not self.stop_master():
            return False
        if not self.reset_master():
            return False
        if not self.start_electable(cur_master):
            return False
        return True

    def restart_follower(self, host_name=None):
        """restart follower
        """
        if not self.stop_follower(host_name):
            return False
        if not self.start_follower(host_name):
            return False
        return True

    def restart_observer(self, host_name=None):
        """restart observer
        """
        if not self.stop_observer(host_name):
            return False
        if not self.start_observer(host_name):
            return False
        return True

    def stop_be(self, host_name):
        """Stop BE
        """
        cmd_a = 'cd %s/be;sh bin/stop_be.sh' % (self.__be_path)
        cmd_b = 'pkill -f %s/be/lib/palo_be' % (self.__be_path)
        ix = 0
        while ix < 120:
            self.__exec_cmd(cmd_a, host_name)
            self.__exec_cmd(cmd_b, host_name)
            if not self.is_be_alive(host_name):
                self.remove_be(host_name)
                return True
            time.sleep(1)
            ix += 1
        return False

    def start_be(self, host_name):
        """Start BE
        """
        if host_name in self.get_be_tuple():
            return False
        cmd= 'cd %s/be;sh bin/start_be.sh' % (self.__be_path)
        ix = 0
        while ix < 120:
            self.__exec_cmd(cmd, host_name)
            if self.is_be_alive(host_name):
                self.add_be(host_name)
                time.sleep(10)
                return True
            time.sleep(1)
            ix += 1
        return False

    def restart_be(self, host_name):
        """Restart be
        """
        if not self.stop_be(host_name):
            return False
        if not self.start_be(host_name):
            return False
        return True

    def stop_all_fe(self):
        """stop all fe
        """
        for host_name in self.get_follower_tuple():
            if not self.stop_follower(host_name):
                return False
            
        for host_name in self.get_observer_tuple():
            if not self.stop_observer(host_name):
                return False

        if not self.stop_master():
            return False

        return True

    def stop_all_be(self):
        """stop all be
        """
        for host_name in self.get_be_tuple():
            if not self.stop_be(host_name):
                return False
        return True

    def update_fe_config(self, option_value_dict):
        """Update FE config
        """
        master = self.get_master()
        follower_tuple = self.get_follower_tuple()
        observer_tuple = self.get_observer_tuple()
        fe_tuple = self.get_fe_tuple()

        if not self.stop_all_fe():
            return False

        configfile_path = '%s/fe/conf/fe.conf' % (self.__fe_path)
        for host_name in fe_tuple:
            for option, value in option_value_dict.iteritems():
                if not self.modify_config(host_name, configfile_path, option, value):
                    return False

        self.start_electable(master)

        follower_threads = []
        observer_threads = []
        for host_name in follower_tuple:
            t = threading.Thread(target=self.start_follower, args=(host_name,))
            t.start()
            follower_threads.append(t)

        for host_name in observer_tuple:
            t = threading.Thread(target=self.start_observer, args=(host_name,))
            t.start()
            observer_threads.append(t)

        for t in follower_threads:
            t.join()
        for t in observer_threads:
            t.join()

        for host_name in follower_tuple + observer_tuple:
            if not self.is_fe_alive(host_name):
                return False

        if not self.reset_master():
            return False

        return True

    def update_be_config(self, option_value_dict):
        """Update BE config
        """
        be_tuple = self.get_be_tuple()
        if not self.stop_all_be():
            return False

        configfile_path = '%s/be/conf/be.conf' % (self.__be_path)
        for host_name in be_tuple:
            for option, value in option_value_dict.iteritems():
                if not self.modify_config(host_name, configfile_path, option, value):
                    return False

        be_threads = []
        for host_name in self.be_tuple:
            t = threading.Thread(target=self.start_be, args=(host_name,))
            t.start()
            be_threads.append(t)

        for t in be_threads:
            t.join()

        for host_name in be_tuple:
            if not self.is_be_alive(host_name):
                return False

        return True

    def modify_config(self, host_name, filepath, option, value):
        """修改配置
        """
        cmd = "grep -q '^{option}' {filepath} && "\
                "sed -i 's/^{option}.*/{option} = {value}/g' {filepath} || "\
                "echo '{option} = {value}' >> {filepath}".format( \
                option=option, value=value, filepath=filepath)
        status, output = self.__exec_cmd(cmd, host_name)
        if status != 0 or output:
            return False
        cmd = "grep -q '^{} = {}' {}".format(option, value, filepath)
        status, output = self.__exec_cmd(cmd, host_name)
        if status != 0:
            return False
        return True

    def remove_config(self, host_name, filepath, option):
        """删除配置
        """
        cmd = 'sed -i "s/^%s.*/ /" %s' % (option, filepath)
        self.__exec_cmd(cmd, host_name)
        cmd = "grep -q '^{} {}'".format(option, filepath)
        status, output = self.__exec_cmd(cmd, host_name)
        if status == 0:
            return False
        return True

    def clean_fe(self, host_name):
        """clean fe
        """
        cmd= 'cd %s/fe;rm -rf log palo-meta/image palo-meta/bdb temp' % (self.__fe_path)
        return self.__exec_cmd(cmd, host_name)

    def clean_be(self, host_name):
        """clean be
        """
        data_path = '/* '.join(self.__be_data_path_list) + '/*'
        cmd= 'cd %s/be;rm -rf log unused bin/be.pid %s' % (self.__be_path, data_path)
        return self.__exec_cmd(cmd, host_name)

    def clean_start(self):
        """clean start
        """

        self.stop_all_fe()
        self.stop_all_be()

        master = env_config.master
        follower_tuple = tuple(env_config.follower_list)
        observer_tuple = tuple(env_config.observer_list)
        be_tuple = tuple(env_config.be_list)

        for host_name in (master,) + follower_tuple + observer_tuple:
            self.clean_fe(host_name)
        for host_name in be_tuple:
            self.clean_be(host_name)

        self.start_electable(master,)

        follower_threads = []
        observer_threads = []
        be_threads = []

        for host_name in follower_tuple:
            t = threading.Thread(target=self.start_follower, args=(host_name,))
            t.start()
            follower_threads.append(t)
        for host_name in observer_tuple:
            t = threading.Thread(target=self.start_observer, args=(host_name,))
            t.start()
            observer_threads.append(t)
        for t in follower_threads:
            t.join()
        for t in observer_threads:
            t.join()

        for host_name in be_tuple:
            t = threading.Thread(target=self.start_be, args=(host_name,))
            t.start()
            be_threads.append(t)
        for t in be_threads:
            t.join()
        
        for host_name in follower_tuple:
            if not self.is_fe_alive(host_name):
                return False
        for host_name in observer_tuple:
            if not self.is_fe_alive(host_name):
                return False
        for host_name in be_tuple:
            if not self.is_be_alive(host_name):
                return False

        if not self.reset_master():
            return False
        client = self.__get_client(self.get_master(), self.get_fe_query_port())
        if not client:
            return False
        be_heartbeat_port = self.get_be_heartbeat_port()
        backend_list = ['%s:%s' % (be, be_heartbeat_port) for be in self.get_be_tuple()]
        return client.add_backend_list(backend_list)

    def check_be(self, host_name, tablet_id):
        """check whether base expansion done
        """
        assert host_name
        data_path = '/data '.join(self.__be_data_path_list) + '/data'
        cmd = 'find %s -name %s_0_*.dat' % (data_path, tablet_id)
        status, output = self.__exec_cmd(cmd, host_name)
        assert not status
        file_name = output.split('/')[-1]
        versions = file_name.split('_')
        if versions[2] > 1:
            return True
        else:
            return False

    def check_ce(self, host_name, tablet_id):
        """check whether cumulative done
        """
        assert host_name
        data_path = '/data '.join(self.__be_data_path_list) + '/data'
        cmd = 'find %s -name %s_*.dat' % (data_path, tablet_id)
        status, output = self.__exec_cmd(cmd, host_name)
        assert not status
        file_path_list = output.split('\r\n')
        for file_path in file_path_list:
            file_name = file_path.split('/')[-1]
            versions = file_name.split('_')
            if versions[1] > 0 and versions[1] != versions[2]:
                return True
        return False
