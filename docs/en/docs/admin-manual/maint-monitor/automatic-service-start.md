---
{
    "title": "Automated Service Startup",
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

# Automatic service start

This document describes how to configure the automatic pull-up of the Doris cluster to ensure that services are not pulled up in time after service breaks down due to special circumstances in the production environment.

The automatic pull-up service of FE and BE must be configured after the Doris cluster is completely set up.

## Systemd Configures the Doris service

For details about systemd usage and parameter parsing, see [here](https://systemd.io/) 

### sudo permission control

sudo permissions are required to control the doris service using systemd. To ensure the minimum granularity of sudo permission assignment, you can assign the systemd control permission of doris-fe and doris-be services to specified non-root users. Configure the systemctl management permission for doris-fe and doris-be in visudo.

```
Cmnd_Alias DORISCTL=/usr/bin/systemctl start doris-fe,/usr/bin/systemctl stop doris-fe,/usr/bin/systemctl start doris-be,/usr/bin/systemctl stop doris-be

## Allow root to run any commands anywhere
root    ALL=(ALL)       ALL
doris   ALL=(ALL)       NOPASSWD:DORISCTL
```

### Configuration procedure

1. You should config the "JAVA_HOME" variable in the config file, both fe.conf and be.conf, or you can't use the command "systemctl start" to start doris
   ```
   echo "JAVA_HOME=your_java_home" >> /home/doris/fe/conf/fe.conf
   echo "JAVA_HOME=your_java_home" >> /home/doris/be/conf/be.conf
   ```
2. Download the doris-fe.service file: [doris-fe.service](https://github.com/apache/doris/blob/master/tools/systemd/doris-fe.service)

3. The details of doris-fe.service are as follows:

    ```
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

    [Unit]
    Description=Doris FE
    After=network-online.target
    Wants=network-online.target

    [Service]
    Type=forking
    User=root
    Group=root
    LimitCORE=infinity
    LimitNOFILE=200000
    Restart=on-failure
    RestartSec=30
    StartLimitInterval=120
    StartLimitBurst=3
    KillMode=none
    ExecStart=/home/doris/fe/bin/start_fe.sh --daemon 
    ExecStop=/home/doris/fe/bin/stop_fe.sh

    [Install]
    WantedBy=multi-user.target
    ```

#### Matters needing attention

- ExecStart and ExecStop are configured based on actual fe paths

4. Download the doris-be.service file : [doris-be.service](https://github.com/apache/doris/blob/master/tools/systemd/doris-be.service)

5. The details of doris-be.service are as follows: 
    ```
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

    [Unit]
    Description=Doris BE
    After=network-online.target
    Wants=network-online.target

    [Service]
    Type=forking
    User=root
    Group=root
    LimitCORE=infinity
    LimitNOFILE=200000
    Restart=on-failure
    RestartSec=30
    StartLimitInterval=120
    StartLimitBurst=3
    KillMode=none
    ExecStart=/home/doris/be/bin/start_be.sh --daemon
    ExecStop=/home/doris/be/bin/stop_be.sh

    [Install]
    WantedBy=multi-user.target
    ```

#### Matters needing attention

- ExecStart and ExecStop are configured based on actual be paths

6. Service configuration

   Place doris-fe.service and doris-be.service in the /usr/lib/systemd/system directory

7. Set self-start

    After you add or modify the configuration file, you need to reload it

    ```
    systemctl daemon-reload
    ```

    Set the start, the essence is in the/etc/systemd/system/multi - user. Target. Wants/add service file link

    ```
    systemctl enable doris-fe
    systemctl enable doris-be
    ```

8. Service initiation

    ```
    systemctl start doris-fe
    systemctl start doris-be
    ```

## Supervisor configures the Doris service

Supervisor Specific use and parameter analysis can be referred to [here](http://supervisord.org/)

Supervisor configuration automatically pulls up the supervisor configuration. You can install the supervisor directly using the yum command or manually using pip. The pip manual installation process is complicated, and only the yum deployment mode is displayed.Manual deployment refer to [here] (http://supervisord.org/installing.html) for installation deployment.

### Configuration procedure

1. yum Install supervisor
    
    ```
    yum install epel-release
    yum install -y supervisor
    ```

2. Start the service and view the status

    ```
    systemctl enable supervisord # bootstrap
    systemctl start supervisord # Start the supervisord service
    systemctl status supervisord # Check the supervisord service status
    ps -ef|grep supervisord # Check whether the supervisord process exists
    ```

3. Configure BE process management

    ```
    Modify the start_be.sh script remove the last symbol &

    vim /path/doris/be/bin/start_be.sh
    Take this code : nohup $LIMIT ${DORIS_HOME}/lib/palo_be "$@" >> $LOG_DIR/be.out 2>&1 </dev/null &
    Be changed to : nohup $LIMIT ${DORIS_HOME}/lib/palo_be "$@" >> $LOG_DIR/be.out 2>&1 </dev/null
    ```

    Create a supervisor process management configuration file for the BE

    ```
    vim /etc/supervisord.d/doris-be.ini

    [program:doris_be]      
    process_name=%(program_name)s      
    directory=/path/doris/be/be
    command=sh /path/doris/be/bin/start_be.sh
    autostart=true
    autorestart=true
    user=root
    numprocs=1
    startretries=3
    stopasgroup=true
    killasgroup=true
    startsecs=5
    #redirect_stderr = true
    #stdout_logfile_maxbytes = 20MB
    #stdout_logfile_backups = 10
    #stdout_logfile=/var/log/supervisor-palo_be.log
    ```

4. Configure FE process management

    ```
    Modify the start_fe.sh script remove the last symbol &

    vim /path/doris/fe/bin/start_fe.sh 
    Take this code : nohup $LIMIT $JAVA $final_java_opt org.apache.doris.PaloFe ${HELPER} "$@" >> $LOG_DIR/fe.out 2>&1 </dev/null &
    Be changed to : nohup $LIMIT $JAVA $final_java_opt org.apache.doris.PaloFe ${HELPER} "$@" >> $LOG_DIR/fe.out 2>&1 </dev/null
    ```

    Create a supervisor process management configuration file for FE

    ```
    vim /etc/supervisord.d/doris-fe.ini

    [program:PaloFe]
    environment = JAVA_HOME="/usr/local/java"
    process_name=PaloFe
    directory=/path/doris/fe
    command=sh /path/doris/fe/bin/start_fe.sh
    autostart=true
    autorestart=true
    user=root
    numprocs=1
    startretries=3
    stopasgroup=true
    killasgroup=true
    startsecs=10
    #redirect_stderr=true
    #stdout_logfile_maxbytes=20MB
    #stdout_logfile_backups=10
    #stdout_logfile=/var/log/supervisor-PaloFe.log
    ```

5. Configure Broker process management

    ```
    Modify the start_broker.sh script remove the last symbol &

    vim /path/apache_hdfs_broker/bin/start_broker.sh
    Take this code : nohup $LIMIT $JAVA $JAVA_OPTS org.apache.doris.broker.hdfs.BrokerBootstrap "$@" >> $BROKER_LOG_DIR/apache_hdfs_broker.out 2>&1 </dev/null &
    Be changed to : nohup $LIMIT $JAVA $JAVA_OPTS org.apache.doris.broker.hdfs.BrokerBootstrap "$@" >> $BROKER_LOG_DIR/apache_hdfs_broker.out 2>&1 </dev/null
    ```

    Create the supervisor process management profile for the Broker

    ```
    vim /etc/supervisord.d/doris-broker.ini

    [program:BrokerBootstrap]
    environment = JAVA_HOME="/usr/local/java"
    process_name=%(program_name)s
    directory=/path/apache_hdfs_broker
    command=sh /path/apache_hdfs_broker/bin/start_broker.sh
    autostart=true
    autorestart=true
    user=root
    numprocs=1
    startretries=3
    stopasgroup=true
    killasgroup=true
    startsecs=5
    #redirect_stderr=true
    #stdout_logfile_maxbytes=20MB
    #stdout_logfile_backups=10
    #stdout_logfile=/var/log/supervisor-BrokerBootstrap.log
    ```

6. First determine whether the Doris service is stopped, then use supervisor to automatically pull up Doris, and then determine whether the process starts normally
    
    ```
    supervisorctl reload # Reload all the Supervisor configuration files
    supervisorctl status # Check the supervisor status and verify that the Doris service process starts normally

    其他命令 : 
    supervisorctl start all # supervisorctl start It is capable of opening processes
    supervisorctl stop doris-be # The process is supervisorctl stop
    ```

#### Matters needing attention:

- If the supervisor installed using yum starts, an error occurs:  pkg_resources.DistributionNotFound: The 'supervisor==3.4.0' distribution was not found

```
supervisor installed directly using the yum command only supports python2,Therefore, the file contents in /usr/bin/supervisorctl and /usr/bin/supervisorctl should be changed at the beginning Change #! /usr/bin/python to #! /usr/bin/python2, python2 must be installed
```

- If the supervisor is configured to automatically pull up the Doris process, if the BE node breaks down due to abnormal factors on Doris, the error stack information that should be output to be.out will be intercepted by the supervisor. We need to look it up in supervisor's log for further analysis. 
























