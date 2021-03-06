---
{
    "title": "Metadata Design Document",
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


# Metadata Design Document

## Noun Interpretation

* FE: Frontend, the front-end node of Doris. Mainly responsible for receiving and returning client requests, metadata, cluster management, query plan generation and so on.
* BE: Backend, the back-end node of Doris. Mainly responsible for data storage and management, query plan execution and other work.
* bdbje: [Oracle Berkeley DB Java Edition](http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html). In Doris, we use bdbje to persist metadata operation logs and high availability of FE.

## Overall architecture
![](/images/palo_architecture.jpg)

As shown above, Doris's overall architecture is divided into two layers. Multiple FEs form the first tier, providing lateral expansion and high availability of FE. Multiple BEs form the second layer, which is responsible for data storage and management. This paper mainly introduces the design and implementation of metadata in FE layer.

1. There are two different kinds of FE nodes: follower and observer. Leader election and data synchronization are taken among FE nodes by bdbje ([BerkeleyDB Java Edition](http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/overview/index-093405.html)).

2. The follower node is elected, and one of the followers becomes the leader node, which is responsible for the writing of metadata. When the leader node goes down, other follower nodes re-elect a leader to ensure high availability of services.

3. The observer node only synchronizes metadata from the leader node and does not participate in the election. It can be scaled horizontally to provide the extensibility of metadata reading services.

> Note: The concepts of follower and observer corresponding to bdbje are replica and observer. You may use both names below.

## Metadata structure

Doris's metadata is in full memory. A complete metadata image is maintained in each FE memory. Within Baidu, a cluster of 2,500 tables and 1 million fragments (3 million copies) occupies only about 2GB of metadata in memory. (Of course, the memory overhead for querying intermediate objects and various job information needs to be estimated according to the actual situation. However, it still maintains a low memory overhead.

At the same time, metadata is stored in the memory as a whole in a tree-like hierarchical structure. By adding auxiliary structure, metadata information at all levels can be accessed quickly.

The following figure shows the contents stored in Doris meta-information.

![](/images/metadata_contents.png)

As shown above, Doris's metadata mainly stores four types of data:

1. User data information. Including database, table Schema, fragmentation information, etc.
2. All kinds of job information. For example, import jobs, Clone jobs, SchemaChange jobs, etc.
3. User and permission information.
4. Cluster and node information.

## Data stream

![](/images/metadata_stream.png)

The data flow of metadata is as follows:

1. Only leader FE can write metadata. After modifying leader's memory, the write operation serializes into a log and writes to bdbje in the form of key-value. The key is a continuous integer, and as log id, value is the serialized operation log.

2. After the log is written to bdbje, bdbje copies the log to other non-leader FE nodes according to the policy (write most/write all). The non-leader FE node modifies its metadata memory image by playback of the log, and completes the synchronization with the metadata of the leader node.

3. When the number of log bars of the leader node reaches the threshold (default 10W bars), the checkpoint thread is started. Checkpoint reads existing image files and subsequent logs and replays a new mirror copy of metadata in memory. The copy is then written to disk to form a new image. The reason for this is to regenerate a mirror copy instead of writing an existing image to an image, mainly considering that the write operation will be blocked during writing the image plus read lock. So every checkpoint takes up twice as much memory space.

4. After the image file is generated, the leader node notifies other non-leader nodes that a new image has been generated. Non-leader actively pulls the latest image files through HTTP to replace the old local files.

5. The logs in bdbje will be deleted regularly after the image is completed.

## Implementation details

### Metadata catalogue

1. The metadata directory is specified by the FE configuration item `meta_dir'.

2. Data storage directory for bdbje under `bdb/` directory.

3. The storage directory for image files under the `image/` directory.

* `Image.[logid]`is the latest image file. The suffix `logid` indicates the ID of the last log contained in the image.
* `Image.ckpt` is the image file being written. If it is successfully written, it will be renamed `image.[logid]` and replaced with the original image file.
* The`cluster_id` is recorded in the `VERSION` file. `Cluster_id` uniquely identifies a Doris cluster. It is a 32-bit integer randomly generated at the first startup of leader. You can also specify a cluster ID through the Fe configuration item `cluster_id'.
* The role of FE itself recorded in the `ROLE` file. There are only `FOLLOWER` and `OBSERVER`. Where `FOLLOWER` denotes FE as an optional node. (Note: Even the leader node has a role of `FOLLOWER`)

### Start-up process

1. FE starts for the first time. If the startup script does not add any parameters, it will try to start as leader. You will eventually see `transfer from UNKNOWN to MASTER` in the FE startup log.

2. FE starts for the first time. If the `-helper` parameter is specified in the startup script and points to the correct leader FE node, the FE first asks the leader node about its role (ROLE) and cluster_id through http. Then pull up the latest image file. After reading image file and generating metadata image, start bdbje and start bdbje log synchronization. After synchronization is completed, the log after image file in bdbje is replayed, and the final metadata image generation is completed.

	> Note 1: When starting with the `-helper` parameter, you need to first add the FE through the leader through the MySQL command, otherwise, the start will report an error.

	> Note 2: `-helper` can point to any follower node, even if it is not leader.

	> Note 3: In the process of synchronization log, the Fe log will show `xxx detached`. At this time, the log pull is in progress, which is a normal phenomenon.

3. FE is not the first startup. If the startup script does not add any parameters, it will determine its identity according to the ROLE information stored locally. At the same time, according to the cluster information stored in the local bdbje, the leader information is obtained. Then read the local image file and the log in bdbje to complete the metadata image generation. (If the roles recorded in the local ROLE are inconsistent with those recorded in bdbje, an error will be reported.)

4. FE is not the first boot, and the `-helper` parameter is specified in the boot script. Just like the first process started, the leader role is asked first. But it will be compared with the ROLE stored by itself. If they are inconsistent, they will report errors.

#### Metadata Read-Write and Synchronization

1. Users can use Mysql to connect any FE node to read and write metadata. If the connection is a non-leader node, the node forwards the write operation to the leader node. When the leader is successfully written, it returns a current and up-to-date log ID of the leader. Later, the non-leader node waits for the log ID it replays to be larger than the log ID it returns to the client before returning the message that the command succeeds. This approach guarantees Read-Your-Write semantics for any FE node.

	> Note: Some non-write operations are also forwarded to leader for execution. For example, `SHOW LOAD` operation. Because these commands usually need to read the intermediate states of some jobs, which are not written to bdbje, there are no such intermediate states in the memory of the non-leader node. (FE's direct metadata synchronization depends entirely on bdbje's log playback. If a metadata modification operation does not write bdbje's log, the result of the modification of the operation will not be seen in other non-leader nodes.)

2. The leader node starts a TimePrinter thread. This thread periodically writes a key-value entry for the current time to bdbje. The remaining non-leader nodes read the recorded time in the log by playback and compare it with the local time. If the lag between the local time and the local time is found to be greater than the specified threshold (configuration item: `meta_delay_toleration_second`). If the write interval is half of the configuration item, the node will be in the **unreadable** state. This mechanism solves the problem that non-leader nodes still provide outdated metadata services after a long time of leader disconnection.

3. The metadata of each FE only guarantees the final consistency. Normally, inconsistent window periods are only milliseconds. We guarantee the monotonous consistency of metadata access in the same session. But if the same client connects different FEs, metadata regression may occur. (But for batch update systems, this problem has little impact.)

### Downtime recovery

1. When the leader node goes down, the rest of the followers will immediately elect a new leader node to provide services.
2. Metadata cannot be written when most follower nodes are down. When metadata is not writable, if a write operation request occurs, the current process is that the **FE process exits**. This logic will be optimized in the future, and read services will still be provided in the non-writable state.
3. The downtime of observer node will not affect the state of any other node. It also does not affect metadata reading and writing at other nodes.
