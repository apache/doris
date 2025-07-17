# Quick start

With newly organized service bootstraper, you just need two steps to start up doris in less than 3 miniutes. Single mode or cluster mode? It's your choice! 

What you need to do is
1. Check the doris config in .env file and change them if neccesary, say newly version controlled by TAG, your own subset used by doris or more memory that should be consumed by fe and be.
2. Start doris in single node mode or cluster mode, that is
   + Start a fe container and a be container. This is sufficient for daily development and verification scenarios.
     ```shell
     docker compose --profile single up -d
     ```
   + Start 3 fe containers and 3 be containers, automatically form a cluster. This allows you to conveniently experience the magic of clusters, and is sufficient to meet the verification needs of POC or staging environments.
     ```shell
     docker compose --profile cluster up -d
     ```

# Detail

The file structure organization is as follows:
```
.
├── .env
├── docker-compose.yaml
└── volume
    ├── doris-meta
    ├── doris-meta2
    ├── doris-meta3
    ├── storage
    ├── storage2
    └── storage3
```

If the profile you are using is single, only one fe and one be file will be mounted, i.e.:
```
.
├── .env
├── docker-compose.yaml
└── volume
    ├── doris-meta
    └── storage
```

## Data Mounting

The volume directory is the root directory for service mounting:
- For the fe service, metadata data is mounted to avoid metadata loss after service restart.
- For the be service, storage data is mounted to avoid data file loss after service restart.

For the doris storage-computation integrated cluster, in addition to the computing resources available to each node itself, such as the number of CPU cores and memory capacity allocated to each instance, the security and stability of data storage are also very important considerations.

Given the particularity of this deployment solution, that is, all service instances run on a single server, you can refer to practical suggestions and minio's deployment strategy for single-machine multi-disk. It is recommended to mount a separate ssd hardware device for each service instance to maximize the fault tolerance of cluster files and data access efficiency.

## .env

Configuration information shared by multiple nodes can be extracted into this file for convenient unified management and use. Currently, it contains the following configuration items:
- `TAG`, the version number for deploying doris
- `COMPOSE_PROJECT_NAME`, specifies a name for the currently running `docker compose`. If it is empty, `docker compose` will default to using the parent directory name of the current path.
- `DORIS_SUBSET`, sets a subnet for doris cluster access, and since doris needs to specify priority_networks in a multi-network card environment, this configuration is also used as priority_networks.
- `MASTER_FE`, specifies master node information for the cluster to facilitate the joining of fe and be.
- `FE_JAVA_OPT` and `BE_JAVA_OPT` set the initial startup settings for the fe and be services respectively, mainly focusing on the setting of`-Xmx`, which defaults to `2G` and can be adjusted as needed.

Please note that although doris supports automatically using the values of environment variables to override the default configurations in fe.conf and be.conf, when defining environment variable names, please use the DORIS_ prefix, like this:
```yaml
environment:
  - MASTER_FE=${MASTER_FE} # Takes effect only in the bootstrap script and will not be written to the conf file
  - DORIS_ENABLE_FQDN_MODE=true # Takes effect not only in the bootstrap script but also will be written to the conf file to override the original configuration with the same name
```
With this feature, you can conveniently and efficiently override or add configuration values in each service without having to mount different configuration files for each service. When the bootstrap script matches environment variable names with configuration names in the conf files, it follows the following rules:
> Remove the DORIS_ prefix from the environment variable, then compare it with the configuration name in the conf file. If they are the same, the configuration in the file will be overridden; otherwise, a new configuration will be added.

# Others

1. For cross-cluster access, such as when a Flink cluster uses flink-connector-doris to access a doris cluster, please configure the hostname and ip mapping of each be node in the flink cluster. The reason is that the internal communication addresses and port numbers of the be nodes are registered in the doris cluster, and flink-connector-doris needs to communicate directly with the be nodes as mentioned in [this document](https://doris.apache.org/docs/3.0/ecosystem/flink-doris-connector#reading-data-from-doris).
2. [Refs this](https://doris.apache.org/docs/3.0/admin-manual/trouble-shooting/metadata-operation?_highlight=http_port#replacement-of-fe-port), for a multi-node fe cluster, the internal communication ports of each node must be consistent, such as 8030. If you modify the port of one node, all nodes must be adjusted consistently. This means you cannot map all ports to the host with the same port number like you can with modifying the port numbers of be nodes.
3. This deployment solution does not involve the node management strategy within the doris cluster. Therefore, node scaling and FQDN switching under this deployment method are consistent with other deployment methods. If changes are needed, please operate with caution. For details, see [elastic-expansion](https://doris.apache.org/docs/3.0/admin-manual/cluster-management/elastic-expansion) and [FQDN](https://doris.apache.org/docs/3.0/admin-manual/cluster-management/fqdn).

