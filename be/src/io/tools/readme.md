# File Cache Microbenchmark

## Compilation

To compile the project, run the following command:

```bash
./build.sh --clean --file-cache-microbench --be
```

This will generate the `file_cache_microbench` executable in the `apache_doris/output/be/lib` directory.

## Usage

1. Create a deployment directory:
   ```bash
   mkdir {deploy_dir}
   ```

2. Create a configuration directory:
   ```bash
   mkdir {deploy_dir}/conf
   ```

3. Copy the executable to the deployment directory:
   ```bash
   cp -r apache_doris/output/be/lib/file_cache_microbench {deploy_dir}
   ```

4. Copy the configuration file to the configuration directory:
   ```bash
   cp -r apache_doris/output/be/conf/be.conf {deploy_dir}/conf
   ```

5. Edit the configuration file `{deploy_dir}/conf/be.conf` and add the following configuration information:
    ```ini
    enable_file_cache=true
    file_cache_path = [ {"path": "/mnt/disk2/file_cache", "total_size":53687091200, "query_limit": 10737418240}]
    test_s3_resource = "resource"
    test_s3_ak = "ak"
    test_s3_sk = "sk"
    test_s3_endpoint = "endpoint"
    test_s3_region = "region"
    test_s3_bucket = "bucket"
    test_s3_prefix = "prefix"
    ```

6. Change to the deployment directory:
   ```bash
   cd {deploy_dir}
   ```

7. Run the microbenchmark:
   ```bash
   ./file_cache_microbench --port={test_port}
   ```

8. Access the variables:
   ```bash
   bvar http://${ip}:${port}/vars/
   ```

9. Check the logs in `{deploy_dir}/log/`.

## API

### get_help
```
curl "http://localhost:{port}/MicrobenchService/get_help"
```

#### Endpoints:
- **GET /get_job_status/<job_id>**
  - Retrieve the status of a submitted job.
  - Parameters:
    - `job_id`: The ID of the job to retrieve status for.
    - `files` (optional): If provided, returns the associated file records for the job.
      - Example: `/get_job_status/job_id?files=10`

- **GET /list_jobs**
  - List all submitted jobs and their statuses.

- **GET /get_help**
  - Get this help information.

- **GET /file_cache_clear**
  - Clear the file cache with the following query parameters:
    ```json
    {
      "sync": <true|false>,                // Whether to synchronize the cache clear operation
      "segment_path": "<path>"             // Optional path of the segment to clear from the cache
    }
    ```
    If `segment_path` is not provided, all caches will be cleared based on the `sync` parameter.

- **GET /file_cache_reset**
  - Reset the file cache with the following query parameters:
    ```json
    {
      "capacity": <new_capacity>,          // New capacity for the specified path
      "path": "<path>"                     // Path of the segment to reset
    }
    ```

- **GET /file_cache_release**
  - Release the file cache with the following query parameters:
    ```json
    {
      "base_path": "<base_path>"           // Optional base path to release specific caches
    }
    ```

- **GET /update_config**
  - Update the configuration with the following JSON body:
    ```json
    {
      "config_key": "<key>",               // The configuration key to update
      "config_value": "<value>",            // The new value for the configuration key
      "persist": <true|false>              // Whether to persist the configuration change
    }
    ```

- **GET /show_config**
  - Retrieve the current configuration settings.

### Notes:
- Ensure that the S3 configuration is set correctly in the environment.
- The program will create and read files in the specified S3 bucket.
- Monitor the logs for detailed execution information and errors.

### Version Information:
you can see it in get_help return msg

## End-to-End Peer Read Benchmark (Cloud Mode)

To benchmark end-to-end file cache peer read IO performance without affecting existing microbench workflows, start `file_cache_microbench` in cloud mode with an optional `--brpc_port` to host `CloudInternalServiceImpl::fetch_peer_data`.

### 1. Configuration (`be.conf`)

Add the following cloud & peer read settings to both Peer Server and Peer Client `be.conf` (use a fresh `storage_root_path` for cloud mode):

```ini
enable_file_cache = true
file_cache_path = [{"path": "/mnt/disk2/file_cache", "total_size": 53687091200, "query_limit": 10737418240}]
cloud_unique_id = "microbench_cloud"
enable_cache_read_from_peer = true
# Set false for pure peer read, or true (with use_s3_fallback=true) to benchmark peer-vs-S3 race
enable_peer_s3_race = false
```

### 2. Start Peer Server & Peer Client

```bash
# Node A (Peer Server): hosts CloudInternalServiceImpl on brpc_port 19060
DORIS_HOME=/path/to/node_a ./file_cache_microbench --port=18080 --brpc_port=19060

# Node B (Peer Client): reads from Node A via peer read
DORIS_HOME=/path/to/node_b ./file_cache_microbench --port=18081
```

### 3. Populate Peer Server Cache

- **Option A (No S3 required)**: Seed Node A local file cache directly with `job_type: "peer_cache_seed"`:
  ```bash
  curl -X POST "http://<node_a>:18080/MicrobenchService/submit_job" -d '{
    "job_type": "peer_cache_seed",
    "file_prefix": "peer_bench",
    "source_job_id": "seed_1",
    "num_files": 16,
    "size_bytes_perfile": 67108864,
    "num_threads": 16,
    "tablet_id": 1,
    "cache_type": "NORMAL"
  }'
  ```
- **Option B (With S3)**: Run a normal `file_cache` write job (`write_iops > 0`, `write_file_cache: true`) on Node A and record its `job_id`.

### 4. Configure Peer Candidates & Run Peer Read Benchmark on Node B

You can either pass `peer_endpoints` directly in the `peer_read` job, or pre-configure via `/set_peer_candidates`:

```bash
# Optional standalone candidate setup & inspection
curl -X POST "http://<node_b>:18081/MicrobenchService/set_peer_candidates" -d '{
  "tablet_id": 1,
  "peers": [{"host": "<node_a_ip>", "port": 19060, "compute_group_id": "cg1"}]
}'
curl "http://<node_b>:18081/MicrobenchService/get_peer_candidates?tablet_id=1"

# End-to-end peer read via CachedRemoteFileReader -> PeerFileCacheReader -> BRPC -> Node A BlockFileCache
curl -X POST "http://<node_b>:18081/MicrobenchService/submit_job" -d '{
  "job_type": "peer_read",
  "file_prefix": "peer_bench",
  "source_job_id": "seed_1",
  "num_files": 16,
  "size_bytes_perfile": 67108864,
  "num_threads": 16,
  "repeat": 3,
  "read_offset": [0, 1],
  "read_length": [67108864, 67108865],
  "read_chunk_size": 4194304,
  "tablet_id": 1,
  "peer_endpoints": [{"host": "<node_a_ip>", "port": 19060, "compute_group_id": "cg1"}],
  "use_s3_fallback": false,
  "clear_local_cache_before_read": true,
  "clear_local_cache_each_repeat": true,
  "cache_write_mode": "DEFAULT",
  "verify_data": true
}'
```

Key options for `job_type: "peer_read"`:
- `use_s3_fallback`: `false` (default) disables S3 fallback so no S3 config is required and any unexpected fallback fails fast; set `true` to benchmark real S3 fallback or `enable_peer_s3_race = true`.
- `clear_local_cache_each_repeat`: `true` drains pending async cache writes / inflight write buffers and removes the file from Node B local cache before each repeat so `repeat > 1` keeps hitting Peer Server instead of Node B local disk or inflight memory.
- Metrics: `throughput_mb_s` measures application logical bytes delivered; `peer_wire_bytes_read` / `peer_wire_throughput_mb_s` consistently measure actual successful peer RPC payload bytes in both cached and `direct_peer_rpc` modes (excluding failed attempts and losing race responses). `max_read_us` is exact across all ops, while `p50/p95/p99_read_us` use per-worker weighted reservoir sampling (up to 2M samples).
- `cache_write_mode`: `DEFAULT`, `SYNC_WRITE`, or `ASYNC_WRITE` to compare sync vs async local cache writeback on peer reads.
- `direct_peer_rpc`: `true` invokes `PeerFileCacheReader::fetch_blocks` directly (bypassing Client local cache lookup/writeback) to measure raw Peer BRPC + Peer Server disk IO ceiling.
- `verify_data`: `true` (default) verifies every byte against deterministic `DataVerifier` tags; set `false` for pure throughput testing without verification CPU overhead.

### 5. Check Results & Bvars

```bash
curl "http://<node_b>:18081/MicrobenchService/get_job_status/<job_id>"
# Client peer bvars:
curl "http://<node_b>:18081/vars/peer_cache_reader_*"
curl "http://<node_b>:18081/vars/peer_race_*"
# Server peer bvars (on Node A brpc_port):
curl "http://<node_a>:19060/vars/file_cache_get_by_peer_*"
```
