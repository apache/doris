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

