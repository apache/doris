#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#############################################################################
# Description:
# Load the file list in the object storage into the Doris database in batches.
# Supports splitting the file list on S3 into batches of a certain quantity, and submitting S3 Load tasks for each batch separately.
# Moreover, it supports specifying the ID of a failed batch to run it independently.
# And it supports setting the maximum number of S3 Load tasks to be submitted, so as to control the resource consumption.
#
# Configuration Instructions:
# 1. Batch Load Configuration:
#    - TOTAL_FILES: The total number of files
#    - BATCH_SIZE: The number of files processed in each batch
#    - FILE_PREFIX: The prefix of the file name, which is empty by default
#
# 2. Specify the batch ID for load
#    - By explicitly specifying the value of SPECIFIC_BATCHES, load the file list included in the specified batches.
#    - For example, if SPECIFIC_BATCHES=(0 2), run the 0th batch and the 2nd batch.
#    - After specifying, other batches will be ignored. This can be used when some batches fail to run.
#
# 3. Concurrency Control:
#    - MAX_RUNNING_JOB: Controls the maximum number of concurrently running tasks (default is 10)
#    - CHECK_INTERVAL: Interval time for checking task status, in seconds (default is 10)
#
# 4. Doris Connection Configuration:
#    - DORIS_HOST, DORIS_QUERY_PORT: Doris server address and port
#    - DORIS_USER, DORIS_PASSWORD: Database username and password
#    - DORIS_DATABASE, DORIS_TABLE: Target database and table name
#
# 5. S3 Configuration: 
#    - S3_PREFIX: S3 bucket path prefix
#    - PROVIDER: Object storage service provider, such as S3, AZURE, GCP, etc.
#    - S3_ENDPOINT: The endpoint address of the S3 storage.
#    - S3_REGION: The region where the S3 storage is located.
#    - S3_ACCESS_KEY, S3_SECRET_KEY: S3 access credentials
#    - The script will automatically add the date path after S3_PREFIX, for example: s3://bucket/path/2025-04-01/*
#
# 6. Other Configurations:
#    - LABEL_PREFIX: Label prefix for each task load, used to distinguish different load tasks
#
# Precautions:
# - Ensure that the mysql client tool is installed before execution
# - Ensure that the database table structure matches the S3 data format, the columns used in the example are: order_id, order_date, customer_name, amount, country
# - After all tasks are completed, the Labels of failed tasks will be listed, you can re-run these specific tasks by setting SPECIFIC_BATCHES
# - If the load fails, you can use the SHOW LOAD command to view detailed error information
# - The S3 file name rule of this script is that it consists of 12 digits and increases incrementally. For example, 000000000000.parquet.
#############################################################################

# Batch file load configuration
TOTAL_FILES=3
BATCH_SIZE=1
FILE_PREFIX=""

# Specify batch IDs to run, leave empty to run all batches
# Example: SPECIFIC_BATCHES=(1 2) will only run batch 1 and 2
SPECIFIC_BATCHES=()

# Label prefix for each task load
LABEL_PREFIX="label"

# Doris connection configuration
DORIS_HOST="127.0.0.1"
DORIS_QUERY_PORT="9030"
DORIS_USER="root"
DORIS_PASSWORD=""
DORIS_DATABASE="testdb"
DORIS_TABLE="sales_data"

# S3 configuration
S3_PREFIX="s3://mybucket/export/sales_data"
PROVIDER="GCP"
S3_ENDPOINT="storage.asia-southeast1.rep.googleapis.com"
S3_REGION="asia-southeast1"
S3_ACCESS_KEY=""
S3_SECRET_KEY=""

# Maximum number of concurrent tasks
MAX_RUNNING_JOB=10

# Interval for checking whether the S3 Load task is completed
CHECK_INTERVAL=10

# Common query function
run_query() {
    mysql -h ${DORIS_HOST} -P ${DORIS_QUERY_PORT} -u ${DORIS_USER} -p${DORIS_PASSWORD} ${DORIS_DATABASE} -N -e "USE ${DORIS_DATABASE}; $1"
}

# Get task count by state
get_task_count() {
    run_query "SHOW LOAD WHERE state='$1' and label like '${LABEL_PREFIX}_batch_%'" | wc -l
}

# Check if task count has reached maximum limit(MAX_RUNNING_JOB)
wait_for_available_slots() {
    while true; do
        pending_tasks=$(get_task_count "PENDING")
        etl_tasks=$(get_task_count "ETL")
        loading_tasks=$(get_task_count "LOADING")
    
        running_jobs=$((pending_tasks + etl_tasks + loading_tasks))
        if [ $running_jobs -le $MAX_RUNNING_JOB ]; then
            break
        fi
        
        echo "Current running job: $running_jobs, Exceeding the limit: $MAX_RUNNING_JOB, Retry after ${CHECK_INTERVAL} seconds..."
        sleep $CHECK_INTERVAL
    done
}

# Submit load task
submit_load_job() {
    local batch_id=$1
    local start_file_idx=$2
    local end_file_idx=$3
    local label="${LABEL_PREFIX}_batch_${batch_id}"

    # Check if index range is valid
    if [ $start_file_idx -gt $end_file_idx ]; then
        echo "Warning: File index range for batch ${batch_id} is invalid (${start_file_idx} > ${end_file_idx}), skipping this batch"
        return 0
    fi

    echo "Starting load ${label} (files ${start_file_idx} to ${end_file_idx})" 
    
    # Build file list
    local file_list=""
    for ((i=start_file_idx; i<=end_file_idx; i++)); do
        # Generate filename with padding
        local formatted_idx=$(printf "%012d" $i)
        local file_name="${FILE_PREFIX}${formatted_idx}.parquet"
        
        if [ -n "$file_list" ]; then
            file_list="${file_list},\"${S3_PREFIX}/${file_name}\""
        else
            file_list="\"${S3_PREFIX}/${file_name}\""
        fi
    done
    
    
    # Build S3 LOAD query
    local sql=$(cat <<EOF
USE ${DORIS_DATABASE};
LOAD LABEL ${label}
(
    DATA INFILE(${file_list})
    INTO TABLE ${DORIS_TABLE}
    FORMAT AS "parquet"
    (order_id, order_date, customer_name, amount, country)
)
WITH S3
(
    "provider" = "${PROVIDER}",
    "s3.endpoint" = "${S3_ENDPOINT}",
    "s3.access_key" = "${S3_ACCESS_KEY}",
    "s3.secret_key" = "${S3_SECRET_KEY}",
    "s3.region" = "${S3_REGION}"
);
EOF
)

    mysql -h ${DORIS_HOST} -P ${DORIS_QUERY_PORT} -u ${DORIS_USER} -p${DORIS_PASSWORD} ${DORIS_DATABASE} -e "${sql}"
    echo "Submit load ${label} success"

    wait_for_available_slots
}

wait_for_all_tasks() {
    echo "Waiting for all load tasks to complete..."
    while true; do
        pending_tasks=$(get_task_count "PENDING")
        etl_tasks=$(get_task_count "ETL")
        loading_tasks=$(get_task_count "LOADING")
        
        total_running=$((pending_tasks + etl_tasks + loading_tasks))
        
        if [ $total_running -eq 0 ]; then
            echo "All Loading Job Finished"
            break
        fi
        
        echo "Current Status: PENDING=$pending_tasks, ETL=$etl_tasks, LOADING=$loading_tasks, Retry after ${CHECK_INTERVAL} seconds..."
        sleep $CHECK_INTERVAL
    done
}

check_failed_tasks() {
    echo "Checking for failed load tasks..."
    local failed_tasks=$(run_query "SHOW LOAD WHERE state='CANCELLED' and label like '${LABEL_PREFIX}_batch_%'")

    if [ -n "$failed_tasks" ]; then
        echo "Failed load tasks:"

        # Process each line of results
        echo "$failed_tasks" | while read -r line; do
            # Extract Label (2nd column)
            local label=$(echo "$line" | awk '{print $2}')
            
            # Extract batch ID from label (e.g., label_batch_5 -> 5)
            local batch_id=$(echo "$label" | sed -E 's/.*_batch_([0-9]+)/\1/')
            
            printf "Batch ID: %s, Label: %s\n" "$batch_id" "$label"
        done
        
        echo "Task execution complete, but there are failed tasks. Please check the errors above."
        return 1
    else
        echo "All tasks executed successfully!"
        return 0
    fi
}

# Main function
main() {
    # Calculate number of batches based on TOTAL_FILES
    local batch_count=$(( (TOTAL_FILES + BATCH_SIZE - 1) / BATCH_SIZE ))
    
    echo "Total files: $TOTAL_FILES, batch size: $BATCH_SIZE, calculated batch count: $batch_count"
    
    # Determine which batches to run
    local batches_to_run=()
    if [ ${#SPECIFIC_BATCHES[@]} -gt 0 ]; then
        echo "Running specified batches: ${SPECIFIC_BATCHES[*]}"
        batches_to_run=("${SPECIFIC_BATCHES[@]}")
    else
        echo "Running all batches (total $batch_count)"
        for ((i=0; i<batch_count; i++)); do
            batches_to_run+=($i)
        done
    fi
    
    # Submit load tasks for selected batches
    for batch in "${batches_to_run[@]}"; do
        # Calculate file index range
        local start_file_idx=$((batch * BATCH_SIZE))
        local end_file_idx=$(( (batch + 1) * BATCH_SIZE - 1 ))
        
        # Ensure the batch doesn't exceed total files
        if [ $end_file_idx -ge $TOTAL_FILES ]; then
            end_file_idx=$((TOTAL_FILES - 1))
        fi
        
        # Call function to submit the task
        submit_load_job $batch $start_file_idx $end_file_idx
    done

    # Wait for all tasks to complete
    wait_for_all_tasks
    
    # Check for failed tasks
    check_failed_tasks
    exit $?
}

# Execute main function
main