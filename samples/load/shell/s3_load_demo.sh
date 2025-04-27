#!/usr/bin/env bash

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

# Run S3 Load tasks in batches.
# Load the directories partitioned by day on S3 into the Doris table according to the partitions.
# Can specify the partition range or a specific date partition, 
# And it supports setting the maximum number of S3 Load tasks to be submitted, so as to control the resource consumption.

#####################################################################
# Description:
# This script is used to load Parquet format data from an S3 bucket into a Doris database.
# It supports batch load by date range or specific dates, automatically controls the number of concurrent tasks,
# and checks for failures after all tasks are completed, making it easy to re-run failed tasks.
#
# How to Use:
# Run: ./s3_load_demo.sh
#
# Configuration Instructions:
# 1. Load Date Range
#    - Modify the START_DATE and END_DATE variables, format is "YYYY-MM-DD"
#    - For example: START_DATE="2025-04-01" END_DATE="2025-04-05"
#
# 2. Load Specific Dates:
#    - Modify the SPECIFIC_DATES array in the script, add the dates to be processed
#    - For example: SPECIFIC_DATES=("2025-04-01" "2025-04-05")
#    - When the SPECIFIC_DATES array is not empty, only these specified dates will be processed
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
# - After all tasks are completed, the Labels of failed tasks will be listed, you can re-run these specific tasks by setting SPECIFIC_DATES
# - If the load fails, you can use the SHOW LOAD command to view detailed error information
#####################################################################

# Specify the partition range to import
START_DATE="2025-04-08"
END_DATE="2025-04-10"

# Specify a particular partitioned array. When the task fails, 
# you can separately specify this partition for rerunning.
SPECIFIC_DATES=()

# Doris connection configuration
DORIS_HOST="127.0.0.1"
DORIS_QUERY_PORT="9030"
DORIS_USER="root"
DORIS_PASSWORD=""
DORIS_DATABASE="testdb"
DORIS_TABLE="sales_data"

# S3 configuration
S3_PREFIX="s3://mybucket/sales_data"
PROVIDER="S3"
S3_ENDPOINT="s3.ap-southeast-1.amazonaws.com"
S3_REGION="ap-southeast-1"
S3_ACCESS_KEY="ak"
S3_SECRET_KEY="sk"

# Maximum number of concurrent tasks
MAX_RUNNING_JOB=10

# Interval for checking whether the S3 Load task is completed
CHECK_INTERVAL=10

# Label prefix for each task load
LABEL_PREFIX="label"

# Generate the date array to process
generate_dates() {
    if [ ${#SPECIFIC_DATES[@]} -gt 0 ]; then
        DATES=()
        for date in "${SPECIFIC_DATES[@]}"; do
            DATES+=("$date")
        done
        
        echo "Running with specified partitions: ${SPECIFIC_DATES[*]}"
    else
        echo "Starting load for partition range: $START_DATE to $END_DATE"
        
        # Build date array
        DATES=()
        current_date="$START_DATE"
        while [ "$(date -d "$current_date" +%s)" -le "$(date -d "$END_DATE" +%s)" ]; do
            DATES+=("$current_date")
            current_date=$(date -I -d "$current_date + 1 day")
        done
    fi
}

# Common query function
run_query() {
    mysql -h ${DORIS_HOST} -P ${DORIS_QUERY_PORT} -u ${DORIS_USER} -p${DORIS_PASSWORD} ${DORIS_DATABASE} -N -e "USE ${DORIS_DATABASE}; $1"
}

# Get task count by state
get_task_count() {
    run_query "SHOW LOAD WHERE state='$1' and label like '${LABEL_PREFIX}_sales_data_%'" | wc -l
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
    local current_date="$1"
    local label="${LABEL_PREFIX}_sales_data_${current_date//-/_}"
    local s3_path="${S3_PREFIX}/${current_date}/*"

    echo "Starting load for ${label}"
    
    # Build S3 LOAD query
    local sql=$(cat <<EOF
USE ${DORIS_DATABASE};
LOAD LABEL ${label}
(
    DATA INFILE("${s3_path}")
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
    local failed_tasks=$(run_query "SHOW LOAD WHERE state='CANCELLED' and label like '${LABEL_PREFIX}_sales_data_%'")

    if [ -n "$failed_tasks" ]; then
        echo "Failed load tasks:"

        # Process each line of results
        echo "$failed_tasks" | while read -r line; do
            # Extract Label (2nd column)
            local label=$(echo "$line" | awk '{print $2}')        

            printf "$label\n"
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
    # Generate the date list to load
    generate_dates
    
    # Submit load tasks for each date
    for current_date in "${DATES[@]}"; do
        submit_load_job "$current_date"
    done
    
    # Wait for all tasks to complete
    wait_for_all_tasks
    
    # Check for failed tasks
    check_failed_tasks
    exit $?
}

# Execute main function
main