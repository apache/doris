# // meta_service
brpc_listen_port = 5000
brpc_num_threads = -1
brpc_idle_timeout_sec = 30
# fdb_cluster = xxx:yyy@127.0.0.1:4500
fdb_cluster = hF4lEbxB:VI87tJPu@127.0.0.1:4500
fdb_cluster_file_path = ./conf/fdb.cluster
http_token = greedisgood9999

# // doris txn config
label_keep_max_second = 259200
expired_txn_scan_key_nums = 1000

# // logging
log_dir = ./log/
# info warn error
log_level = info
log_size_mb = 1024
log_filenum_quota = 10
log_immediate_flush = false
# log_verbose_modules = *

# // recycler config
recycle_interval_seconds = 3600
retention_seconds = 259200
recycle_concurrency = 16
# recycle_whitelist =
# recycle_blacklist =

# //max stage num
max_num_stages = 40
