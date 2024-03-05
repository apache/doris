-- set those session variables before run cloud p0 regression
set global insert_visible_timeout_ms=60000;
set global enable_auto_analyze=false;
set global enable_audit_plugin=true;
set global enable_memtable_on_sink_node=false;
set global enable_pipeline_x_engine=false;