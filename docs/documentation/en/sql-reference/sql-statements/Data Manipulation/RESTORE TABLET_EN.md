# RESTORE TABLET
Description

This function is used to recover the tablet data that was deleted by mistake in the trash directory.

Note: For the time being, this function only provides an HTTP interface in be service. If it is to be used,
A restore tablet API request needs to be sent to the HTTP port of the be machine for data recovery. The API format is as follows:
Method: Postal
URI: http://be_host:be_http_port/api/restore_tablet?tablet_id=xxx&schema_hash=xxx

'35;'35; example

Curl -X POST "http://hostname:8088 /api /restore" tablet? Tablet id =123456 &schema hash =1111111 "
##keyword
RESTORE,TABLET,RESTORE,TABLET
