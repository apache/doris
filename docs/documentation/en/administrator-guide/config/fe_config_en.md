# Configuration

## brpc_max_body_size

  This configuration is mainly used to modify the parameter max_body_size of brpc. The default configuration is 64M. It usually occurs in multi distinct + no group by + exceeds 1t data. In particular, if you find that the query is stuck, and be appears the word "body size is too large" in log.

  Because this is a brpc configuration, users can also directly modify this parameter on-the-fly by visiting ```http://host:brpc_port/flags```
