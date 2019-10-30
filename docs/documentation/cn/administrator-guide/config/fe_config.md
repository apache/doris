# 基本配置

## brpc_max_body_size

  这个配置主要用来修改 brpc 的参数 max_body_size ，默认配置是 64M。一般发生在 multi distinct + 无 group by + 超过1T 数据量的情况下。尤其如果发现查询卡死，且 BE 出现类似 body_size is too large 的字样。

  由于这是一个 brpc 的配置，用户也可以在运行中直接修改该参数。通过访问 http://host:brpc_port/flags 修改。
