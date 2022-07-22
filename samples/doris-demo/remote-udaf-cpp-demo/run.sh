PATH=/home/clz/baidu/bdg/doris/palo-toolchain/ldb_toolchain/bin:$PATH
make clean
make 
patchelf --set-interpreter /opt/compiler/gcc-4.8.2/lib64/ld-linux-x86-64.so.2 ./function_server_demo
patchelf --set-rpath /opt/compiler/gcc-4.8.2/lib64/ ./function_server_demo


./function_server_demo 9000