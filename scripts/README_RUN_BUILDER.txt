Usage:
  # set proxy variables then run
  PROXY_HOST=10.41.158.189 PROXY_PORT=7897 /home/zhisheng/doris/scripts/run_doris_builder.sh

Notes:
  - The script uses sudo to build/run the container. Provide your password when prompted.
  - If you prefer to run docker without sudo, add your user to the docker group and re-login.
  - Logs will be written into the repo: build_thirdparty.log and build_be.log
