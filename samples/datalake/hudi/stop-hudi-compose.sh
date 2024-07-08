#!/usr/bin/env bash

docker compose -f hudi-compose.yml --env-file hudi-compose.env down
