# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import command
import utils


def parse_args():
    ap = argparse.ArgumentParser(description="")
    args_parsers = ap.add_subparsers(dest="command")
    for cmd in command.ALL_COMMANDS:
        cmd.add_parser(args_parsers)

    return ap.parse_args(), ap.format_help()


def run(args, help):
    timer = utils.Timer()
    for cmd in command.ALL_COMMANDS:
        if args.command == cmd.name:
            return cmd.run(args)
    timer.cancel()
    print(help)
    return -1


def main():
    args, help = parse_args()
    run(args, help)


if __name__ == '__main__':
    main()
