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
import sys
import traceback
import utils


def parse_args():
    ap = argparse.ArgumentParser(description="")
    args_parsers = ap.add_subparsers(dest="command")
    for cmd in command.ALL_COMMANDS:
        cmd.add_parser(args_parsers)

    return ap.parse_args(), ap.format_help()


def run(args, help):
    for cmd in command.ALL_COMMANDS:
        if args.command == cmd.name:
            timer = utils.Timer()
            result = cmd.run(args)
            if utils.is_enable_log():
                timer.show()
            return result
    print(help)
    return ""


def main():
    args, help = parse_args()
    if getattr(args, "output_json", False):
        utils.set_enable_log(False)
    return run(args, help)


if __name__ == '__main__':
    try:
        data = main()
        if not utils.is_enable_log():
            print(utils.pretty_json({"code": 0, "data": data}))
    except:
        if not utils.is_enable_log():
            print(utils.pretty_json({
                "code": 1,
                "err": traceback.format_exc()
            }))
        else:
            traceback.print_exception(*sys.exc_info())
        sys.exit(1)
