#!/usr/bin/env python
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
#
# Modified from Apache Arrow project.

from __future__ import print_function
import lintutils
from subprocess import PIPE
import argparse
import difflib
import multiprocessing as mp
import sys
from functools import partial


# examine the output of clang-format and if changes are
# present assemble a (unified)patch of the difference
def _check_one_file(filename, formatted):
    with open(filename, "rb") as reader:
        original = reader.read()

    if formatted != original:
        # Run the equivalent of diff -u
        diff = list(difflib.unified_diff(
            original.decode('utf8').splitlines(True),
            formatted.decode('utf8').splitlines(True),
            fromfile=filename,
            tofile="{} (after clang format)".format(
                filename)))
    else:
        diff = None

    return filename, diff

def _check_dir(arguments, source_dir, exclude_globs):
    formatted_filenames = []
    for path in lintutils.get_sources(source_dir, exclude_globs):
            formatted_filenames.append(str(path))

    if arguments.fix:
        if not arguments.quiet:
            print("\n".join(map(lambda x: "Formatting {}".format(x),
                                formatted_filenames)))

        # Break clang-format invocations into chunks: each invocation formats
        # 16 files. Wait for all processes to complete
        results = lintutils.run_parallel([
            [arguments.clang_format_binary, "-style=file", "-i"] + some
            for some in lintutils.chunk(formatted_filenames, 16)
        ])
        for returncode, stdout, stderr in results:
            # if any clang-format reported a parse error, bubble it
            if returncode != 0:
                sys.exit(returncode)

    else:
        # run an instance of clang-format for each source file in parallel,
        # then wait for all processes to complete
        results = lintutils.run_parallel([
            [arguments.clang_format_binary, "-style=file", filename]
            for filename in formatted_filenames
        ], stdout=PIPE, stderr=PIPE)

        checker_args = []
        for filename, res in zip(formatted_filenames, results):
            # if any clang-format reported a parse error, bubble it
            returncode, stdout, stderr = res
            if returncode != 0:
                print(stderr)
                sys.exit(returncode)
            checker_args.append((filename, stdout))

        error = False
        pool = mp.Pool()
        try:
            # check the output from each invocation of clang-format in parallel
            for filename, diff in pool.starmap(_check_one_file, checker_args):
                if not arguments.quiet:
                    print("Checking {}".format(filename))
                if diff:
                    print("{} had clang-format style issues".format(filename))
                    # Print out the diff to stderr
                    error = True
                    # pad with a newline
                    print(file=sys.stderr)
                    sys.stderr.writelines(diff)
        except Exception:
            error = True
            raise
        finally:
            pool.terminate()
            pool.join()
        sys.exit(1 if error else 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs clang-format on all of the source "
        "files. If --fix is specified enforce format by "
        "modifying in place, otherwise compare the output "
        "with the existing file and output any necessary "
        "changes as a patch in unified diff format")
    parser.add_argument("--clang_format_binary",
                        required=True,
                        help="Path to the clang-format binary")
    parser.add_argument("--exclude_globs",
                        help="Filename containing globs for files "
                        "that should be excluded from the checks")
    parser.add_argument("--source_dirs",
                        required=True,
                        help="Comma-separated root directories of the source code")
    parser.add_argument("--fix", default=False,
                        action="store_true",
                        help="If specified, will re-format the source "
                        "code instead of comparing the re-formatted "
                        "output, defaults to %(default)s")
    parser.add_argument("--quiet", default=False,
                        action="store_true",
                        help="If specified, only print errors")
    arguments = parser.parse_args()

    exclude_globs = []
    if arguments.exclude_globs:
        with open(arguments.exclude_globs) as f:
            exclude_globs.extend(line.strip() for line in f)

    for source_dir in arguments.source_dirs.split(','):
        if len(source_dir) > 0:
            _check_dir(arguments, source_dir, exclude_globs)
