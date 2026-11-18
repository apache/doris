#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Load and validate the single editable reviewer-routing policy."""

import argparse
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "reviewer-routing.json"
LOGIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"Duplicate configuration key: {key}")
        result[key] = value
    return result


def logins(value, where):
    require(isinstance(value, list), f"{where} must be a list of GitHub logins.")
    require(all(isinstance(p, str) and LOGIN.fullmatch(p) and "--" not in p for p in value),
            f"{where} contains an invalid GitHub login; omit @ prefixes and bot accounts.")
    require(len({p.lower() for p in value}) == len(value), f"{where} contains duplicate logins.")


def pattern(value, where):
    require(isinstance(value, str) and value, f"{where} must be a nonempty regex string.")
    try:
        re.compile(value)
    except re.error as error:
        raise ValueError(f"Invalid regex in {where}: {error}") from error


def load_config(path=CONFIG):
    config = json.loads(Path(path).read_text(), object_pairs_hook=unique_keys)
    require(isinstance(config, dict), "Routing configuration must be an object.")
    require(set(config) <= {"schema_version", "fallback_reviewer", "disabled_reviewers", "modules", "test_routes"},
            "Unknown top-level routing configuration field.")
    require(type(config.get("schema_version")) is int and config["schema_version"] == 1,
            "Routing schema_version must be 1.")
    logins([config.get("fallback_reviewer")], "fallback_reviewer")
    config.setdefault("disabled_reviewers", [])
    config.setdefault("test_routes", [])
    logins(config["disabled_reviewers"], "disabled_reviewers")
    require(config["fallback_reviewer"].lower() not in {p.lower() for p in config["disabled_reviewers"]},
            "fallback_reviewer is disabled; choose another fallback before disabling this account.")
    modules = config.get("modules")
    require(isinstance(modules, dict) and {"other", "test.other"} <= modules.keys(),
            "modules must include the other and test.other fallback categories.")
    for mid, module in modules.items():
        require(re.fullmatch(r"[a-z0-9]+(?:[._-][a-z0-9]+)*", mid), f"Invalid module ID: {mid}")
        require(isinstance(module, dict) and set(module) == {"label", "priority", "patterns", "reviewers"},
                f"{mid} requires exactly label, priority, patterns and reviewers.")
        require(isinstance(module["label"], str) and module["label"], f"{mid}.label must be nonempty.")
        require(type(module["priority"]) is int, f"{mid}.priority must be an integer.")
        require(isinstance(module["patterns"], list), f"{mid}.patterns must be a list.")
        for value in module["patterns"]:
            pattern(value, f"{mid}.patterns")
        logins(module["reviewers"], f"{mid}.reviewers")
    require(all(modules["other"]["priority"] < m["priority"] for mid, m in modules.items() if mid != "other"),
            "The other module must have the lowest priority.")
    require(isinstance(config["test_routes"], list), "test_routes must be a list.")
    for route in config["test_routes"]:
        require(isinstance(route, dict) and set(route) == {"pattern", "module"},
                "Each test_routes entry requires exactly pattern and module.")
        require(isinstance(route["module"], str) and route["module"] in modules,
                f"Unknown test_routes module: {route['module']}")
        pattern(route["pattern"], "test_routes")
    return config


def load_routing(tool=ROOT, config_path=None):
    tool = Path(tool)
    config = load_config(config_path or tool / "reviewer-routing.json")
    disabled = {person.lower() for person in config["disabled_reviewers"]}
    modules = []
    for mid, rule in config["modules"].items():
        active = [login for login in rule["reviewers"] if login.lower() not in disabled]
        candidates = [{"login": login} for login in active]
        modules.append({"id": mid, "label": rule["label"], "reviewers": rule["reviewers"], "candidates": candidates})
    return {"config": config, "modules": modules}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--list", action="store_true")
    args = parser.parse_args()
    try:
        routing = load_routing(config_path=args.config)
    except ValueError as error:
        parser.error(str(error))
    if args.check:
        print(f"Valid routing policy: {len(routing['modules'])} modules; fallback {routing['config']['fallback_reviewer']}.")
    else:
        print("Module\tOrdered active reviewers")
        for module in routing["modules"]:
            print(module["id"] + "\t" + ", ".join(p["login"] for p in module["candidates"]))


if __name__ == "__main__":
    main()
