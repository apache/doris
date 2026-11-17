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

"""Resolve module candidates from maintainer overrides or historical evidence."""

import datetime as dt


def eligible_candidates(module, now):
    if "reviewer_override" in module:
        history = {person["login"].lower(): person for person in module["candidates"]}
        empty = {"review_prs": 0, "approval_prs": 0, "merge_prs": 0,
                 "last_review": "", "review_examples": []}
        return [{**empty, **history.get(login.lower(), {}), "login": login,
                 "confidence": "maintainer", "selection_source": "maintainer_override",
                 "preference_weight": 1 / (rank + 1)}
                for rank, login in enumerate(module["reviewer_override"])]
    return [{**person, "selection_source": "history"}
            for person in module["candidates"] if person["auto_eligible"]
            and (now - dt.datetime.fromisoformat(person["last_review"].replace("Z", "+00:00"))).total_seconds() <= 180 * 86400]


def preference_weight(person, first_candidate):
    if person["selection_source"] == "maintainer_override":
        return person["preference_weight"]
    return person["score"] / first_candidate["score"]
