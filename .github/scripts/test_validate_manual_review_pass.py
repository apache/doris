#!/usr/bin/env python3

import copy
import unittest

from validate_manual_review_pass import validate


HEAD = "a" * 40


class ManualReviewPassTest(unittest.TestCase):
    def setUp(self) -> None:
        self.event = {
            "action": "created",
            "comment": {"body": f"/force-pass code-review {HEAD}",
                        "user": {"login": "maintainer", "type": "User"}},
            "sender": {"login": "maintainer"},
            "issue": {"number": 123, "pull_request": {"url": "https://api.github.com/repos/apache/doris/pulls/123"}},
        }
        self.pull = {"number": 123, "state": "open", "head": {"sha": HEAD}}
        self.permission = {"permission": "write"}

    def test_authorized_explicit_pass(self) -> None:
        for permission in ("write", "admin"):
            self.assertEqual(HEAD, validate(self.event, self.pull, {"permission": permission}))

    def test_rejects_read_only_and_missing_permission(self) -> None:
        for permission in ({}, {"permission": "read"}, {"permission": "triage"}):
            with self.subTest(permission=permission), self.assertRaises(ValueError):
                validate(self.event, self.pull, permission)

    def test_rejects_edited_deleted_forged_and_bot_comments(self) -> None:
        events = []
        for action in ("edited", "deleted"):
            event = copy.deepcopy(self.event)
            event["action"] = action
            events.append(event)
        event = copy.deepcopy(self.event)
        event["sender"]["login"] = "someone-else"
        events.append(event)
        event = copy.deepcopy(self.event)
        event["comment"]["user"]["type"] = "Bot"
        events.append(event)
        for event in events:
            with self.subTest(event=event), self.assertRaises(ValueError):
                validate(event, self.pull, self.permission)

    def test_rejects_stale_closed_and_wrong_pr(self) -> None:
        pulls = [{**self.pull, "head": {"sha": "b" * 40}},
                 {**self.pull, "state": "closed"}, {**self.pull, "number": 124}]
        for pull in pulls:
            with self.subTest(pull=pull), self.assertRaises(ValueError):
                validate(self.event, pull, self.permission)
        self.event["issue"].pop("pull_request")
        with self.assertRaises(ValueError):
            validate(self.event, self.pull, self.permission)

    def test_requires_exact_command_and_full_sha(self) -> None:
        for body in ("force pass", "/force-pass code-review", "/force-pass code-review aaaaaaa",
                     f"quoted /force-pass code-review {HEAD}",
                     f"/force-pass code-review {HEAD} extra",
                     f"/force-pass code-review {HEAD}\nstate=success"):
            self.event["comment"]["body"] = body
            with self.subTest(body=body), self.assertRaises(ValueError):
                validate(self.event, self.pull, self.permission)

    def test_normalizes_sha_case(self) -> None:
        self.event["comment"]["body"] = f"/force-pass code-review {HEAD.upper()}\n"
        self.assertEqual(HEAD, validate(self.event, self.pull, self.permission))


if __name__ == "__main__":
    unittest.main()
