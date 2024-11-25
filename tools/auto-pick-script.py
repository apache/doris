#!/usr/bin/env python3
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

import os
import sys
from github import Github
import subprocess

# Get GitHub personal access token and other parameters
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME', 'apache/doris')  # Default repository name
CONFLICT_LABEL = os.getenv('CONFLICT_LABEL', 'cherry-pick-conflict')  # Conflict label from environment variable

# Check if the required command-line arguments are provided
if len(sys.argv) != 3:
    print("Usage: python script.py <PR_NUMBER> <TARGET_BRANCH>")
    sys.exit(1)

pr_number = int(sys.argv[1])  # PR number from command-line argument
TARGET_BRANCH = sys.argv[2]    # Target branch from command-line argument

# Create GitHub instance
g = Github(GITHUB_TOKEN)
repo = g.get_repo(REPO_NAME)

# Get the specified PR
pr = repo.get_pull(pr_number)

# Check if the PR has been merged
if not pr.merged:
    print(f"PR #{pr_number} has not been merged yet.")
    exit(1)

merge_commit_sha = pr.merge_commit_sha

# Get the latest commit from the target branch
base_branch = repo.get_branch(TARGET_BRANCH)

# Create a new branch for cherry-picking the PR
new_branch_name = f'auto-pick-{pr.number}-{TARGET_BRANCH}'
repo.create_git_ref(ref=f'refs/heads/{new_branch_name}', sha=base_branch.commit.sha)
print(f"Created new branch {new_branch_name} from {TARGET_BRANCH}.")
subprocess.run(["git", "config", "--global", "credential.helper", "store"], check=True)

# Clone the repository locally and switch to the new branch
repo_url = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{REPO_NAME}.git"
repo_dir = REPO_NAME.split("/")[-1]  # Get the directory name
#create tmp dir
repo_tmp_dir = f"/tmp/{pr_number}/{TARGET_BRANCH}/"
if os.path.exists(repo_tmp_dir):
    print(f"Directory {repo_tmp_dir} already exists. Deleting it.")
    subprocess.run(["rm", "-rf", repo_tmp_dir])
os.makedirs(repo_tmp_dir)
# cd to tmp
os.chdir(repo_tmp_dir)
subprocess.run(["git", "clone", repo_url])

subprocess.run(["git", "checkout", new_branch_name], cwd=repo_dir)

# Set Git user identity for commits
subprocess.run(["git", "config", "user.email", "your-email@example.com"], cwd=repo_dir)
subprocess.run(["git", "config", "user.name", "Your Name"], cwd=repo_dir)


# Execute the cherry-pick operation
try:
    subprocess.run(["git", "cherry-pick", merge_commit_sha], cwd=repo_dir, check=True)
    print(f"Successfully cherry-picked commit {merge_commit_sha} into {new_branch_name}.")

    # Check if the commit is present in the new branch
    commit_check = subprocess.run(
        ["git", "rev-list", "--count", f"{merge_commit_sha}"],
        cwd=repo_dir,
        capture_output=True,
        text=True
    )

    if commit_check.returncode == 0 and int(commit_check.stdout.strip()) > 0:
        # Push the new branch
        subprocess.run(["git", "push", "origin", new_branch_name], cwd=repo_dir, check=True)
        print(f"Pushed new branch {new_branch_name} to origin.")

        # Create a new PR for the cherry-picked changes
        new_pr = repo.create_pull(
            title=f"{TARGET_BRANCH}: {pr.title} #{pr.number}",  # Prefix with branch name
            body=f"Cherry-picked from #{pr.number}",  # Keep the original PR body
            head=new_branch_name,
            base=TARGET_BRANCH
        )
        print(f"Created a new PR #{new_pr.number} for cherry-picked changes.")
    else:
        print(f"Commit {merge_commit_sha} was not found in {new_branch_name} after cherry-picking.")

except subprocess.CalledProcessError:
    print(f"Conflict occurred while cherry-picking commit {merge_commit_sha}.")
    # Add conflict label
    pr.add_to_labels(CONFLICT_LABEL)
    print(f"Added label '{CONFLICT_LABEL}' to PR #{pr.number} due to conflict.")