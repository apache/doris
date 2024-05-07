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
import requests
import json
import match
import sys

def check_review_pass(pr_num, maintainers_file, token):
  """
  Checks if all necessary files have been reviewed by maintainers.

  Args:
      pr_num (int): PR number.
      maintainers_file (str): Path to maintainers.json.
      token (str): GitHub token.

  Returns:
      bool: True if all files are reviewed, False otherwise.
  """
  headers = {'Authorization': f'{token}'}
  
  # Get PR review information and extract reviewers and statuses
  response = requests.get(f"https://api.github.com/repos/apache/doris/pulls/{pr_num}/reviews?per_page=100", headers=headers)
  reviews = response.json()
  reviewers = [review['user']['login'] for review in reviews]
  statuses = [review['state'] for review in reviews]
  print(reviewers)
  print(statuses)
  # Create a dictionary with the latest status for each reviewer
  latest_statuses = {reviewer: status for reviewer, status in zip(reviewers, statuses)}

  # Create a list of reviewers who have approved
  approves = [reviewer for reviewer, status in latest_statuses.items() if status == 'APPROVED']

  # Get list of changed files
  response = requests.get(f"https://api.github.com/repos/apache/doris/pulls/{pr_num}/files", headers=headers)
  file_changes = response.json()
  file_change_names = [file['filename'] for file in file_changes]

  # Read maintainers.json
  with open(maintainers_file) as f:
      data = json.load(f)
  need_maintainers_review_path = [item['path'] for item in data['paths']]

  # Check if each path's files have been reviewed by a maintainer
  has_maintainer_review = True
  for file in file_change_names:
      path_found = False
      for path_item in data['paths']:
          path = path_item['path']
          maintainers = path_item['maintainers']

          if match.match(file, path):
              path_found = True
              if maintainers:
                  if not any(maintainer in approves for maintainer in maintainers):
                      has_maintainer_review = False
                      break
              else:
                  continue

      if not path_found:
          continue
  print(approves)
  if len(approves) < 2:
      print("PR has not been approved by at least 2 reviewers")
      exit(1)

  return has_maintainer_review

if __name__ == "__main__":

  pr_num = sys.argv[1]
  token = sys.argv[2]
  maintainers_file = 'tools/maintainers/maintainers.json'  # Adjust path if needed

  if check_review_pass(pr_num, maintainers_file, token):
      print("Thanks for your contribution to Doris.")
  else:
      print("PR has file changes that need to be reviewed by maintainers.")
      exit(1)
