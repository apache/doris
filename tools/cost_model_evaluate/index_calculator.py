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

import math
from typing import List, Tuple
import unittest

# The index is motivated by Testing the Accuracy of Query Optimizers


class IndexCalculator:
    def __init__(self, cost_time_list: List[Tuple[float, float]]) -> None:
        self.cost_time_list = cost_time_list
        sorted(self.cost_time_list, key=lambda t: t[0])
        self.max_c = max(self.cost_time_list, key=lambda ct: ct[0])[0]
        self.min_c = min(self.cost_time_list, key=lambda ct: ct[0])[0]
        self.max_t = max(self.cost_time_list, key=lambda ct: ct[1])[1]
        self.min_t = min(self.cost_time_list, key=lambda ct: ct[1])[1]

    def calculate(self) -> float:

        l = len(self.cost_time_list)
        score = 0.0
        for j in range(0, l):
            for i in range(0, j):
                score += self.weight(i)*self.weight(j) * \
                    self.distance(i, j)*self.sgn(i, j)
        return score

    def weight(self, i: int) -> float:
        return self.cost_time_list[0][0]/self.cost_time_list[i][0]

    def distance(self, i: int, j: int) -> float:
        d0 = (self.cost_time_list[i][0] - self.cost_time_list[j]
              [0])/(self.max_c - self.min_c + 0.00001)
        d1 = (self.cost_time_list[i][1] - self.cost_time_list[j]
              [1])/(self.max_t - self.min_t + 0.00001)

        return math.sqrt(d0*d0 + d1*d1)

    def sgn(self, i: int, j: int) -> float:
        if self.cost_time_list[j][1] - self.cost_time_list[i][1] >= 0:
            return 1
        else:
            return -1


class Test(unittest.TestCase):
    def test(self):
        idx_cal = IndexCalculator([(1, 2), (2, 3)])
        self.assertEqual(round(idx_cal.calculate(), 2), 0.71)


if __name__ == '__main__':
    unittest.main()
