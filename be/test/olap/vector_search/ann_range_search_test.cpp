// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include "common/object_pool.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/ann_index_iterator.h"
#include "olap/rowset/segment_v2/ann_index_reader.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/virtual_column_iterator.h"
#include "olap/vector_search/vector_search_utils.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nothing.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/functions/functions_comparison.h"

namespace doris::vectorized {
/*
TExpr {
  01: nodes (list) = list<struct>[3] {
    [0] = TExprNode {
      01: node_type (i32) = 2,
      02: type (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 2,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      03: opcode (i32) = 14,
      04: num_children (i32) = 2,
      20: output_scale (i32) = -1,
      26: fn (struct) = TFunction {
        01: name (struct) = TFunctionName {
          02: function_name (string) = "ge",
        },
        02: binary_type (i32) = 0,
        03: arg_types (list) = list<struct>[2] {
          [0] = TTypeDesc {
            01: types (list) = list<struct>[1] {
              [0] = TTypeNode {
                01: type (i32) = 0,
                02: scalar_type (struct) = TScalarType {
                  01: type (i32) = 8,
                },
              },
            },
            03: byte_size (i64) = -1,
          },
          [1] = TTypeDesc {
            01: types (list) = list<struct>[1] {
              [0] = TTypeNode {
                01: type (i32) = 0,
                02: scalar_type (struct) = TScalarType {
                  01: type (i32) = 8,
                },
              },
            },
            03: byte_size (i64) = -1,
          },
        },
        04: ret_type (struct) = TTypeDesc {
          01: types (list) = list<struct>[1] {
            [0] = TTypeNode {
              01: type (i32) = 0,
              02: scalar_type (struct) = TScalarType {
                01: type (i32) = 2,
              },
            },
          },
          03: byte_size (i64) = -1,
        },
        05: has_var_args (bool) = false,
        07: signature (string) = "ge(double, double)",
        11: id (i64) = 0,
        13: vectorized (bool) = true,
        14: is_udtf_function (bool) = false,
        15: is_static_load (bool) = false,
        16: expiration_time (i64) = 360,
      },
      28: child_type (i32) = 8,
      29: is_nullable (bool) = true,
    },
    [1] = TExprNode {
      01: node_type (i32) = 16,
      02: type (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 8,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: num_children (i32) = 0,
      15: slot_ref (struct) = TSlotRef {
        01: slot_id (i32) = 3,
        02: tuple_id (i32) = 0,
        03: col_unique_id (i32) = -1,
        04: is_virtual_slot (bool) = true,
      },
      20: output_scale (i32) = -1,
      29: is_nullable (bool) = true,
      36: label (string) = "dis",
    },
    [2] = TExprNode {
      01: node_type (i32) = 8,
      02: type (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 8,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: num_children (i32) = 0,
      09: float_literal (struct) = TFloatLiteral {
        01: value (double) = 10,
      },
      20: output_scale (i32) = -1,
      29: is_nullable (bool) = false,
    },
  },
}
*/
const std::string ann_range_search_thrift =
        R"xxx({"1":{"lst":["rec",3,{"1":{"i32":2},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"3":{"i32":14},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"ge"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}},{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":2}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"ge(double, double)"},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"28":{"i32":8},"29":{"tf":1}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":3},"2":{"i32":0},"3":{"i32":-1},"4":{"tf":1}}},"20":{"i32":-1},"29":{"tf":1},"36":{"str":"dis"}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":10}}},"20":{"i32":-1},"29":{"tf":0}}]}})xxx";
/*
TDescriptorTable {
  01: slotDescriptors (list) = list<struct>[8] {
    [0] = TSlotDescriptor {
      01: id (i32) = 0,
      02: parent (i32) = 0,
      03: slotType (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 5,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: columnPos (i32) = -1,
      05: byteOffset (i32) = -1,
      06: nullIndicatorByte (i32) = 0,
      07: nullIndicatorBit (i32) = 0,
      08: colName (string) = "siteid",
      09: slotIdx (i32) = 0,
      10: isMaterialized (bool) = true,
      11: col_unique_id (i32) = 0,
      12: is_key (bool) = true,
      13: need_materialize (bool) = true,
      14: is_auto_increment (bool) = false,
      16: col_default_value (string) = "10",
      17: primitive_type (i32) = 5,
    },
    [1] = TSlotDescriptor {
      01: id (i32) = 1,
      02: parent (i32) = 0,
      03: slotType (struct) = TTypeDesc {
        01: types (list) = list<struct>[2] {
          [0] = TTypeNode {
            01: type (i32) = 1,
            04: contains_null (bool) = true,
            05: contains_nulls (list) = list<bool>[1] {
              [0] = true,
            },
          },
          [1] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 7,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: columnPos (i32) = -1,
      05: byteOffset (i32) = -1,
      06: nullIndicatorByte (i32) = 0,
      07: nullIndicatorBit (i32) = -1,
      08: colName (string) = "embedding",
      09: slotIdx (i32) = 3,
      10: isMaterialized (bool) = true,
      11: col_unique_id (i32) = 1,
      12: is_key (bool) = false,
      13: need_materialize (bool) = true,
      14: is_auto_increment (bool) = false,
      17: primitive_type (i32) = 20,
    },
    [2] = TSlotDescriptor {
      01: id (i32) = 2,
      02: parent (i32) = 0,
      03: slotType (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 23,
              02: len (i32) = 2147483643,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: columnPos (i32) = -1,
      05: byteOffset (i32) = -1,
      06: nullIndicatorByte (i32) = 0,
      07: nullIndicatorBit (i32) = 0,
      08: colName (string) = "comment",
      09: slotIdx (i32) = 2,
      10: isMaterialized (bool) = true,
      11: col_unique_id (i32) = 2,
      12: is_key (bool) = false,
      13: need_materialize (bool) = true,
      14: is_auto_increment (bool) = false,
      17: primitive_type (i32) = 23,
    },
    [3] = TSlotDescriptor {
      01: id (i32) = 3,
      02: parent (i32) = 0,
      03: slotType (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 8,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: columnPos (i32) = -1,
      05: byteOffset (i32) = -1,
      06: nullIndicatorByte (i32) = 0,
      07: nullIndicatorBit (i32) = 0,
      08: colName (string) = "",
      09: slotIdx (i32) = 1,
      10: isMaterialized (bool) = true,
      11: col_unique_id (i32) = -1,
      12: is_key (bool) = false,
      13: need_materialize (bool) = true,
      14: is_auto_increment (bool) = false,
      17: primitive_type (i32) = 0,
      18: virtual_column_expr (struct) = TExpr {
        01: nodes (list) = list<struct>[12] {
          [0] = TExprNode {
            01: node_type (i32) = 20,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 2,
            20: output_scale (i32) = -1,
            26: fn (struct) = TFunction {
              01: name (struct) = TFunctionName {
                02: function_name (string) = "l2_distance_approximate",
              },
              02: binary_type (i32) = 0,
              03: arg_types (list) = list<struct>[2] {
                [0] = TTypeDesc {
                  01: types (list) = list<struct>[2] {
                    [0] = TTypeNode {
                      01: type (i32) = 1,
                      04: contains_null (bool) = true,
                      05: contains_nulls (list) = list<bool>[1] {
                        [0] = true,
                      },
                    },
                    [1] = TTypeNode {
                      01: type (i32) = 0,
                      02: scalar_type (struct) = TScalarType {
                        01: type (i32) = 8,
                      },
                    },
                  },
                  03: byte_size (i64) = -1,
                },
                [1] = TTypeDesc {
                  01: types (list) = list<struct>[2] {
                    [0] = TTypeNode {
                      01: type (i32) = 1,
                      04: contains_null (bool) = true,
                      05: contains_nulls (list) = list<bool>[1] {
                        [0] = true,
                      },
                    },
                    [1] = TTypeNode {
                      01: type (i32) = 0,
                      02: scalar_type (struct) = TScalarType {
                        01: type (i32) = 8,
                      },
                    },
                  },
                  03: byte_size (i64) = -1,
                },
              },
              04: ret_type (struct) = TTypeDesc {
                01: types (list) = list<struct>[1] {
                  [0] = TTypeNode {
                    01: type (i32) = 0,
                    02: scalar_type (struct) = TScalarType {
                      01: type (i32) = 8,
                    },
                  },
                },
                03: byte_size (i64) = -1,
              },
              05: has_var_args (bool) = false,
              07: signature (string) = "l2_distance_approximate(array<double>, array<double>)",
              09: scalar_fn (struct) = TScalarFunction {
                01: symbol (string) = "",
              },
              11: id (i64) = 0,
              13: vectorized (bool) = true,
              14: is_udtf_function (bool) = false,
              15: is_static_load (bool) = false,
              16: expiration_time (i64) = 360,
            },
            29: is_nullable (bool) = true,
          },
          [1] = TExprNode {
            01: node_type (i32) = 5,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[2] {
                [0] = TTypeNode {
                  01: type (i32) = 1,
                  04: contains_null (bool) = true,
                  05: contains_nulls (list) = list<bool>[1] {
                    [0] = true,
                  },
                },
                [1] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            03: opcode (i32) = 4,
            04: num_children (i32) = 1,
            20: output_scale (i32) = -1,
            26: fn (struct) = TFunction {
              01: name (struct) = TFunctionName {
                02: function_name (string) = "casttoarray",
              },
              02: binary_type (i32) = 0,
              03: arg_types (list) = list<struct>[1] {
                [0] = TTypeDesc {
                  01: types (list) = list<struct>[2] {
                    [0] = TTypeNode {
                      01: type (i32) = 1,
                      04: contains_null (bool) = true,
                      05: contains_nulls (list) = list<bool>[1] {
                        [0] = true,
                      },
                    },
                    [1] = TTypeNode {
                      01: type (i32) = 0,
                      02: scalar_type (struct) = TScalarType {
                        01: type (i32) = 7,
                      },
                    },
                  },
                  03: byte_size (i64) = -1,
                },
              },
              04: ret_type (struct) = TTypeDesc {
                01: types (list) = list<struct>[2] {
                  [0] = TTypeNode {
                    01: type (i32) = 1,
                    04: contains_null (bool) = true,
                    05: contains_nulls (list) = list<bool>[1] {
                      [0] = true,
                    },
                  },
                  [1] = TTypeNode {
                    01: type (i32) = 0,
                    02: scalar_type (struct) = TScalarType {
                      01: type (i32) = 8,
                    },
                  },
                },
                03: byte_size (i64) = -1,
              },
              05: has_var_args (bool) = false,
              07: signature (string) = "casttoarray(array<float>)",
              09: scalar_fn (struct) = TScalarFunction {
                01: symbol (string) = "",
              },
              11: id (i64) = 0,
              13: vectorized (bool) = true,
              14: is_udtf_function (bool) = false,
              15: is_static_load (bool) = false,
              16: expiration_time (i64) = 360,
            },
            29: is_nullable (bool) = false,
          },
          [2] = TExprNode {
            01: node_type (i32) = 16,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[2] {
                [0] = TTypeNode {
                  01: type (i32) = 1,
                  04: contains_null (bool) = true,
                  05: contains_nulls (list) = list<bool>[1] {
                    [0] = true,
                  },
                },
                [1] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 7,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            15: slot_ref (struct) = TSlotRef {
              01: slot_id (i32) = 1,
              02: tuple_id (i32) = 0,
              03: col_unique_id (i32) = 1,
              04: is_virtual_slot (bool) = false,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
            36: label (string) = "embedding",
          },
          [3] = TExprNode {
            01: node_type (i32) = 21,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[2] {
                [0] = TTypeNode {
                  01: type (i32) = 1,
                  04: contains_null (bool) = true,
                  05: contains_nulls (list) = list<bool>[1] {
                    [0] = true,
                  },
                },
                [1] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 8,
            20: output_scale (i32) = -1,
            28: child_type (i32) = 8,
            29: is_nullable (bool) = false,
          },
          [4] = TExprNode {
            01: node_type (i32) = 8,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            09: float_literal (struct) = TFloatLiteral {
              01: value (double) = 1,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
          },
          [5] = TExprNode {
            01: node_type (i32) = 8,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            09: float_literal (struct) = TFloatLiteral {
              01: value (double) = 2,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
          },
          [6] = TExprNode {
            01: node_type (i32) = 8,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            09: float_literal (struct) = TFloatLiteral {
              01: value (double) = 3,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
          },
          [7] = TExprNode {
            01: node_type (i32) = 8,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            09: float_literal (struct) = TFloatLiteral {
              01: value (double) = 4,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
          },
          [8] = TExprNode {
            01: node_type (i32) = 8,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            09: float_literal (struct) = TFloatLiteral {
              01: value (double) = 5,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
          },
          [9] = TExprNode {
            01: node_type (i32) = 8,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            09: float_literal (struct) = TFloatLiteral {
              01: value (double) = 6,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
          },
          [10] = TExprNode {
            01: node_type (i32) = 8,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            09: float_literal (struct) = TFloatLiteral {
              01: value (double) = 7,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
          },
          [11] = TExprNode {
            01: node_type (i32) = 8,
            02: type (struct) = TTypeDesc {
              01: types (list) = list<struct>[1] {
                [0] = TTypeNode {
                  01: type (i32) = 0,
                  02: scalar_type (struct) = TScalarType {
                    01: type (i32) = 8,
                  },
                },
              },
              03: byte_size (i64) = -1,
            },
            04: num_children (i32) = 0,
            09: float_literal (struct) = TFloatLiteral {
              01: value (double) = 20,
            },
            20: output_scale (i32) = -1,
            29: is_nullable (bool) = false,
          },
        },
      },
    },
    [4] = TSlotDescriptor {
      01: id (i32) = 4,
      02: parent (i32) = 1,
      03: slotType (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 5,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: columnPos (i32) = -1,
      05: byteOffset (i32) = -1,
      06: nullIndicatorByte (i32) = 0,
      07: nullIndicatorBit (i32) = 0,
      08: colName (string) = "siteid",
      09: slotIdx (i32) = 0,
      10: isMaterialized (bool) = true,
      11: col_unique_id (i32) = 0,
      12: is_key (bool) = true,
      13: need_materialize (bool) = true,
      14: is_auto_increment (bool) = false,
      16: col_default_value (string) = "10",
      17: primitive_type (i32) = 5,
    },
    [5] = TSlotDescriptor {
      01: id (i32) = 5,
      02: parent (i32) = 1,
      03: slotType (struct) = TTypeDesc {
        01: types (list) = list<struct>[2] {
          [0] = TTypeNode {
            01: type (i32) = 1,
            04: contains_null (bool) = true,
            05: contains_nulls (list) = list<bool>[1] {
              [0] = true,
            },
          },
          [1] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 7,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: columnPos (i32) = -1,
      05: byteOffset (i32) = -1,
      06: nullIndicatorByte (i32) = 0,
      07: nullIndicatorBit (i32) = -1,
      08: colName (string) = "embedding",
      09: slotIdx (i32) = 3,
      10: isMaterialized (bool) = true,
      11: col_unique_id (i32) = 1,
      12: is_key (bool) = false,
      13: need_materialize (bool) = true,
      14: is_auto_increment (bool) = false,
      17: primitive_type (i32) = 20,
    },
    [6] = TSlotDescriptor {
      01: id (i32) = 6,
      02: parent (i32) = 1,
      03: slotType (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 23,
              02: len (i32) = 2147483643,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: columnPos (i32) = -1,
      05: byteOffset (i32) = -1,
      06: nullIndicatorByte (i32) = 0,
      07: nullIndicatorBit (i32) = 0,
      08: colName (string) = "comment",
      09: slotIdx (i32) = 2,
      10: isMaterialized (bool) = true,
      11: col_unique_id (i32) = 2,
      12: is_key (bool) = false,
      13: need_materialize (bool) = true,
      14: is_auto_increment (bool) = false,
      17: primitive_type (i32) = 23,
    },
    [7] = TSlotDescriptor {
      01: id (i32) = 7,
      02: parent (i32) = 1,
      03: slotType (struct) = TTypeDesc {
        01: types (list) = list<struct>[1] {
          [0] = TTypeNode {
            01: type (i32) = 0,
            02: scalar_type (struct) = TScalarType {
              01: type (i32) = 8,
            },
          },
        },
        03: byte_size (i64) = -1,
      },
      04: columnPos (i32) = -1,
      05: byteOffset (i32) = -1,
      06: nullIndicatorByte (i32) = 0,
      07: nullIndicatorBit (i32) = 0,
      08: colName (string) = "",
      09: slotIdx (i32) = 1,
      10: isMaterialized (bool) = true,
      11: col_unique_id (i32) = -1,
      12: is_key (bool) = false,
      13: need_materialize (bool) = true,
      14: is_auto_increment (bool) = false,
      17: primitive_type (i32) = 0,
    },
  },
  02: tupleDescriptors (list) = list<struct>[2] {
    [0] = TTupleDescriptor {
      01: id (i32) = 0,
      02: byteSize (i32) = 0,
      03: numNullBytes (i32) = 0,
      04: tableId (i64) = -1273902531,
    },
    [1] = TTupleDescriptor {
      01: id (i32) = 1,
      02: byteSize (i32) = 0,
      03: numNullBytes (i32) = 0,
    },
  },
  03: tableDescriptors (list) = list<struct>[1] {
    [0] = TTableDescriptor {
      01: id (i64) = 1746777786941,
      02: tableType (i32) = 1,
      03: numCols (i32) = 3,
      04: numClusteringCols (i32) = 0,
      07: tableName (string) = "vector_table_3",
      08: dbName (string) = "",
      11: olapTable (struct) = TOlapTable {
        01: tableName (string) = "vector_table_3",
      },
    },
  },
}
*/
const std::string thrift_table_desc =
        R"xxx({"1":{"lst":["rec",8,{"1":{"i32":0},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":-1},"5":{"i32":-1},"6":{"i32":0},"7":{"i32":0},"8":{"str":"siteid"},"9":{"i32":0},"10":{"tf":1},"11":{"i32":0},"12":{"tf":1},"13":{"tf":1},"14":{"tf":0},"16":{"str":"10"},"17":{"i32":5}},{"1":{"i32":1},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}},"4":{"i32":-1},"5":{"i32":-1},"6":{"i32":0},"7":{"i32":-1},"8":{"str":"embedding"},"9":{"i32":3},"10":{"tf":1},"11":{"i32":1},"12":{"tf":0},"13":{"tf":1},"14":{"tf":0},"17":{"i32":20}},{"1":{"i32":2},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]},"3":{"i64":-1}}},"4":{"i32":-1},"5":{"i32":-1},"6":{"i32":0},"7":{"i32":0},"8":{"str":"comment"},"9":{"i32":2},"10":{"tf":1},"11":{"i32":2},"12":{"tf":0},"13":{"tf":1},"14":{"tf":0},"17":{"i32":23}},{"1":{"i32":3},"2":{"i32":0},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":-1},"5":{"i32":-1},"6":{"i32":0},"7":{"i32":0},"8":{"str":""},"9":{"i32":1},"10":{"tf":1},"11":{"i32":-1},"12":{"tf":0},"13":{"tf":1},"14":{"tf":0},"17":{"i32":0},"18":{"rec":{"1":{"lst":["rec",12,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"l2_distance_approximate"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}},{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"l2_distance_approximate(array<double>, array<double>)"},"9":{"rec":{"1":{"str":""}}},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"29":{"tf":1}},{"1":{"i32":5},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"3":{"i32":4},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"casttoarray"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"casttoarray(array<float>)"},"9":{"rec":{"1":{"str":""}}},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"29":{"tf":0}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":1},"2":{"i32":0},"3":{"i32":1},"4":{"tf":0}}},"20":{"i32":-1},"29":{"tf":0},"36":{"str":"embedding"}},{"1":{"i32":21},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":8},"20":{"i32":-1},"28":{"i32":8},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":1}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":2}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":3}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":4}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":5}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":6}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":7}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":20}}},"20":{"i32":-1},"29":{"tf":0}}]}}}},{"1":{"i32":4},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]},"3":{"i64":-1}}},"4":{"i32":-1},"5":{"i32":-1},"6":{"i32":0},"7":{"i32":0},"8":{"str":"siteid"},"9":{"i32":0},"10":{"tf":1},"11":{"i32":0},"12":{"tf":1},"13":{"tf":1},"14":{"tf":0},"16":{"str":"10"},"17":{"i32":5}},{"1":{"i32":5},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}},"4":{"i32":-1},"5":{"i32":-1},"6":{"i32":0},"7":{"i32":-1},"8":{"str":"embedding"},"9":{"i32":3},"10":{"tf":1},"11":{"i32":1},"12":{"tf":0},"13":{"tf":1},"14":{"tf":0},"17":{"i32":20}},{"1":{"i32":6},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":23},"2":{"i32":2147483643}}}}]},"3":{"i64":-1}}},"4":{"i32":-1},"5":{"i32":-1},"6":{"i32":0},"7":{"i32":0},"8":{"str":"comment"},"9":{"i32":2},"10":{"tf":1},"11":{"i32":2},"12":{"tf":0},"13":{"tf":1},"14":{"tf":0},"17":{"i32":23}},{"1":{"i32":7},"2":{"i32":1},"3":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":-1},"5":{"i32":-1},"6":{"i32":0},"7":{"i32":0},"8":{"str":""},"9":{"i32":1},"10":{"tf":1},"11":{"i32":-1},"12":{"tf":0},"13":{"tf":1},"14":{"tf":0},"17":{"i32":0}}]},"2":{"lst":["rec",2,{"1":{"i32":0},"2":{"i32":0},"3":{"i32":0},"4":{"i64":-1273902531}},{"1":{"i32":1},"2":{"i32":0},"3":{"i32":0}}]},"3":{"lst":["rec",1,{"1":{"i64":1746777786941},"2":{"i32":1},"3":{"i32":3},"4":{"i32":0},"7":{"str":"vector_table_3"},"8":{"str":""},"11":{"rec":{"1":{"str":"vector_table_3"}}}}]}})xxx";

TEST_F(VectorSearchTest, TestPrepareAnnRangeSearch) {
    TExpr texpr = read_from_json<TExpr>(ann_range_search_thrift);
    // std::cout << "range_search thrift:\n" << apache::thrift::ThriftDebugString(texpr) << std::endl;
    // TExpr distance_function_call =
    //         read_from_json<TExpr>(distance_function_call_thrift);
    TDescriptorTable table1 = read_from_json<TDescriptorTable>(thrift_table_desc);
    // std::cout << "table thrift:\n" << apache::thrift::ThriftDebugString(table1) << std::endl;
    std::unique_ptr<doris::ObjectPool> pool = std::make_unique<doris::ObjectPool>();
    auto desc_tbl = std::make_unique<DescriptorTbl>();
    DescriptorTbl* desc_tbl_ptr = desc_tbl.get();
    ASSERT_TRUE(DescriptorTbl::create(pool.get(), table1, &(desc_tbl_ptr)).ok());
    RowDescriptor row_desc = RowDescriptor(*desc_tbl_ptr, {0}, {false});
    std::unique_ptr<doris::RuntimeState> state = std::make_unique<doris::RuntimeState>();
    state->set_desc_tbl(desc_tbl_ptr);

    VExprContextSPtr range_search_ctx;
    doris::VectorSearchUserParams user_params;
    ASSERT_TRUE(vectorized::VExpr::create_expr_tree(texpr, range_search_ctx).ok());
    ASSERT_TRUE(range_search_ctx->prepare(state.get(), row_desc).ok());
    ASSERT_TRUE(range_search_ctx->open(state.get()).ok());
    ASSERT_TRUE(range_search_ctx->prepare_ann_range_search(user_params).ok());
    ASSERT_TRUE(range_search_ctx->_ann_range_search_runtime.is_ann_range_search == true);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.is_le_or_lt, false);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.dst_col_idx, 3);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.src_col_idx, 1);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.radius, 10);

    doris::vectorized::RangeSearchParams ann_range_search_runtime =
            range_search_ctx->_ann_range_search_runtime.to_range_search_params();
    EXPECT_EQ(ann_range_search_runtime.radius, 10.0f);
    std::vector<int> query_array_groud_truth = {1, 2, 3, 4, 5, 6, 7, 20};
    std::vector<int> query_array_f32;
    for (int i = 0; i < query_array_groud_truth.size(); ++i) {
        query_array_f32.push_back(static_cast<int>(ann_range_search_runtime.query_value[i]));
    }
    for (int i = 0; i < query_array_f32.size(); ++i) {
        EXPECT_EQ(query_array_f32[i], query_array_groud_truth[i]);
    }
}

TEST_F(VectorSearchTest, TestEvaluateAnnRangeSearch) {
    TExpr texpr = read_from_json<TExpr>(ann_range_search_thrift);
    TDescriptorTable table1 = read_from_json<TDescriptorTable>(thrift_table_desc);
    std::unique_ptr<doris::ObjectPool> pool = std::make_unique<doris::ObjectPool>();
    auto desc_tbl = std::make_unique<DescriptorTbl>();
    DescriptorTbl* desc_tbl_ptr = desc_tbl.get();
    ASSERT_TRUE(DescriptorTbl::create(pool.get(), table1, &(desc_tbl_ptr)).ok());
    RowDescriptor row_desc = RowDescriptor(*desc_tbl_ptr, {0}, {false});
    std::unique_ptr<doris::RuntimeState> state = std::make_unique<doris::RuntimeState>();
    state->set_desc_tbl(desc_tbl_ptr);

    VExprContextSPtr range_search_ctx;
    ASSERT_TRUE(vectorized::VExpr::create_expr_tree(texpr, range_search_ctx).ok());
    ASSERT_TRUE(range_search_ctx->prepare(state.get(), row_desc).ok());
    ASSERT_TRUE(range_search_ctx->open(state.get()).ok());
    doris::VectorSearchUserParams user_params;
    ASSERT_TRUE(range_search_ctx->prepare_ann_range_search(user_params).ok());
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.user_params, user_params);
    ASSERT_TRUE(range_search_ctx->_ann_range_search_runtime.is_ann_range_search == true);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.is_le_or_lt, false);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.src_col_idx, 1);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.dst_col_idx, 3);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.radius, 10);

    std::vector<ColumnId> idx_to_cid;
    idx_to_cid.resize(4);
    idx_to_cid[0] = 0;
    idx_to_cid[1] = 1;
    idx_to_cid[2] = 2;
    idx_to_cid[3] = 3;
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> cid_to_index_iterators;
    cid_to_index_iterators.resize(4);
    cid_to_index_iterators[1] =
            std::make_unique<doris::vector_search_utils::MockAnnIndexIterator>();
    std::vector<std::unique_ptr<segment_v2::ColumnIterator>> column_iterators;
    column_iterators.resize(4);
    column_iterators[3] = std::make_unique<doris::segment_v2::VirtualColumnIterator>();

    roaring::Roaring row_bitmap;
    doris::vector_search_utils::MockAnnIndexIterator* mock_ann_index_iter =
            dynamic_cast<doris::vector_search_utils::MockAnnIndexIterator*>(
                    cid_to_index_iterators[1].get());

    std::map<std::string, std::string> properties;
    properties["index_type"] = "hnsw";
    properties["metric_type"] = "l2_distance";
    auto pair = vector_search_utils::create_tmp_ann_index_reader(properties);
    mock_ann_index_iter->_ann_reader = pair.second;

    // Explain:
    // 1. predicate is dist >= 10, so it is not a within range search
    // 2. return 10 results
    EXPECT_CALL(*mock_ann_index_iter,
                range_search(testing::Truly([](const doris::vectorized::RangeSearchParams& params) {
                                 return params.is_le_or_lt == false && params.radius == 10.0f;
                             }),
                             testing::_, testing::_))
            .WillOnce(testing::Invoke([](const doris::vectorized::RangeSearchParams& params,
                                         const doris::VectorSearchUserParams& custom_params,
                                         doris::vectorized::RangeSearchResult* result) {
                result->roaring = std::make_shared<roaring::Roaring>();
                result->row_ids = nullptr;
                result->distance = nullptr;
                return Status::OK();
            }));

    AnnIndexStats ann_index_stats;
    ASSERT_TRUE(range_search_ctx
                        ->evaluate_ann_range_search(cid_to_index_iterators, idx_to_cid,
                                                    column_iterators, row_bitmap, ann_index_stats)
                        .ok());

    doris::segment_v2::VirtualColumnIterator* virtual_column_iter =
            dynamic_cast<doris::segment_v2::VirtualColumnIterator*>(column_iterators[3].get());
    vectorized::IColumn::Ptr column = virtual_column_iter->get_materialized_column();
    const vectorized::ColumnFloat32* float_column =
            check_and_get_column<const vectorized::ColumnFloat32>(column.get());
    const vectorized::ColumnNothing* nothing_column =
            check_and_get_column<const vectorized::ColumnNothing>(column.get());
    ASSERT_EQ(float_column, nullptr);
    ASSERT_NE(nothing_column, nullptr);
    EXPECT_EQ(column->size(), 0);

    const auto& get_row_id_to_idx = virtual_column_iter->get_row_id_to_idx();
    EXPECT_EQ(get_row_id_to_idx.size(), 0);
}

TEST_F(VectorSearchTest, TestEvaluateAnnRangeSearch2) {
    // Modify expr from dis >= 10 to dis < 10
    TExpr texpr = read_from_json<TExpr>(ann_range_search_thrift);
    TExprNode& opNode = texpr.nodes[0];
    opNode.opcode = TExprOpcode::LT;
    opNode.fn.name.function_name = doris::vectorized::NameLess::name;
    TDescriptorTable table1 = read_from_json<TDescriptorTable>(thrift_table_desc);
    std::unique_ptr<doris::ObjectPool> pool = std::make_unique<doris::ObjectPool>();
    auto desc_tbl = std::make_unique<DescriptorTbl>();
    DescriptorTbl* desc_tbl_ptr = desc_tbl.get();
    ASSERT_TRUE(DescriptorTbl::create(pool.get(), table1, &(desc_tbl_ptr)).ok());
    RowDescriptor row_desc = RowDescriptor(*desc_tbl_ptr, {0}, {false});
    std::unique_ptr<doris::RuntimeState> state = std::make_unique<doris::RuntimeState>();
    state->set_desc_tbl(desc_tbl_ptr);

    VExprContextSPtr range_search_ctx;
    ASSERT_TRUE(vectorized::VExpr::create_expr_tree(texpr, range_search_ctx).ok());
    ASSERT_TRUE(range_search_ctx->prepare(state.get(), row_desc).ok());
    ASSERT_TRUE(range_search_ctx->open(state.get()).ok());
    doris::VectorSearchUserParams user_params;
    ASSERT_TRUE(range_search_ctx->prepare_ann_range_search(user_params).ok());
    ASSERT_TRUE(range_search_ctx->_ann_range_search_runtime.is_ann_range_search == true);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.is_le_or_lt, true);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.src_col_idx, 1);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.dst_col_idx, 3);
    ASSERT_EQ(range_search_ctx->_ann_range_search_runtime.radius, 10);

    std::vector<ColumnId> idx_to_cid;
    idx_to_cid.resize(4);
    idx_to_cid[0] = 0;
    idx_to_cid[1] = 1;
    idx_to_cid[2] = 2;
    idx_to_cid[3] = 3;
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> cid_to_index_iterators;
    cid_to_index_iterators.resize(4);
    cid_to_index_iterators[1] =
            std::make_unique<doris::vector_search_utils::MockAnnIndexIterator>();
    std::vector<std::unique_ptr<segment_v2::ColumnIterator>> column_iterators;
    column_iterators.resize(4);
    column_iterators[3] = std::make_unique<doris::segment_v2::VirtualColumnIterator>();
    roaring::Roaring row_bitmap;
    doris::vector_search_utils::MockAnnIndexIterator* mock_ann_index_iter =
            dynamic_cast<doris::vector_search_utils::MockAnnIndexIterator*>(
                    cid_to_index_iterators[1].get());
    std::map<std::string, std::string> properties;
    properties["index_type"] = "hnsw";
    properties["metric_type"] = "l2_distance";
    auto pair = vector_search_utils::create_tmp_ann_index_reader(properties);
    mock_ann_index_iter->_ann_reader = pair.second;

    // Explain:
    // 1. predicate is dist >= 10, so it is not a within range search
    // 2. return 10 results
    EXPECT_CALL(*mock_ann_index_iter,
                range_search(testing::Truly([](const doris::vectorized::RangeSearchParams& params) {
                                 return params.is_le_or_lt == true && params.radius == 10.0f;
                             }),
                             testing::_, testing::_))
            .WillOnce(testing::Invoke([](const doris::vectorized::RangeSearchParams& params,
                                         const doris::VectorSearchUserParams& custom_params,
                                         doris::vectorized::RangeSearchResult* result) {
                size_t num_results = 10;
                result->roaring = std::make_shared<roaring::Roaring>();
                result->row_ids = std::make_unique<std::vector<uint64_t>>();
                for (size_t i = 0; i < num_results; ++i) {
                    result->roaring->add(i * 10);
                    result->row_ids->push_back(i * 10);
                }
                result->distance = std::make_unique<float[]>(10);
                return Status::OK();
            }));

    AnnIndexStats ann_index_stats;
    ASSERT_TRUE(range_search_ctx
                        ->evaluate_ann_range_search(cid_to_index_iterators, idx_to_cid,
                                                    column_iterators, row_bitmap, ann_index_stats)
                        .ok());

    doris::segment_v2::VirtualColumnIterator* virtual_column_iter =
            dynamic_cast<doris::segment_v2::VirtualColumnIterator*>(column_iterators[3].get());

    vectorized::IColumn::Ptr column = virtual_column_iter->get_materialized_column();
    const vectorized::ColumnFloat64* double_column =
            check_and_get_column<const vectorized::ColumnFloat64>(column.get());
    const vectorized::ColumnNothing* nothing_column =
            check_and_get_column<const vectorized::ColumnNothing>(column.get());
    ASSERT_NE(double_column, nullptr);
    ASSERT_EQ(nothing_column, nullptr);
    EXPECT_EQ(double_column->size(), 10);
    EXPECT_EQ(row_bitmap.cardinality(), 10);

    const auto& get_row_id_to_idx = virtual_column_iter->get_row_id_to_idx();
    EXPECT_EQ(get_row_id_to_idx.size(), 10);
}

} // namespace doris::vectorized