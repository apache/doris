# Python UDF/UDAF/UDTF 环境配置与多版本管理

## 概述

在使用 Python UDF/UDAF/UDTF 之前，请确保 Doris 的 Backend (BE) 节点已正确配置 Python 运行环境。Doris 支持通过 **Conda** 或 **Virtual Environment (venv)** 管理 Python 环境，允许不同的 UDF 使用不同版本的 Python 解释器和依赖库。

Doris 提供两种 Python 环境管理方式:
- **Conda 模式**: 使用 Miniconda/Anaconda 管理多版本环境
- **Venv 模式**: 使用 Python 内置的虚拟环境 (venv) 管理多版本环境

## BE 配置参数

在所有 BE 节点的 `be.conf` 配置文件中设置以下参数，并**重启 BE** 使配置生效。

### 配置参数说明

| 参数名 | 类型 | 可选值 | 默认值 | 说明 |
|--------|------|--------|--------|------|
| `enable_python_udf_support` | bool | `true` / `false` | `false` | 是否启用 Python UDF 功能 |
| `python_env_mode` | string | `conda` / `venv` | `""` | Python 多版本环境管理方式 |
| `python_conda_root_path` | string | 目录路径 | `""` | Miniconda 的根目录<br>仅在 `python_env_mode = conda` 时生效 |
| `python_venv_root_path` | string | 目录路径 | `${DORIS_HOME}/lib/udf/python` | venv 多版本管理的根目录<br>仅在 `python_env_mode = venv` 时生效 |
| `python_venv_interpreter_paths` | string | 路径列表(用 `:` 分隔) | `""` | 可用 Python 解释器的目录列表<br>仅在 `python_env_mode = venv` 时生效 |
| `min_python_process_nums` | int32 | 整数 | `4` | Python Server 进程池最少运行的进程数 |
| `max_python_process_nums` | int32 | 整数 | `64` | Python Server 进程池最多运行的进程数 |

## 方式一: 使用 Conda 管理 Python 环境

### 1. 配置 BE

在 `be.conf` 中添加以下配置:

```properties
# be.conf
enable_python_udf_support = true
python_env_mode = conda
python_conda_root_path = /path/to/miniconda3
```

### 2. 环境查找规则

Doris 会在 `${python_conda_root_path}/envs/` 目录下查找与 UDF 中 `runtime_version` 匹配的 Conda 环境。

**匹配规则**:
- `runtime_version` **必须填写 Python 版本的完整版本号**，格式为 `x.x.x` 或 `x.x.xx`，例如 `"3.9.18"`、`"3.12.11"`
- Doris 会遍历所有 Conda 环境，检查每个环境中 Python 解释器的实际版本是否与 `runtime_version` 完全匹配
- 如果找不到匹配的环境，则会报错: `Python environment with version x.x.x not found`

**示例**:
- UDF 中指定 `runtime_version = "3.9.18"`，Doris 会在所有环境中查找 Python 版本为 3.9.18 的环境
- 环境名称可以是任意的 (如 `py39`、`my-env`、`data-science` 等)，只要该环境中的 Python 版本为 3.9.18 即可
- 必须填写完整版本号，不能使用版本前缀，如 `"3.9"` 或 `"3.12"`

### 3. 目录结构示意图

```
# Doris BE 节点文件系统结构 (Conda 模式)

/path/to/miniconda3                  ← python_conda_root_path (由 be.conf 配置)
│
├── bin/
│   ├── conda                        ← conda 命令行工具 (运维使用)
│   └── ...                          ← 其他 conda 工具
│
├── envs/                            ← 所有 Conda 环境存放目录
│   │
│   ├── py39/                        ← Conda 环境 1 (用户创建)
│   │   ├── bin/
│   │   │   ├── python               ← Python 3.9 解释器 (Doris 直接调用)
│   │   │   ├── pip
│   │   │   └── ...
│   │   ├── lib/
│   │   │   └── python3.9/
│   │   │       └── site-packages/   ← 该环境的第三方依赖 (如 pandas， pyarrow)
│   │   └── ...
│   │
│   ├── py312/                       ← Conda 环境 2 (用户创建)
│   │   ├── bin/
│   │   │   └── python               ← Python 3.12 解释器
│   │   └── lib/
│   │       └── python3.12/
│   │           └── site-packages/   ← 预装的依赖 (如 torch， sklearn)
│   │
│   └── ml-env/                      ← 语义化环境名 (推荐)
│       ├── bin/
│       │   └── python               ← 可能是 Python 3.12 + GPU 依赖
│       └── lib/
│           └── python3.12/
│               └── site-packages/
│
└── ...
```

### 4. 创建 Conda 环境

> **⚠️ 重要**: Doris Python UDF/UDAF/UDTF 功能**强制依赖** `pandas` 和 `pyarrow` 两个库,**必须**在所有 Python 环境中预先安装这两个依赖,否则 UDF 将无法正常运行。

**在所有 BE 节点上**执行以下命令创建 Python 环境:

```bash
# 安装 Miniconda (如果尚未安装)
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/miniconda3

# 创建 Python 3.9.18 环境并安装必需的依赖 (环境名可自定义)
/opt/miniconda3/bin/conda create -n py39 python=3.9.18 pandas pyarrow -y

# 创建 Python 3.12.11 环境并预装依赖 (环境名可自定义)
/opt/miniconda3/bin/conda create -n py312 python=3.12.11 pandas pyarrow numpy -y

# 使用语义化命名的环境 (重要: Python 版本必须精确指定,且必须安装 pandas 和 pyarrow)
/opt/miniconda3/bin/conda create -n ml-env python=3.12.11 pandas pyarrow scikit-learn -y

# 激活环境并安装额外依赖
source /opt/miniconda3/bin/activate py39
conda install requests beautifulsoup4 -y
conda deactivate

# 验证环境中的 Python 版本
/opt/miniconda3/envs/py39/bin/python --version     # 应输出: Python 3.9.18
/opt/miniconda3/envs/py312/bin/python --version    # 应输出: Python 3.12.11
/opt/miniconda3/envs/ml-env/bin/python --version   # 应输出: Python 3.12.11
```

### 5. 在 UDF 中使用

```sql
-- 使用 Python 3.9.18 环境
CREATE FUNCTION py_process_data(STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.9.18",  -- 必须指定完整版本号，匹配 Python 3.9.18
    "always_nullable" = "true"
)
AS $$
import pandas as pd

def evaluate(data):
    return data.upper()
$$;

-- 使用 Python 3.12.11 环境
CREATE FUNCTION py_ml_predict(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.12.11",  -- 必须指定完整版本号，匹配 Python 3.12.11
    "always_nullable" = "true"
)
AS $$
def evaluate(x):
    # 可以使用 Python 3.12.11 环境中安装的库
    return x * 2
$$;

-- 注意: 无论环境名是 py312 还是 ml-env，只要 Python 版本是 3.12.11，都可以使用
-- runtime_version 只关注 Python 版本，不关注环境名称
```

## 方式二: 使用 Venv 管理 Python 环境

### 1. 配置 BE

在 `be.conf` 中添加以下配置:

```properties
# be.conf
enable_python_udf_support = true
python_env_mode = venv
python_venv_root_path = /doris/python_envs
python_venv_interpreter_paths = /opt/python3.9/bin/python3.9:/opt/python3.12/bin/python3.12
```

### 2. 配置参数说明

- **`python_venv_root_path`**: 虚拟环境的根目录，所有 venv 环境都将创建在此目录下
- **`python_venv_interpreter_paths`**: 以英文冒号 `:` 分隔的 Python 解释器绝对路径列表。Doris 会检查每个解释器的版本，并根据 UDF 中指定的 `runtime_version` (完整版本号，如 `"3.9.18"`) 匹配对应的解释器

### 3. 目录结构示意图

```
# Doris BE 配置 (be.conf)
python_venv_interpreter_paths = "/opt/python3.9/bin/python3.9:/opt/python3.12/bin/python3.12"
python_venv_root_path = /doris/python_envs

/opt/python3.9/bin/python3.9                ← 系统预装 Python 3.9
/opt/python3.12/bin/python3.12              ← 系统预装 Python 3.12

/doris/python_envs/                         ← 所有虚拟环境的根目录 (python_venv_root_path)
│
├── python3.9.18/                           ← 环境 ID = Python 完整版本
│   ├── bin/
│   │   ├── python
│   │   └── pip
│   └── lib/python3.9/site-packages/
│       ├── pandas==2.1.0
│       └── pyarrow==15.0.0
│
├── python3.12.11/                          ← Python 3.12.11 环境
│   ├── bin/
│   │   ├── python
│   │   └── pip
│   └── lib/python3.12/site-packages/
│       ├── pandas==2.1.0
│       └── pyarrow==15.0.0
│
└── python3.12.10/                          ← Python 3.12.10 环境
    └── ...
```

### 4. 创建 Venv 环境

> **⚠️ 重要**: Doris Python UDF/UDAF/UDTF 功能**强制依赖** `pandas` 和 `pyarrow` 两个库,**必须**在所有 Python 环境中预先安装这两个依赖,否则 UDF 将无法正常运行。

**在所有 BE 节点上**执行以下命令:

```bash
# 创建虚拟环境根目录
mkdir -p /doris/python_envs

# 使用 Python 3.9 创建虚拟环境
/opt/python3.9/bin/python3.9 -m venv /doris/python_envs/python3.9.18

# 激活环境并安装必需的依赖 (pandas 和 pyarrow 必须安装)
source /doris/python_envs/python3.9.18/bin/activate
pip install pandas pyarrow numpy
deactivate

# 使用 Python 3.12 创建虚拟环境
/opt/python3.12/bin/python3.12 -m venv /doris/python_envs/python3.12.11

# 激活环境并安装必需的依赖 (pandas 和 pyarrow 必须安装)
source /doris/python_envs/python3.12.11/bin/activate
pip install pandas pyarrow numpy scikit-learn
deactivate
```

### 5. 在 UDF 中使用

```sql
-- 使用 Python 3.9.18 环境
CREATE FUNCTION py_clean_text(STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.9.18",  -- 必须指定完整版本号，匹配 Python 3.9.18
    "always_nullable" = "true"
)
AS $$
def evaluate(text):
    return text.strip().upper()
$$;

-- 使用 Python 3.12.11 环境
CREATE FUNCTION py_calculate(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.12.11",  -- 必须指定完整版本号，匹配 Python 3.12.11
    "always_nullable" = "true"
)
AS $$
import numpy as np

def evaluate(x):
    return np.sqrt(x)
$$;
```

## 环境管理最佳实践

### 1. 选择合适的管理方式

| 场景 | 推荐方式 | 原因 |
|------|---------|------|
| 需要频繁切换 Python 版本 | Conda | 环境隔离性好，依赖管理简单 |
| 已有 Conda 环境 | Conda | 可直接复用现有环境 |
| 系统资源有限 | Venv | 占用空间小，启动快 |
| 已有 Python 系统环境 | Venv | 无需额外安装 Conda |

### 2. 环境一致性要求

> **⚠️ 重要**: 所有 BE 节点必须配置**完全相同**的 Python 环境，包括:
> - Python 版本必须一致
> - 已安装的依赖包及其版本必须一致
> - 环境目录路径必须一致

**建议操作流程**:
1. 在一台 BE 节点上创建并测试环境
2. 导出依赖列表: `pip freeze > requirements.txt`
3. 使用自动化工具 (如 Ansible、SaltStack等) 在所有 BE 节点上复制环境配置
4. 验证所有节点的环境一致性

### 3. 依赖包管理

**Conda 模式**:
```bash
# 导出环境
conda env export -n py39 > environment.yml

# 在其他节点复制环境
conda env create -f environment.yml
```

**Venv 模式**:
```bash
# 导出依赖
source /doris/python_envs/python3.9.18/bin/activate
pip freeze > requirements.txt

# 在其他节点安装相同依赖
source /doris/python_envs/python3.9.18/bin/activate
pip install -r requirements.txt
```

### 4. 推荐预装的依赖

> **⚠️ 必需依赖**: `pandas` 和 `pyarrow` 是 Doris Python UDF/UDAF/UDTF 的**强制依赖**,必须在所有环境中安装。

```bash
# 必需依赖 (强制要求)
pip install pandas pyarrow

# 推荐的数据处理库
pip install numpy

# 科学计算
pip install scipy scikit-learn

# 文本处理
pip install regex

# 日期时间
pip install python-dateutil pytz
```

## 注意事项

### 1. 配置修改生效

- 修改 `be.conf` 后，**必须重启 BE 进程**才能生效
- 重启前请确保配置正确，避免服务中断

### 2. 路径验证

配置前请确保路径正确:

```bash
# Conda 模式: 验证 conda 路径
ls -la /opt/miniconda3/bin/conda
/opt/miniconda3/bin/conda env list

# Venv 模式: 验证解释器路径
/opt/python3.9/bin/python3.9 --version
/opt/python3.12/bin/python3.12 --version
```

### 3. 权限设置

确保 Doris BE 进程有权限访问 Python 环境目录:

```bash
# Conda 模式
chmod -R 755 /opt/miniconda3

# Venv 模式
chmod -R 755 /doris/python_envs
chown -R doris:doris /doris/python_envs  # 假设 BE 进程用户为 doris
```

### 4. 资源限制

根据实际需求调整 Python 进程池参数:

```properties
# 高并发场景
min_python_process_nums = 8
max_python_process_nums = 128

# 资源受限场景
min_python_process_nums = 2
max_python_process_nums = 32
```

## 环境验证

在每个 BE 节点上验证环境是否正确:

```bash
# Conda 模式
/opt/miniconda3/envs/py39/bin/python --version
/opt/miniconda3/envs/py39/bin/python -c "import pandas; print(pandas.__version__)"

# Venv 模式
/doris/python_envs/python3.9.18/bin/python --version
/doris/python_envs/python3.9.18/bin/python -c "import pandas; print(pandas.__version__)"
```

## 常见问题排查

### Q1: UDF 调用时提示 "Python environment not found"

**原因**: 
- `runtime_version` 指定的版本在系统中不存在
- 环境路径配置不正确

**解决方案**:
```bash
# 检查 Conda 环境列表
conda env list

# 检查 Venv 解释器是否存在
ls -la /opt/python3.9/bin/python3.9

# 检查 BE 配置
grep python /path/to/be.conf
```

### Q2: UDF 调用时提示 "ModuleNotFoundError: No module named 'xxx'"

**原因**: Python 环境中未安装所需依赖包

**解决方案**:

> **特别提醒**: 如果提示 `ModuleNotFoundError: No module named 'pandas'` 或 `ModuleNotFoundError: No module named 'pyarrow'`,说明环境中缺少必需依赖。这两个库是 Doris Python UDF 的**强制依赖**,必须安装。

```bash
# Conda 模式 - 安装必需依赖
source /opt/miniconda3/bin/activate py39
conda install pandas pyarrow -y
conda deactivate

# Conda 模式 - 安装其他依赖
source /opt/miniconda3/bin/activate py39
conda install xxx -y
conda deactivate

# Venv 模式 - 安装必需依赖
source /doris/python_envs/python3.9.18/bin/activate
pip install pandas pyarrow
deactivate

# Venv 模式 - 安装其他依赖
source /doris/python_envs/python3.9.18/bin/activate
pip install xxx
deactivate
```

### Q3: 不同 BE 节点执行结果不一致

**原因**: 各 BE 节点的 Python 环境或依赖版本不一致

**解决方案**:
1. 检查所有节点的 Python 版本和依赖版本
2. 统一使用 `requirements.txt` 或 `environment.yml` 部署环境
3. 验证所有节点环境一致性

### Q4: 修改 be.conf 后未生效

**可能的原因**: 未重启 BE 进程

## 使用限制

1. **性能考虑**:
   - Python UDF 性能低于内置函数，建议用于逻辑复杂但数据量不大的场景
   - 对于大数据量处理，优先考虑向量化模式

2. **类型限制**:
   - 不支持 HLL、Bitmap 等特殊类型

3. **环境隔离**:
   - 同一函数名在不同数据库中可重复定义
   - 调用时需指定数据库名 (如 `db.func()`) 以避免歧义

4. **并发限制**:
   - Python UDF 通过进程池执行，并发数受 `max_python_process_nums` 限制
   - 高并发场景需适当调大该参数

---

## 相关文档

- [Python UDF 使用指南](python-udf-documentation.md)
- [Python UDAF 使用指南](python-udaf-documentation.md)
- [Python UDTF 使用指南](python-udtf-documentation.md)
