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
import base64
import gc
import importlib
import inspect
import ipaddress
import json
import sys
import os
import traceback
import logging
import time
import threading
import pickle
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Callable, Optional, Tuple, get_origin, Dict
from datetime import datetime
from enum import Enum
from pathlib import Path
from logging.handlers import RotatingFileHandler

import pandas as pd
import pyarrow as pa
from pyarrow import flight


class ServerState:
    """Global server state container."""

    unix_socket_path: str = ""

    PYTHON_SERVER_START_SUCCESS_MSG: str = "Start python server successfully"

    @staticmethod
    def setup_logging():
        """Setup logging configuration for the UDF server with rotation."""

        doris_home = os.getenv("DORIS_HOME")
        if not doris_home:
            # Fallback to current directory if DORIS_HOME is not set
            doris_home = os.getcwd()

        log_dir = os.path.join(doris_home, "log")
        os.makedirs(log_dir, exist_ok=True)

        # Use shared log file with process ID in each log line
        log_file = os.path.join(log_dir, "python_udf_output.log")
        max_bytes = 128 * 1024 * 1024  # 128MB
        backup_count = 5

        # Use RotatingFileHandler to automatically manage log file size
        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )

        # Include process ID in log format
        file_handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] [PID:%(process)d] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"
            )
        )

        logging.basicConfig(
            level=logging.INFO,
            handlers=[file_handler],
        )
        logging.info(
            "Logging initialized. Log file: %s (max_size=%dMB, backups=%d)",
            log_file,
            max_bytes // (1024 * 1024),
            backup_count,
        )

    @staticmethod
    def extract_base_unix_socket_path(unix_socket_uri: str) -> str:
        """
        Extract the file system path from a gRPC Unix socket URI.

        Args:
            unix_socket_uri: URI in format 'grpc+unix:///path/to/socket'

        Returns:
            The file system path without the protocol prefix
        """
        if unix_socket_uri.startswith("grpc+unix://"):
            unix_socket_uri = unix_socket_uri[len("grpc+unix://") :]
        return unix_socket_uri

    @staticmethod
    def remove_unix_socket(unix_socket_uri: str) -> None:
        """
        Remove the Unix domain socket file if it exists.

        Args:
            unix_socket_uri: URI of the Unix socket to remove
        """
        if unix_socket_uri is None:
            return
        base_unix_socket_path = ServerState.extract_base_unix_socket_path(
            unix_socket_uri
        )
        if os.path.exists(base_unix_socket_path):
            try:
                os.unlink(base_unix_socket_path)
                logging.info(
                    "Removed UNIX socket %s successfully", base_unix_socket_path
                )
            except OSError as e:
                logging.error(
                    "Failed to remove UNIX socket %s: %s", base_unix_socket_path, e
                )
        else:
            logging.warning("UNIX socket %s does not exist", base_unix_socket_path)

    @staticmethod
    def monitor_parent_exit():
        """
        Monitor the parent process and exit gracefully if it dies.
        This prevents orphaned UDF server processes.
        """
        parent_pid = os.getppid()
        if parent_pid == 1:
            # Parent process is init, no need to monitor
            logging.info("Parent process is init (PID 1), skipping parent monitoring")
            return

        logging.info("Started monitoring parent process (PID: %s)", parent_pid)

        while True:
            try:
                # os.kill(pid, 0) only checks whether the process exists
                # without sending an actual signal
                os.kill(parent_pid, 0)
            except OSError:
                # Parent process died
                ServerState.remove_unix_socket(ServerState.unix_socket_path)
                logging.error(
                    "Parent process %s died, exiting UDF server, unix socket path: %s",
                    parent_pid,
                    ServerState.unix_socket_path,
                )
                os._exit(0)
            # Check every 2 seconds
            time.sleep(2)


ServerState.setup_logging()
monitor_thread = threading.Thread(target=ServerState.monitor_parent_exit, daemon=True)
monitor_thread.start()


@contextmanager
def temporary_sys_path(path: str):
    """
    Context manager to temporarily add a path to sys.path.
    Ensures the path is removed after use to avoid pollution.

    Args:
        path: Directory path to add to sys.path

    Yields:
        None
    """
    path_added = False
    if path not in sys.path:
        sys.path.insert(0, path)
        path_added = True

    try:
        yield
    finally:
        if path_added and path in sys.path:
            sys.path.remove(path)


def int32_to_uint32(value: int) -> int:
    """
    Convert a signed int32 to unsigned uint32 representation.

    This is used for IPv4 addresses where Arrow uses int32 but the value
    should be interpreted as uint32.

    Args:
        value: Signed 32-bit integer (can be negative)

    Returns:
        Unsigned 32-bit integer (0 to 4294967295)

    Example:
        >>> int32_to_uint32(-1062731519)
        3232235777  # 192.168.1.1
    """
    return value & 0xFFFFFFFF


def uint32_to_int32(value: int) -> int:
    """
    Convert an unsigned uint32 to signed int32 representation.

    This is used when returning IPv4 addresses to Arrow which expects int32.

    Args:
        value: Unsigned 32-bit integer (0 to 4294967295)

    Returns:
        Signed 32-bit integer (-2147483648 to 2147483647)

    Example:
        >>> uint32_to_int32(3232235777)
        -1062731519  # 192.168.1.1 as signed int32
    """
    if value > 0x7FFFFFFF:  # 2147483647
        return value - 0x100000000  # 2 ** 32
    return value


def convert_arrow_field_to_python(field, column_metadata=None):
    """
    Convert Arrow field to Python value, such as IP types.

    This function checks the column metadata for Doris IP types and automatically
    converts them to Python ipaddress objects:
    - IPv4 (Arrow int32) -> ipaddress.IPv4Address
    - IPv6 (Arrow utf8) -> ipaddress.IPv6Address
    - ...

    Args:
        field: Arrow scalar field value
        column_metadata: Optional Arrow field metadata dict containing type information

    Returns:
        Converted Python value (with automatic IP address conversion if metadata present)
    """
    if field is None:
        return None

    if pa.types.is_map(field.type):
        # pyarrow.lib.MapScalar's as_py() returns a list of tuples, convert to dict
        list_of_tuples = field.as_py()
        return dict(list_of_tuples) if list_of_tuples is not None else None
    
    # Check if we should apply special IP type conversion based on metadata
    if column_metadata:
        # Arrow metadata keys can be either bytes or str depending on how they were created
        doris_type = column_metadata.get(b'doris_type') or column_metadata.get('doris_type')
        
        # Handle Doris IPv4 type (Arrow int32 -> ipaddress.IPv4Address)
        if doris_type in (b'IPV4', 'IPV4'):
            if pa.types.is_int32(field.type):
                value = field.as_py()
                if value is not None:
                    try:
                        return ipaddress.IPv4Address(int32_to_uint32(value))
                    except (ValueError, TypeError) as e:
                        logging.warning(
                            "Failed to convert int32 %s to IPv4Address: %s", value, e
                        )
                        return value
                return None
        # Handle Doris IPv6 type (Arrow utf8 -> ipaddress.IPv6Address)
        elif doris_type in (b'IPV6', 'IPV6'):
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                value = field.as_py()
                if value is not None:
                    try:
                        return ipaddress.IPv6Address(value)
                    except (ValueError, TypeError) as e:
                        logging.warning(
                            "Failed to convert string '%s' to IPv6Address: %s", value, e
                        )
                        return value
                return None
    
    return field.as_py()


def convert_python_to_arrow_value(value, output_type=None):
    """
    Convert Python value back to Arrow-compatible value.

    This function handles the reverse conversion of IP addresses:
    - ipaddress.IPv4Address -> int (with uint32 to int32 conversion)
    - ipaddress.IPv6Address -> str (for Arrow utf8)

    Type Safety:
        For IPv4/IPv6 return types, MUST return ipaddress objects.
        Returning raw integers or strings will raise TypeError.

    Args:
        value: Python value to convert (can be single value or iterable)
        output_type: Optional Arrow DataType with metadata

    Returns:
        Arrow-compatible value
    """
    if value is None:
        return None

    is_ipv4_output = False
    is_ipv6_output = False

    if output_type is not None and hasattr(output_type, 'metadata') and output_type.metadata:
        # Arrow metadata keys can be either bytes or str depending on how they were created
        doris_type = output_type.metadata.get(b'doris_type') or output_type.metadata.get('doris_type')
        if doris_type in (b'IPV4', 'IPV4'):
            is_ipv4_output = True
        elif doris_type in (b'IPV6', 'IPV6'):
            is_ipv6_output = True

    # Convert IPv4Address back to int
    if isinstance(value, ipaddress.IPv4Address):
        return uint32_to_int32(int(value))

    # Convert IPv6Address back to str
    if isinstance(value, ipaddress.IPv6Address):
        return str(value)

    # IPv4 output must return IPv4Address objects
    if is_ipv4_output and isinstance(value, int):
        raise TypeError(
            f"IPv4 UDF must return ipaddress.IPv4Address object, got int ({value}). "
            f"Use: return ipaddress.IPv4Address({value})"
        )

    # IPv6 output must return IPv6Address objects
    if is_ipv6_output and isinstance(value, str):
        raise TypeError(
            f"IPv6 UDF must return ipaddress.IPv6Address object, got str ('{value}'). "
            f"Use: return ipaddress.IPv6Address('{value}')"
        )

    # Handle list of values (but not tuples that might be struct data)
    if isinstance(value, list):
        # For list types, recursively convert elements
        if output_type and pa.types.is_list(output_type):
            element_type = output_type.value_type
            return [convert_python_to_arrow_value(v, element_type) for v in value]
        else:
            # No type info, just recurse without type
            return [convert_python_to_arrow_value(v, None) for v in value]

    # Handle tuple values (could be struct data)
    if isinstance(value, tuple):
        # For struct types, convert each field with its corresponding type
        if output_type and pa.types.is_struct(output_type):
            if len(value) != len(output_type):
                raise ValueError(
                    f"Struct has {len(output_type)} fields but tuple has {len(value)} elements"
                )
            # Convert each tuple element with its corresponding field type
            return tuple(
                convert_python_to_arrow_value(v, output_type[i].type)
                for i, v in enumerate(value)
            )
        else:
            # Not a struct type, treat as regular tuple and recurse without type
            return tuple(convert_python_to_arrow_value(v, None) for v in value)
    
    if isinstance(value, dict):
        # For map types, convert keys and values recursively
        if output_type and pa.types.is_map(output_type):
            key_type = output_type.key_type
            item_type = output_type.item_type
            # Convert dict to list of tuples (PyArrow Map format)
            converted_items = [
                (convert_python_to_arrow_value(k, key_type),
                convert_python_to_arrow_value(v, item_type))
                for k, v in value.items()
            ]
            return converted_items
        else:
            # No type info, just recurse without type
            return [(convert_python_to_arrow_value(k, None),
                    convert_python_to_arrow_value(v, None))
                    for k, v in value.items()]

    if isinstance(value, pd.Series):
        return value.apply(lambda v: convert_python_to_arrow_value(v, output_type))

    return value


class VectorType(Enum):
    """Enum representing supported vector types."""

    LIST = "list"
    PANDAS_SERIES = "pandas.Series"
    ARROW_ARRAY = "pyarrow.Array"

    @property
    def python_type(self):
        """
        Returns the Python type corresponding to this VectorType.

        Returns:
            The Python type class (list, pd.Series, or pa.Array)
        """
        mapping = {
            VectorType.LIST: list,
            VectorType.PANDAS_SERIES: pd.Series,
            VectorType.ARROW_ARRAY: pa.Array,
        }
        return mapping[self]

    @staticmethod
    def resolve_vector_type(param: inspect.Parameter):
        """
        Resolves the param's type annotation to the corresponding VectorType enum.
        Returns None if the type is unsupported or not a vector type.
        """
        if (
            param is None
            or param.annotation is None
            or param.annotation is inspect.Parameter.empty
        ):
            return None

        annotation = param.annotation
        origin = get_origin(annotation)
        raw_type = origin if origin is not None else annotation

        if raw_type is list:
            return VectorType.LIST
        if raw_type is pd.Series:
            return VectorType.PANDAS_SERIES

        return None


class ClientType(Enum):
    """Enum representing Python client types."""

    UDF = 0
    UDAF = 1
    UDTF = 2
    UNKNOWN = 3

    def __str__(self) -> str:
        """Return string representation of the client type."""
        return self.name


class PythonUDFMeta:
    """Metadata container for a Python UDF."""

    def __init__(
        self,
        name: str,
        symbol: str,
        location: str,
        udf_load_type: int,
        runtime_version: str,
        always_nullable: bool,
        inline_code: bytes,
        input_types: pa.Schema,
        output_type: pa.DataType,
        client_type: int,
    ) -> None:
        """
        Initialize Python UDF metadata.

        Args:
            name: UDF function name
            symbol: Symbol to load (function name or module.function)
            location: File path or directory containing the UDF
            udf_load_type: 0 for inline code, 1 for module
            runtime_version: Python runtime version requirement
            always_nullable: Whether the UDF can return NULL values
            inline_code: Base64-encoded inline Python code (if applicable)
            input_types: PyArrow schema for input parameters
            output_type: PyArrow data type for return value
            client_type: 0 for UDF, 1 for UDAF, 2 for UDTF
        """
        self.name = name
        self.symbol = symbol
        self.location = location
        self.udf_load_type = udf_load_type
        self.runtime_version = runtime_version
        self.always_nullable = always_nullable
        self.inline_code = inline_code
        self.input_types = input_types
        self.output_type = output_type
        self.client_type = ClientType(client_type)

    def is_udf(self) -> bool:
        """Check if this is a UDF (User-Defined Function)."""
        return self.client_type == ClientType.UDF

    def is_udaf(self) -> bool:
        """Check if this is a UDAF (User-Defined Aggregate Function)."""
        return self.client_type == ClientType.UDAF

    def is_udtf(self) -> bool:
        """Check if this is a UDTF (User-Defined Table Function)."""
        return self.client_type == ClientType.UDTF

    def __str__(self) -> str:
        """Returns a string representation of the UDF metadata."""
        udf_load_type_str = "INLINE" if self.udf_load_type == 0 else "MODULE"
        return (
            f"PythonUDFMeta(name={self.name}, symbol={self.symbol}, "
            f"location={self.location}, udf_load_type={udf_load_type_str}, runtime_version={self.runtime_version}, "
            f"always_nullable={self.always_nullable}, client_type={self.client_type.name}, "
            f"input_types={self.input_types}, output_type={self.output_type})"
        )


class AdaptivePythonUDF:
    """
    A wrapper around a UDF function that supports both scalar and vectorized execution modes.
    The mode is determined by the type hints of the function parameters.
    """

    def __init__(self, python_udf_meta: PythonUDFMeta, func: Callable) -> None:
        """
        Initialize the adaptive UDF wrapper.

        Args:
            python_udf_meta: Metadata describing the UDF
            func: The actual Python function to execute
        """
        self.python_udf_meta = python_udf_meta
        self._eval_func = func

    def __str__(self) -> str:
        """Returns a string representation of the UDF wrapper."""
        input_type_strs = [str(t) for t in self.python_udf_meta.input_types.types]
        output_type_str = str(self.python_udf_meta.output_type)
        eval_func_str = f"{self.python_udf_meta.name}({', '.join(input_type_strs)}) -> {output_type_str}"
        return f"AdaptivePythonUDF(python_udf_meta: {self.python_udf_meta}, eval_func: {eval_func_str})"

    def __call__(self, record_batch: pa.RecordBatch) -> pa.Array:
        """
        Executes the UDF on the given record batch. Supports both scalar and vectorized modes.

        :param record_batch: Input data with N columns, each of length num_rows
        :return: Output array of length num_rows
        """
        if record_batch.num_rows == 0:
            return pa.array([], type=self._get_output_type())

        if self._should_use_vectorized():
            return self._vectorized_call(record_batch)

        return self._scalar_call(record_batch)

    @staticmethod
    def _cast_arrow_to_vector(arrow_array: pa.Array, vec_type: VectorType):
        """
        Convert a pa.Array to an instance of the specified VectorType.
        """
        if vec_type == VectorType.LIST:
            return arrow_array.to_pylist()
        elif vec_type == VectorType.PANDAS_SERIES:
            return arrow_array.to_pandas()
        else:
            raise ValueError(f"Unsupported vector type: {vec_type}")

    def _should_use_vectorized(self) -> bool:
        """
        Determines whether to use vectorized mode based on parameter type annotations.
        Returns True if any parameter is annotated as:
            - list
            - pd.Series
        """
        try:
            signature = inspect.signature(self._eval_func)
        except ValueError:
            # Cannot inspect built-in or C functions; default to scalar
            return False

        for param in signature.parameters.values():
            if VectorType.resolve_vector_type(param):
                return True

        return False

    def _scalar_call(self, record_batch: pa.RecordBatch) -> pa.Array:
        """
        Applies the UDF in scalar mode: one row at a time.

        Args:
            record_batch: Input data batch

        Returns:
            Output array with results for each row
        """
        columns = record_batch.columns
        num_rows = record_batch.num_rows
        result = []

        column_metadata = [
            record_batch.schema.field(col_idx).metadata
            for col_idx in range(len(columns))
        ]

        for i in range(num_rows):
            converted_args = [
                convert_arrow_field_to_python(col[i], meta)
                for col, meta in zip(columns, column_metadata)
            ]

            try:
                res = self._eval_func(*converted_args)
                # Check if result is None when always_nullable is False
                if res is None and not self.python_udf_meta.always_nullable:
                    raise RuntimeError(
                        f"the result of row {i} is null, but the return type is not nullable, "
                        f"please check the always_nullable property in create function statement, "
                        f"it should be true"
                    )
                result.append(convert_python_to_arrow_value(res, self.python_udf_meta.output_type))
            except Exception as e:
                logging.error(
                    "Error in scalar UDF execution at row %s: %s\nArgs: %s\nTraceback: %s",
                    i,
                    e,
                    converted_args,
                    traceback.format_exc(),
                )
                # Return None for failed rows if always_nullable is True
                if self.python_udf_meta.always_nullable:
                    result.append(None)
                else:
                    raise

        return pa.array(result, type=self._get_output_type())

    def _vectorized_call(self, record_batch: pa.RecordBatch) -> pa.Array:
        """
        Applies the UDF in vectorized mode: processes entire columns at once.

        Args:
            record_batch: Input data batch

        Returns:
            Output array with results
        """
        column_args = record_batch.columns
        logging.info("Vectorized call with %s columns", len(column_args))

        sig = inspect.signature(self._eval_func)
        params = list(sig.parameters.values())

        if len(column_args) != len(params):
            raise ValueError(f"UDF expects {len(params)} args, got {len(column_args)}")

        converted_args = []
        for param, arrow_col in zip(params, column_args):
            vec_type = VectorType.resolve_vector_type(param)

            if vec_type is None:
                # For scalar types (int, float, str, etc.), extract the first value
                # instead of converting to list
                pylist = arrow_col.to_pylist()
                if len(pylist) > 0:
                    converted = pylist[0]
                    logging.info(
                        "Converted %s to scalar (first value): %s",
                        param.name,
                        type(converted).__name__,
                    )
                else:
                    converted = None
                    logging.info(
                        "Converted %s to scalar (None, empty column)", param.name
                    )
            else:
                converted = self._cast_arrow_to_vector(arrow_col, vec_type)
                logging.info("Converted %s: %s", param.name, vec_type)

            converted_args.append(converted)

        try:
            result = self._eval_func(*converted_args)
        except Exception as e:
            logging.error(
                "Error in vectorized UDF: %s\nTraceback: %s", e, traceback.format_exc()
            )
            raise RuntimeError(f"Error in vectorized UDF: {e}") from e

        result = convert_python_to_arrow_value(result, self.python_udf_meta.output_type)

        # Convert result to PyArrow Array
        result_array = None
        if isinstance(result, pd.Series):
            result_array = pa.array(result, type=self._get_output_type())
        elif isinstance(result, list):
            result_array = pa.array(result, type=self._get_output_type())
        else:
            # Scalar result - broadcast to all rows
            out_type = self._get_output_type()
            logging.warning(
                "UDF returned scalar value, broadcasting to %s rows",
                record_batch.num_rows,
            )
            result_array = pa.array([result] * record_batch.num_rows, type=out_type)

        # Check for None values when always_nullable is False
        if not self.python_udf_meta.always_nullable:
            null_count = result_array.null_count
            if null_count > 0:
                # Find the first null index for error message
                for i, value in enumerate(result_array):
                    if value.is_valid is False:
                        raise RuntimeError(
                            f"the result of row {i} is null, but the return type is not nullable, "
                            f"please check the always_nullable property in create function statement, "
                            f"it should be true"
                        )

        return result_array

    def _get_output_type(self) -> pa.DataType:
        """
        Returns the expected output type for the UDF.

        Returns:
            PyArrow DataType for the output
        """
        return self.python_udf_meta.output_type or pa.null()


class UDFLoader(ABC):
    """Abstract base class for loading UDFs from different sources."""

    def __init__(self, python_udf_meta: PythonUDFMeta) -> None:
        """
        Initialize the UDF loader.

        Args:
            python_udf_meta: Metadata describing the UDF to load
        """
        self.python_udf_meta = python_udf_meta

    @abstractmethod
    def load(self) -> AdaptivePythonUDF:
        """Load the UDF and return an AdaptivePythonUDF wrapper."""
        raise NotImplementedError("Subclasses must implement load().")


class InlineUDFLoader(UDFLoader):
    """Loads a UDF defined directly in inline code."""

    def load(self) -> AdaptivePythonUDF:
        """
        Load and execute inline Python code to extract the UDF function.

        Returns:
            AdaptivePythonUDF wrapper around the loaded function

        Raises:
            RuntimeError: If code execution fails
            ValueError: If the function is not found or not callable
        """
        symbol = self.python_udf_meta.symbol
        inline_code = self.python_udf_meta.inline_code.decode("utf-8")
        env: dict[str, Any] = {}

        try:
            # Execute the code in a clean environment
            # pylint: disable=exec-used
            # Note: exec() is necessary here for dynamic UDF loading from inline code
            exec(inline_code, env)  # nosec B102
        except Exception as e:
            logging.error(
                "Failed to exec inline code: %s\nTraceback: %s",
                e,
                traceback.format_exc(),
            )
            raise RuntimeError(f"Failed to exec inline code: {e}") from e

        func = env.get(symbol)
        if func is None:
            available_funcs = [
                k for k, v in env.items() if callable(v) and not k.startswith("_")
            ]
            logging.error(
                "Function '%s' not found in inline code. Available functions: %s",
                symbol,
                available_funcs,
            )
            raise ValueError(f"Function '{symbol}' not found in inline code.")

        if not callable(func):
            logging.error(
                "'%s' exists but is not callable (type: %s)", symbol, type(func)
            )
            raise ValueError(f"'{symbol}' is not a callable function.")

        return AdaptivePythonUDF(self.python_udf_meta, func)


class ModuleUDFLoader(UDFLoader):
    """Loads a UDF from a Python module file (.py)."""

    # Class-level lock dictionary for thread-safe module imports
    # Using RLock allows the same thread to acquire the lock multiple times
    _import_locks: Dict[str, threading.RLock] = {}
    _import_locks_lock = threading.Lock()

    @classmethod
    def _get_import_lock(cls, module_name: str) -> threading.RLock:
        """
        Get or create a reentrant lock for the given module name.

        Uses double-checked locking pattern for optimal performance:
        - Fast path: return existing lock without acquiring global lock
        - Slow path: create new lock under global lock protection
        """
        # Fast path: check without lock (read-only, safe for most cases)
        if module_name in cls._import_locks:
            return cls._import_locks[module_name]

        # Slow path: create lock under protection
        with cls._import_locks_lock:
            # Double-check: another thread might have created it while we waited
            if module_name not in cls._import_locks:
                cls._import_locks[module_name] = threading.RLock()
            return cls._import_locks[module_name]

    def load(self) -> AdaptivePythonUDF:
        """
        Loads a UDF from a Python module file.

        Returns:
            AdaptivePythonUDF instance wrapping the loaded function

        Raises:
            ValueError: If module file not found
            TypeError: If symbol is not callable
        """
        symbol = self.python_udf_meta.symbol  # [package_name.]module_name.function_name
        location = self.python_udf_meta.location  # /path/to/module_name[.py]

        if not os.path.exists(location):
            raise ValueError(f"Module file not found: {location}")

        package_name, module_name, func_name = self.parse_symbol(symbol)
        func = self.load_udf_from_module(location, package_name, module_name, func_name)

        if not callable(func):
            raise TypeError(
                f"'{symbol}' exists but is not callable (type: {type(func).__name__})"
            )

        return AdaptivePythonUDF(self.python_udf_meta, func)

    def parse_symbol(self, symbol: str):
        """
        Parse symbol into (package_name, module_name, func_name)

        Supported formats:
        - "module.func"           → (None, module, func)
        - "package.module.func"   → (package, module, func)
        """
        if not symbol or "." not in symbol:
            raise ValueError(
                f"Invalid symbol format: '{symbol}'. "
                "Expected 'module.function' or 'package.module.function'"
            )

        parts = symbol.split(".")
        if len(parts) == 2:
            # module.func → Single-file mode
            module_name, func_name = parts
            package_name = None
            if not module_name or not module_name.strip():
                raise ValueError(f"Module name is empty in symbol: '{symbol}'")
            if not func_name or not func_name.strip():
                raise ValueError(f"Function name is empty in symbol: '{symbol}'")
        elif len(parts) > 2:
            package_name = parts[0]
            module_name = ".".join(parts[1:-1])
            func_name = parts[-1]
            if not package_name or not package_name.strip():
                raise ValueError(f"Package name is empty in symbol: '{symbol}'")
            if not module_name or not module_name.strip():
                raise ValueError(f"Module name is empty in symbol: '{symbol}'")
            if not func_name or not func_name.strip():
                raise ValueError(f"Function name is empty in symbol: '{symbol}'")
        else:
            raise ValueError(f"Invalid symbol format: '{symbol}'")

        return package_name, module_name, func_name

    def _get_or_import_module(self, location: str, full_module_name: str) -> Any:
        """Get module from cache or import it (thread-safe)."""
        # Use a per-module lock to prevent race conditions during import
        import_lock = ModuleUDFLoader._get_import_lock(full_module_name)

        with import_lock:
            # Double-check pattern: verify module is still not loaded after acquiring lock
            if full_module_name in sys.modules:
                cached_module = sys.modules[full_module_name]
                # Verify the cached module is valid (has __file__ or __path__ attribute)
                # This prevents using broken/incomplete modules from failed imports
                if cached_module is not None and (
                    hasattr(cached_module, "__file__")
                    or hasattr(cached_module, "__path__")
                ):
                    return cached_module
                else:
                    del sys.modules[full_module_name]

            # Import the module (only one thread will reach here per module)
            with temporary_sys_path(location):
                try:
                    module = importlib.import_module(full_module_name)
                    return module
                except Exception as e:
                    # Clean up any partially-imported modules from sys.modules
                    # This prevents broken modules from being cached
                    if full_module_name in sys.modules:
                        del sys.modules[full_module_name]
                    raise

    def _extract_function(
        self, module: Any, func_name: str, module_name: str
    ) -> Callable:
        """Extract and validate function from module."""
        func = getattr(module, func_name, None)
        if func is None:
            # Diagnostic info: log module details to understand why function is missing
            module_attrs = dir(module)
            module_file = getattr(module, "__file__", "N/A")
            module_dict_keys = (
                list(module.__dict__.keys()) if hasattr(module, "__dict__") else []
            )

            logging.error(
                "Function '%s' not found in module '%s'. "
                "Module file: %s, "
                "Public attributes: %s, "
                "All dict keys: %s",
                func_name,
                module_name,
                module_file,
                [a for a in module_attrs if not a.startswith("_")][:20],
                module_dict_keys[:20],
            )

            # Check if module has import errors stored
            if hasattr(module, "__import_error__"):
                logging.error(
                    "Module '%s' has stored import error: %s",
                    module_name,
                    module.__import_error__,
                )

            raise AttributeError(
                f"Function '{func_name}' not found in module '{module_name}'"
            )
        if not callable(func):
            raise TypeError(f"'{func_name}' is not callable")
        return func

    def _load_single_file_udf(
        self, location: str, module_name: str, func_name: str
    ) -> Callable:
        """Load UDF from a single Python file."""
        py_file = os.path.join(location, f"{module_name}.py")
        if not os.path.isfile(py_file):
            raise ImportError(f"Python file not found: {py_file}")

        try:
            udf_module = self._get_or_import_module(location, module_name)
            return self._extract_function(udf_module, func_name, module_name)
        except (ImportError, AttributeError, TypeError) as e:
            raise ImportError(
                f"Failed to load single-file UDF '{module_name}.{func_name}': {e}"
            ) from e
        except Exception as e:
            logging.error(
                "Unexpected error loading UDF: %s\n%s", e, traceback.format_exc()
            )
            raise

    def _ensure_package_init(self, package_path: str, package_name: str) -> None:
        """Ensure __init__.py exists in the package directory."""
        init_path = os.path.join(package_path, "__init__.py")
        if not os.path.exists(init_path):
            logging.warning(
                "__init__.py not found in package '%s', attempting to create it",
                package_name,
            )
            try:
                with open(init_path, "w", encoding="utf-8") as f:
                    f.write(
                        "# Auto-generated by UDF loader to make directory a Python package\n"
                    )
            except OSError as e:
                raise ImportError(
                    f"Cannot create __init__.py in package '{package_name}': {e}"
                ) from e

    def _build_full_module_name(self, package_name: str, module_name: str) -> str:
        """Build the full module name for package mode."""
        if module_name == "__init__":
            return package_name
        return f"{package_name}.{module_name}"

    def _load_package_udf(
        self, location: str, package_name: str, module_name: str, func_name: str
    ) -> Callable:
        """Load UDF from a Python package."""
        package_path = os.path.join(location, package_name)
        if not os.path.isdir(package_path):
            raise ImportError(f"Package '{package_name}' not found in '{location}'")

        self._ensure_package_init(package_path, package_name)

        try:
            full_module_name = self._build_full_module_name(package_name, module_name)
            udf_module = self._get_or_import_module(location, full_module_name)
            return self._extract_function(udf_module, func_name, full_module_name)
        except (ImportError, AttributeError, TypeError) as e:
            raise ImportError(
                f"Failed to load packaged UDF '{package_name}.{module_name}.{func_name}': {e}"
            ) from e
        except Exception as e:
            logging.error(
                "Unexpected error loading packaged UDF: %s\n%s",
                e,
                traceback.format_exc(),
            )
            raise

    def load_udf_from_module(
        self,
        location: str,
        package_name: Optional[str],
        module_name: str,
        func_name: str,
    ) -> Callable:
        """
        Load a UDF from a Python module, supporting both:
        1. Single-file mode: package_name=None, module_name="your_file"
        2. Package mode: package_name="your_pkg", module_name="submodule" or "__init__"

        Args:
            location:
                - In package mode: parent directory of the package
                - In single-file mode: directory containing the .py file
            package_name:
                - If None or empty: treat as single-file mode
                - Else: standard package name
            module_name:
                - In package mode: submodule name (e.g., "main") or "__init__"
                - In single-file mode: filename without .py (e.g., "udf_script")
            func_name: name of the function to load

        Returns:
            The callable UDF function.
        """
        if not os.path.isdir(location):
            raise ValueError(f"Location is not a directory: {location}")

        if not package_name or package_name.strip() == "":
            return self._load_single_file_udf(location, module_name, func_name)
        else:
            return self._load_package_udf(
                location, package_name, module_name, func_name
            )


class UDFLoaderFactory:
    """Factory to select the appropriate loader based on UDF location."""

    @staticmethod
    def get_loader(python_udf_meta: PythonUDFMeta) -> UDFLoader:
        """
        Factory method to create the appropriate UDF loader based on metadata.

        Args:
            python_udf_meta: UDF metadata containing load type and location

        Returns:
            Appropriate UDFLoader instance (InlineUDFLoader or ModuleUDFLoader)

        Raises:
            ValueError: If UDF load type or location is unsupported
        """
        location = python_udf_meta.location
        udf_load_type = python_udf_meta.udf_load_type  # 0: inline, 1: module

        if udf_load_type == 0:
            return InlineUDFLoader(python_udf_meta)
        elif udf_load_type == 1:
            if UDFLoaderFactory.check_module(location):
                return ModuleUDFLoader(python_udf_meta)
            else:
                raise ValueError(f"Unsupported UDF location: {location}")
        else:
            raise ValueError(f"Unsupported UDF load type: {udf_load_type}")

    @staticmethod
    def check_module(location: str) -> bool:
        """
        Checks if a location is a valid Python module or package.

        A valid module is either:
        - A .py file, or
        - A directory containing __init__.py (i.e., a package).

        Raises:
            ValueError: If the location does not exist or contains no Python module.

        Returns:
            True if valid.
        """
        if not os.path.exists(location):
            raise ValueError(f"Module not found: {location}")

        if os.path.isfile(location):
            if location.endswith(".py"):
                return True
            else:
                raise ValueError(f"File is not a Python module (.py): {location}")

        if os.path.isdir(location):
            if UDFLoaderFactory.has_python_file_recursive(location):
                return True
            else:
                raise ValueError(
                    f"Directory contains no Python (.py) files: {location}"
                )

        raise ValueError(f"Invalid module location (not file or directory): {location}")

    @staticmethod
    def has_python_file_recursive(location: str) -> bool:
        """
        Recursively checks if a directory contains any Python (.py) files.

        Args:
            location: Directory path to search

        Returns:
            True if at least one .py file is found, False otherwise
        """
        path = Path(location)
        if not path.is_dir():
            return False
        return any(path.rglob("*.py"))


class UDAFClassLoader:
    """
    Utility class for loading UDAF classes from various sources.

    This class is responsible for loading UDAF classes from:
    - Inline code (embedded in SQL)
    - Module files (imported from filesystem)
    """

    @staticmethod
    def load_udaf_class(python_udf_meta: PythonUDFMeta) -> type:
        """
        Load the UDAF class from metadata.

        Args:
            python_udf_meta: UDAF metadata

        Returns:
            The UDAF class

        Raises:
            RuntimeError: If inline code execution fails
            ValueError: If class is not found or invalid
        """
        loader = UDFLoaderFactory.get_loader(python_udf_meta)

        # For UDAF, we need the class, not an instance
        if isinstance(loader, InlineUDFLoader):
            return UDAFClassLoader.load_from_inline(python_udf_meta)
        elif isinstance(loader, ModuleUDFLoader):
            return UDAFClassLoader.load_from_module(python_udf_meta, loader)
        else:
            raise ValueError(f"Unsupported loader type: {type(loader)}")

    @staticmethod
    def load_from_inline(python_udf_meta: PythonUDFMeta) -> type:
        """
        Load UDAF class from inline code.

        Args:
            python_udf_meta: UDAF metadata with inline code

        Returns:
            The UDAF class
        """
        symbol = python_udf_meta.symbol
        inline_code = python_udf_meta.inline_code.decode("utf-8")
        env: dict[str, Any] = {}

        try:
            exec(inline_code, env)  # nosec B102
        except Exception as e:
            raise RuntimeError(f"Failed to exec inline code: {e}") from e

        udaf_class = env.get(symbol)
        if udaf_class is None:
            raise ValueError(f"UDAF class '{symbol}' not found in inline code")

        if not inspect.isclass(udaf_class):
            raise ValueError(f"'{symbol}' is not a class (type: {type(udaf_class)})")

        UDAFClassLoader.validate_udaf_class(udaf_class)
        return udaf_class

    @staticmethod
    def load_from_module(
        python_udf_meta: PythonUDFMeta, loader: ModuleUDFLoader
    ) -> type:
        """
        Load UDAF class from module file.

        Args:
            python_udf_meta: UDAF metadata with module location
            loader: Module loader instance

        Returns:
            The UDAF class
        """
        symbol = python_udf_meta.symbol
        location = python_udf_meta.location

        package_name, module_name, class_name = loader.parse_symbol(symbol)
        udaf_class = loader.load_udf_from_module(
            location, package_name, module_name, class_name
        )

        if not inspect.isclass(udaf_class):
            raise ValueError(f"'{symbol}' is not a class (type: {type(udaf_class)})")

        UDAFClassLoader.validate_udaf_class(udaf_class)
        return udaf_class

    @staticmethod
    def validate_udaf_class(udaf_class: type):
        """
        Validate that the UDAF class implements required methods.

        Args:
            udaf_class: The class to validate

        Raises:
            ValueError: If class doesn't implement required methods or properties
        """
        required_methods = ["__init__", "accumulate", "merge", "finish"]
        for method in required_methods:
            if not hasattr(udaf_class, method):
                raise ValueError(
                    f"UDAF class must implement '{method}' method. "
                    f"Missing in {udaf_class.__name__}"
                )

        # Check for aggregate_state property
        if not hasattr(udaf_class, "aggregate_state"):
            raise ValueError(
                f"UDAF class must have 'aggregate_state' property. "
                f"Missing in {udaf_class.__name__}"
            )

        # Verify it's actually a property
        try:
            attr = inspect.getattr_static(udaf_class, "aggregate_state")
            if not isinstance(attr, property):
                raise ValueError(
                    f"'aggregate_state' must be a @property in {udaf_class.__name__}"
                )
        except AttributeError:
            raise ValueError(
                f"UDAF class must have 'aggregate_state' property. "
                f"Missing in {udaf_class.__name__}"
            )


class UDAFStateManager:
    """
    Manages UDAF aggregate states for Python UDAF execution.

    This class maintains a mapping from place_id to UDAF instances,
    following the Snowflake UDAF pattern:
    - __init__(): Initialize state
    - aggregate_state: Property returning serializable state
    - accumulate(*args): Add input values
    - merge(other_state): Merge two states
    - finish(): Return final result
    """

    def __init__(self):
        """Initialize the state manager."""
        self.states: Dict[int, Any] = {}  # place_id -> UDAF instance
        self.udaf_class = None  # UDAF class to instantiate
        self._destroy_counter = 0  # Track number of destroys since last GC
        self._gc_threshold = 100  # Trigger GC every N destroys

    def set_udaf_class(self, udaf_class: type):
        """
        Set the UDAF class to use for creating instances.

        Args:
            udaf_class: The UDAF class

        Note:
            Validation is performed by UDAFClassLoader before calling this method.
        """
        self.udaf_class = udaf_class

    def create_state(self, place_id: int) -> None:
        """
        Create a new UDAF state for the given place_id.

        Args:
            place_id: Unique identifier for this aggregate state (globally unique)

        Note:
            This method assumes C++ layer guarantees no concurrent access to the same place_id.
        """
        try:
            self.states[place_id] = self.udaf_class()
        except Exception as e:
            logging.error(
                "Failed to create UDAF state for place_id=%s: %s\nUDAF class: %s\nTraceback: %s",
                place_id,
                e,
                self.udaf_class.__name__ if self.udaf_class else "None",
                traceback.format_exc(),
            )
            raise RuntimeError(f"Failed to create UDAF state: {e}") from e

    def get_state(self, place_id: int) -> Any:
        """
        Get the UDAF state for the given place_id.

        Args:
            place_id: Unique identifier for the aggregate state

        Returns:
            The UDAF instance
        """
        return self.states[place_id]

    def accumulate(self, place_id: int, *args) -> None:
        """
        Accumulate input values into the aggregate state.

        Args:
            place_id: Unique identifier for the aggregate state
            *args: Input values to accumulate
        """
        state = self.states[place_id]
        try:
            state.accumulate(*args)
        except Exception as e:
            logging.error(
                "Error in accumulate for place_id %s: %s",
                place_id,
                e,
            )
            raise RuntimeError(f"Error in accumulate: {e}") from e

    def serialize(self, place_id: int) -> bytes:
        """
        Serialize the aggregate state to bytes.

        Args:
            place_id: Unique identifier for the aggregate state

        Returns:
            Serialized state as bytes (using pickle)
        """
        state = self.states[place_id]
        try:
            aggregate_state = state.aggregate_state
            serialized = pickle.dumps(aggregate_state)
            return serialized
        except Exception as e:
            logging.error(
                "Error serializing state for place_id %s: %s",
                place_id,
                e,
            )
            raise RuntimeError(f"Error serializing state: {e}") from e

    def merge(self, place_id: int, other_state_bytes: bytes) -> None:
        """
        Merge another serialized state into this state.

        Args:
            place_id: Unique identifier for the aggregate state
            other_state_bytes: Serialized state to merge (pickle bytes)
        """
        try:
            other_state = pickle.loads(other_state_bytes)
        except Exception as e:
            logging.error("Error deserializing state bytes: %s", e)
            raise RuntimeError(f"Error deserializing state: {e}") from e

        state = self.states[place_id]

        try:
            state.merge(other_state)
        except Exception as e:
            logging.error(
                "Error in merge for place_id %s: %s",
                place_id,
                e,
            )
            raise RuntimeError(f"Error in merge: {e}") from e

    def finalize(self, place_id: int) -> Any:
        """
        Get the final result from the aggregate state.

        Args:
            place_id: Unique identifier for the aggregate state

        Returns:
            Final aggregation result
        """
        state = self.states[place_id]
        try:
            result = state.finish()
            return result
        except Exception as e:
            logging.error(
                "Error finalizing state for place_id %s: %s",
                place_id,
                e,
            )
            raise RuntimeError(f"Error finalizing state: {e}") from e

    def reset(self, place_id: int) -> None:
        """
        Reset the aggregate state (for window functions).

        Args:
            place_id: Unique identifier for the aggregate state

        Raises:
            RuntimeError: If state does not exist for this place_id or UDAF class not set
        """
        try:
            self.states[place_id] = self.udaf_class()
        except Exception as e:
            logging.error(
                "Error resetting state for place_id %s: %s",
                place_id,
                e,
            )
            raise RuntimeError(f"Error resetting state: {e}") from e

    def destroy(self, place_id: int) -> None:
        """
        Destroy the aggregate state and free resources.

        Args:
            place_id: Unique identifier for the aggregate state
        """
        if place_id not in self.states:
            return

        del self.states[place_id]

        self._destroy_counter += 1
        # Trigger GC periodically based on destroy count
        if self._destroy_counter >= self._gc_threshold:
            remaining = len(self.states)

            # Clear all states - force full cleanup
            if remaining == 0:
                self.states.clear()
                gc.collect()
                logging.debug(
                    "[UDAF GC] Full cleanup: all states destroyed, GC triggered"
                )
            # Many states destroyed recently - trigger GC
            elif self._destroy_counter >= self._gc_threshold:
                gc.collect()
                logging.debug(
                    "[UDAF GC] Periodic GC triggered after %d destroys, %d states remaining",
                    self._destroy_counter,
                    remaining,
                )

            self._destroy_counter = 0


class FlightServer(flight.FlightServerBase):
    """Arrow Flight server for executing Python UDFs, UDAFs, and UDTFs."""

    def __init__(self, location: str):
        """
        Initialize the Flight server.

        Args:
            location: Unix socket path for the server
        """
        super().__init__(location)
        # Use a dictionary to maintain separate state managers for each UDAF function
        # Key: function signature (name + input_types), Value: UDAFStateManager instance
        self.udaf_state_managers: Dict[str, UDAFStateManager] = {}
        self.udaf_managers_lock = threading.Lock()

    def _get_udaf_state_manager(
        self, python_udaf_meta: PythonUDFMeta
    ) -> UDAFStateManager:
        """
        Get or create a state manager for the given UDAF function.
        Each UDAF function gets its own independent state manager.

        Args:
            python_udaf_meta: Metadata for the UDAF function

        Returns:
            UDAFStateManager instance for this specific UDAF
        """
        # Create a unique key based on function name and argument types
        type_names = [str(field.type) for field in python_udaf_meta.input_types]
        func_key = f"{python_udaf_meta.name}({','.join(type_names)})"

        with self.udaf_managers_lock:
            if func_key not in self.udaf_state_managers:
                manager = UDAFStateManager()
                # Load and set the UDAF class for this manager using UDAFClassLoader
                udaf_class = UDAFClassLoader.load_udaf_class(python_udaf_meta)
                manager.set_udaf_class(udaf_class)
                self.udaf_state_managers[func_key] = manager

        return self.udaf_state_managers[func_key]

    @staticmethod
    def parse_python_udf_meta(
        descriptor: flight.FlightDescriptor,
    ) -> Optional[PythonUDFMeta]:
        """
        Parses UDF/UDAF/UDTF metadata from a command descriptor.

        Returns:
            PythonUDFMeta object containing the function metadata
        """

        if descriptor.descriptor_type != flight.DescriptorType.CMD:
            logging.error("Invalid descriptor type: %s", descriptor.descriptor_type)
            return None

        cmd_json = json.loads(descriptor.command)
        name = cmd_json["name"]
        symbol = cmd_json["symbol"]
        location = cmd_json["location"]
        udf_load_type = cmd_json["udf_load_type"]
        runtime_version = cmd_json["runtime_version"]
        always_nullable = cmd_json["always_nullable"]
        # client_type: 0: UDF, 1: UDAF, 2: UDTF
        client_type = cmd_json["client_type"]

        inline_code = base64.b64decode(cmd_json["inline_code"])
        input_binary = base64.b64decode(cmd_json["input_types"])
        output_binary = base64.b64decode(cmd_json["return_type"])

        input_schema = pa.ipc.read_schema(pa.BufferReader(input_binary))
        output_schema = pa.ipc.read_schema(pa.BufferReader(output_binary))

        if len(output_schema) != 1:
            logging.error(
                "Output schema must have exactly one field: %s", output_schema
            )
            return None

        output_type = output_schema.field(0).type

        python_udf_meta = PythonUDFMeta(
            name=name,
            symbol=symbol,
            location=location,
            udf_load_type=udf_load_type,
            runtime_version=runtime_version,
            always_nullable=always_nullable,
            inline_code=inline_code,
            input_types=input_schema,
            output_type=output_type,
            client_type=client_type,
        )

        return python_udf_meta

    @staticmethod
    def check_schema(
        record_batch: pa.RecordBatch, expected_schema: pa.Schema
    ) -> Tuple[bool, str]:
        """
        Validates that the input RecordBatch schema matches the expected schema.
        Checks that field count and types match, but field names can differ.

        :return: (result, error_message)
        """
        actual = record_batch.schema
        expected = expected_schema

        # Check field count
        if len(actual) != len(expected):
            return (
                False,
                f"Schema length mismatch, got {len(actual)} fields, expected {len(expected)} fields",
            )

        # Check each field type (ignore field names)
        for i, (actual_field, expected_field) in enumerate(zip(actual, expected)):
            if not actual_field.type.equals(expected_field.type):
                return False, (
                    f"Type mismatch at field index {i}, "
                    f"got {actual_field.type}, expected {expected_field.type}"
                )

        return True, ""

    def _create_unified_response(
        self, success: bool, rows_processed: int, data: bytes
    ) -> pa.RecordBatch:
        """
        Create unified UDAF response batch.

        Schema: [success: bool, rows_processed: int64, serialized_data: binary]
        """
        return pa.RecordBatch.from_arrays(
            [
                pa.array([success], type=pa.bool_()),
                pa.array([rows_processed], type=pa.int64()),
                pa.array([data], type=pa.binary()),
            ],
            schema=pa.schema(
                [
                    pa.field("success", pa.bool_()),
                    pa.field("rows_processed", pa.int64()),
                    pa.field("serialized_data", pa.binary()),
                ]
            ),
        )

    def _handle_udaf_create(
        self, place_id: int, state_manager: UDAFStateManager
    ) -> pa.RecordBatch:
        """Handle UDAF CREATE operation.

        Returns: [success: bool]
        """
        try:
            state_manager.create_state(place_id)
            success = True
        except Exception as e:
            logging.error(
                "CREATE operation failed for place_id=%s: %s",
                place_id,
                e,
            )
            success = False

        return pa.RecordBatch.from_arrays(
            [pa.array([success], type=pa.bool_())], ["success"]
        )

    def _handle_udaf_accumulate(
        self,
        place_id: int,
        is_single_place: bool,
        row_start: int,
        row_end: int,
        data_batch: pa.RecordBatch,
        state_manager: UDAFStateManager,
    ) -> pa.RecordBatch:
        """
        Handle UDAF ACCUMULATE operation with optimized metadata from app_metadata.

        Args:
            place_id: Primary place identifier
            is_single_place: If True, single aggregation; if False, GROUP BY aggregation
            row_start: Start row index in data batch
            row_end: End row index in data batch (exclusive)
            data_batch: Input data RecordBatch (argument columns + optional places column)
            state_manager: UDAF state manager instance

        Returns: [rows_processed: int64] (0 if failed)
        """
        if data_batch is None:
            raise ValueError("ACCUMULATE requires data_batch, got None")

        rows_processed = 0

        try:
            has_places = (
                data_batch.schema.field(data_batch.num_columns - 1).name == "places"
            )
            num_input_cols = (
                data_batch.num_columns - 1 if has_places else data_batch.num_columns
            )
            loop_start = row_start
            loop_end = min(row_end, data_batch.num_rows)

            if is_single_place:
                if place_id not in state_manager.states:
                    raise KeyError(f"State for place_id {place_id} not found")
                state = state_manager.states[place_id]

                # Extract row range using Arrow slicing (zero-copy)
                sliced_batch = data_batch.slice(loop_start, loop_end - loop_start)
                columns = [sliced_batch.column(j) for j in range(num_input_cols)]

                column_metadata = [
                    data_batch.schema.field(j).metadata
                    for j in range(num_input_cols)
                ]

                for i in range(sliced_batch.num_rows):
                    try:
                        # Apply IP conversion based on metadata
                        row_args = tuple(
                            convert_arrow_field_to_python(col[i], meta)
                            for col, meta in zip(columns, column_metadata)
                        )
                        state.accumulate(*row_args)
                        rows_processed += 1
                    except Exception as e:
                        logging.error(
                            "Error in accumulate for place_id %s at row %d: %s",
                            place_id,
                            loop_start + i,
                            e,
                        )
                        raise RuntimeError(f"Error in accumulate: {e}") from e

                del columns
                del sliced_batch
            else:
                # Multiple places (GROUP BY): iterate row by row
                places_col = data_batch.column(data_batch.num_columns - 1)
                num_rows = data_batch.num_rows
                data_columns = [data_batch.column(j) for j in range(num_input_cols)]

                column_metadata = [
                    data_batch.schema.field(j).metadata
                    for j in range(num_input_cols)
                ]

                # Process each row directly from Arrow arrays (single pass)
                for i in range(num_rows):
                    try:
                        place_id = places_col[i].as_py()
                        state = state_manager.states[place_id]
                        row_args = tuple(
                            convert_arrow_field_to_python(col[i], meta)
                            for col, meta in zip(data_columns, column_metadata)
                        )
                        state.accumulate(*row_args)
                        rows_processed += 1
                    except KeyError:
                        logging.error(
                            "State not found for place_id=%s at row %d. "
                            "CREATE must be called before ACCUMULATE.",
                            place_id,
                            i,
                        )
                        raise
                    except Exception as e:
                        logging.error(
                            "Error in accumulate for place_id %s at row %d: %s",
                            place_id,
                            i,
                            e,
                        )
                        raise RuntimeError(f"Error in accumulate: {e}") from e

                del data_columns
                del places_col
                del data_batch

        except Exception as e:
            logging.error(
                "ACCUMULATE operation failed at row %d: %s\nTraceback: %s",
                rows_processed,
                e,
                traceback.format_exc(),
            )
            raise

        return pa.RecordBatch.from_arrays(
            [pa.array([rows_processed], type=pa.int64())], ["rows_processed"]
        )

    def _handle_udaf_serialize(
        self, place_id: int, state_manager: UDAFStateManager
    ) -> pa.RecordBatch:
        """Handle UDAF SERIALIZE operation.

        Returns: [serialized_state: binary] (empty if failed)
        """
        try:
            serialized = state_manager.serialize(place_id)
        except Exception as e:
            logging.error(
                "SERIALIZE operation failed for place_id=%s: %s",
                place_id,
                e,
            )
            serialized = b""

        return pa.RecordBatch.from_arrays(
            [pa.array([serialized], type=pa.binary())], ["serialized_state"]
        )

    def _handle_udaf_merge(
        self,
        place_id: int,
        data_binary: bytes,
        state_manager: UDAFStateManager,
    ) -> pa.RecordBatch:
        """Handle UDAF MERGE operation.

        data_binary contains the serialized state to merge.
        Returns: [success: bool]
        """
        if data_binary is None:
            raise ValueError(f"MERGE requires data_binary, got None")

        try:
            state_manager.merge(place_id, data_binary)
            success = True
        except Exception as e:
            logging.error(
                "MERGE operation failed for place_id=%s: %s",
                place_id,
                e,
            )
            success = False

        return pa.RecordBatch.from_arrays(
            [pa.array([success], type=pa.bool_())], ["success"]
        )

    def _handle_udaf_finalize(
        self,
        place_id: int,
        output_type: pa.DataType,
        state_manager: UDAFStateManager,
    ) -> pa.RecordBatch:
        """Handle UDAF FINALIZE operation.

        Returns: [result: output_type] (null if failed)
        """
        try:
            result = convert_python_to_arrow_value(state_manager.finalize(place_id), output_type)
        except Exception as e:
            logging.error(
                "FINALIZE operation failed for place_id=%s: %s",
                place_id,
                e,
            )
            result = None

        return pa.RecordBatch.from_arrays(
            [pa.array([result], type=output_type)], ["result"]
        )

    def _handle_udaf_reset(
        self, place_id: int, state_manager: UDAFStateManager
    ) -> pa.RecordBatch:
        """Handle UDAF RESET operation.

        Returns: [success: bool]
        """
        try:
            state_manager.reset(place_id)
            success = True
        except Exception as e:
            logging.error(
                "RESET operation failed for place_id=%s: %s",
                place_id,
                e,
            )
            success = False

        return pa.RecordBatch.from_arrays(
            [pa.array([success], type=pa.bool_())], ["success"]
        )

    def _handle_udaf_destroy(
        self, place_ids: list, state_manager: UDAFStateManager
    ) -> bool:
        """Handle UDAF DESTROY operation for one or more place_ids.

        Args:
            place_ids: List of place_ids to destroy (can be single element)
            state_manager: UDAF state manager

        Returns:
            bool: True if all destroys succeeded, False if any failed
        """
        num_ids = len(place_ids)
        success_count = 0
        failed_count = 0

        for place_id in place_ids:
            try:
                state_manager.destroy(place_id)
                success_count += 1
            except Exception as e:
                logging.error(
                    "Failed to destroy place_id=%s: %s",
                    place_id,
                    e,
                )
                failed_count += 1

        if failed_count > 0:
            if num_ids > 1:
                logging.warning(
                    "[UDAF Memory] Destroy completed with %d succeeded, %d failed",
                    success_count,
                    failed_count,
                )
            return False

        return True

    def _handle_exchange_udf(
        self,
        python_udf_meta: PythonUDFMeta,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
    ) -> None:
        """Handle bidirectional streaming for UDF execution."""
        loader = UDFLoaderFactory.get_loader(python_udf_meta)
        udf = loader.load()
        logging.info("Loaded UDF: %s", udf)

        started = False
        for chunk in reader:
            if not chunk.data:
                logging.info("Empty chunk received, skipping")
                continue

            check_schema_result, error_msg = self.check_schema(
                chunk.data, python_udf_meta.input_types
            )
            if not check_schema_result:
                logging.error("Schema mismatch: %s", error_msg)
                raise ValueError(f"Schema mismatch: {error_msg}")

            result_array = udf(chunk.data)

            if not python_udf_meta.output_type.equals(result_array.type):
                logging.error(
                    "Output type mismatch: got %s, expected %s",
                    result_array.type,
                    python_udf_meta.output_type,
                )
                raise ValueError(
                    f"Output type mismatch: got {result_array.type}, expected {python_udf_meta.output_type}"
                )

            result_batch = pa.RecordBatch.from_arrays([result_array], ["result"])
            if not started:
                try:
                    writer.begin(result_batch.schema)
                    started = True
                except Exception as e:
                    logging.error(
                        "Failed to begin UDF writer stream (client may have disconnected): %s",
                        e,
                    )
                    return

            try:
                writer.write_batch(result_batch)
            except Exception as e:
                logging.error(
                    "Failed to write UDF response batch (client may have disconnected): %s",
                    e,
                )
                return

    def _handle_exchange_udaf(
        self,
        python_udaf_meta: PythonUDFMeta,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
    ) -> None:
        """
        Handle bidirectional streaming for UDAF execution.

        Protocol (optimized with direct RecordBatch transmission):
        - app_metadata: 30-byte binary structure containing:
          * meta_version: uint32 (4 bytes) - Metadata version (currently 1)
          * operation: uint8 (1 byte) - UDAFOperationType enum
          * is_single_place: uint8 (1 byte) - Boolean (ACCUMULATE only)
          * place_id: int64 (8 bytes) - Aggregate state identifier (globally unique)
          * row_start: int64 (8 bytes) - Start row index (ACCUMULATE only)
          * row_end: int64 (8 bytes) - End row index (ACCUMULATE only)

        - RecordBatch data: [argument_types..., places: int64, binary_data: binary]
          * Schema is function-specific: created from argument_types + places + binary_data columns
          * Different operations fill different columns:
            - ACCUMULATE (single-place): data columns are filled, places is NULL, binary_data is NULL
            - ACCUMULATE (multi-place): data columns are filled, places contains place IDs, binary_data is NULL
            - MERGE: data columns are NULL, places is NULL, binary_data contains serialized state
            - Other operations (CREATE/SERIALIZE/FINALIZE/RESET/DESTROY): all columns are NULL
          * places column: indicates which place each row belongs to in GROUP BY scenarios
          * This eliminates extra serialization/deserialization for ACCUMULATE operations

        Response: Unified schema [success: bool, rows_processed: int64, serialized_data: binary]
        - Different operations use different fields:
          * CREATE/MERGE/RESET/DESTROY: use success only
          * ACCUMULATE: use success + rows_processed (number of rows processed)
          * SERIALIZE: use success + serialized_data (serialized_state)
          * FINALIZE: use success + serialized_data (serialized result)
        """

        # Get or create state manager for this specific UDAF function
        state_manager = self._get_udaf_state_manager(python_udaf_meta)
        started = False

        # Define unified response schema (consistent with C++ kUnifiedUDAFResponseSchema)
        unified_schema = pa.schema(
            [
                pa.field("success", pa.bool_()),
                pa.field("rows_processed", pa.int64()),
                pa.field("serialized_data", pa.binary()),
            ]
        )

        for chunk in reader:
            if not chunk.data or chunk.data.num_rows == 0:
                logging.warning("Empty chunk received, skipping")
                continue

            batch = chunk.data
            app_metadata = chunk.app_metadata

            # Validate app_metadata
            if not app_metadata or len(app_metadata) != 30:
                raise ValueError(
                    f"Invalid app_metadata: expected 30 bytes, got {len(app_metadata) if app_metadata else 0}"
                )

            # Parse fixed-size binary metadata (30 bytes total)
            # Layout: meta_version(4) + operation(1) + is_single_place(1) + place_id(8) + row_start(8) + row_end(8)
            metadata_bytes = app_metadata.to_pybytes()

            # Validate metadata version
            meta_version = int.from_bytes(metadata_bytes[0:4], "little", signed=False)
            if meta_version != 1:
                raise ValueError(
                    f"Unsupported metadata version: {meta_version}. Expected version 1. "
                    "Please upgrade the Python server or downgrade the C++ client."
                )
            operation_type = UDAFOperationType(metadata_bytes[4])
            is_single_place = metadata_bytes[5] == 1
            place_id = int.from_bytes(metadata_bytes[6:14], "little", signed=True)
            row_start = int.from_bytes(metadata_bytes[14:22], "little", signed=True)
            row_end = int.from_bytes(metadata_bytes[22:30], "little", signed=True)

            # Extract data from batch
            # RPC schema: [argument_types..., places: int64, binary_data: binary]
            # - Second-to-last column is places (int64)
            # - Last column is binary_data (binary)
            # - ACCUMULATE (single-place): data columns filled, places is NULL, binary_data is NULL
            # - ACCUMULATE (multi-place): data columns filled, places contains place IDs, binary_data is NULL
            # - MERGE: data columns are NULL, places is NULL, binary_data is filled
            # - Other operations: all columns are NULL

            if batch.num_columns < 1:
                raise ValueError(f"Expected at least 1 column, got {batch.num_columns}")

            # Last column is binary_data
            binary_col = batch.column(batch.num_columns - 1)
            binary_data = binary_col[0].as_py() if binary_col[0].is_valid else None

            # Handle different operations and convert to unified format
            try:
                if operation_type == UDAFOperationType.CREATE:
                    result_batch = self._handle_udaf_create(place_id, state_manager)
                    success = result_batch.column(0)[0].as_py()
                    result_batch = self._create_unified_response(
                        success=success, rows_processed=0, data=b""
                    )
                elif operation_type == UDAFOperationType.ACCUMULATE:
                    num_data_cols = batch.num_columns - 1
                    data_batch = pa.RecordBatch.from_arrays(
                        [batch.column(i) for i in range(num_data_cols)],
                        schema=pa.schema(
                            [batch.schema.field(i) for i in range(num_data_cols)]
                        ),
                    )
                    result_batch_accumulate = self._handle_udaf_accumulate(
                        place_id,
                        is_single_place,
                        row_start,
                        row_end,
                        data_batch,
                        state_manager,
                    )
                    rows_processed = result_batch_accumulate.column(0)[0].as_py()
                    result_batch = self._create_unified_response(
                        success=(rows_processed > 0),
                        rows_processed=rows_processed,
                        data=b"",
                    )
                elif operation_type == UDAFOperationType.SERIALIZE:
                    result_batch_serialize = self._handle_udaf_serialize(
                        place_id, state_manager
                    )
                    serialized = result_batch_serialize.column(0)[0].as_py()
                    result_batch = self._create_unified_response(
                        success=(len(serialized) > 0) if serialized else False,
                        rows_processed=0,
                        data=serialized if serialized else b"",
                    )
                elif operation_type == UDAFOperationType.MERGE:
                    # For MERGE: binary_data contains the serialized state
                    result_batch_merge = self._handle_udaf_merge(
                        place_id, binary_data, state_manager
                    )
                    success = result_batch_merge.column(0)[0].as_py()
                    result_batch = self._create_unified_response(
                        success=success, rows_processed=0, data=b""
                    )
                elif operation_type == UDAFOperationType.FINALIZE:
                    result_batch_finalize = self._handle_udaf_finalize(
                        place_id, python_udaf_meta.output_type, state_manager
                    )
                    # Serialize the result to binary (including NULL results)
                    # NULL is a valid aggregation result, not an error
                    sink = pa.BufferOutputStream()
                    ipc_writer = pa.ipc.new_stream(sink, result_batch_finalize.schema)
                    ipc_writer.write_batch(result_batch_finalize)
                    ipc_writer.close()
                    result_data = sink.getvalue().to_pybytes()
                    result_batch = self._create_unified_response(
                        success=True,
                        rows_processed=0,
                        data=result_data,
                    )
                elif operation_type == UDAFOperationType.RESET:
                    result_batch_reset = self._handle_udaf_reset(
                        place_id, state_manager
                    )
                    success = result_batch_reset.column(0)[0].as_py()
                    result_batch = self._create_unified_response(
                        success=success, rows_processed=0, data=b""
                    )
                elif operation_type == UDAFOperationType.DESTROY:
                    if row_end > 1:
                        # Batch destroy mode - binary_data contains serialized place_ids
                        if binary_data is None:
                            raise ValueError("DESTROY_BATCH: binary_data is None")
                        data_reader = pa.ipc.open_stream(binary_data)
                        data_batch = data_reader.read_next_batch()
                        if data_batch.num_columns != 1:
                            raise ValueError(
                                f"DESTROY_BATCH: Expected 1 column (place_ids), got {data_batch.num_columns}"
                            )
                        place_ids_array = data_batch.column(0)
                        place_ids = [
                            place_ids_array[i].as_py()
                            for i in range(len(place_ids_array))
                        ]
                    else:
                        # Single destroy mode
                        place_ids = [place_id]

                    success = self._handle_udaf_destroy(place_ids, state_manager)
                    result_batch = self._create_unified_response(
                        success=success, rows_processed=0, data=b""
                    )
                else:
                    raise ValueError(f"Unsupported operation type: {operation_type}")
            except Exception as e:
                logging.error(
                    "Operation %s failed for place_id=%s: %s\nTraceback: %s",
                    operation_type,
                    place_id,
                    e,
                    traceback.format_exc(),
                )
                result_batch = self._create_unified_response(
                    success=False, rows_processed=0, data=b""
                )

            # Begin stream with unified schema on first call
            if not started:
                try:
                    writer.begin(unified_schema)
                    started = True
                except Exception as e:
                    logging.error(
                        "Failed to begin writer stream (client may have disconnected): %s",
                        e,
                    )
                    # Client disconnected, stop processing
                    return

            try:
                writer.write_batch(result_batch)
            except Exception as e:
                logging.error(
                    "Failed to write response batch (client may have disconnected): %s",
                    e,
                )
                # Client disconnected, stop processing
                return

            del result_batch

    def _handle_exchange_udtf(
        self,
        python_udtf_meta: PythonUDFMeta,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
    ) -> None:
        """
        Handle bidirectional streaming for UDTF execution.

        Protocol (ListArray-based):
        - Input: RecordBatch with input columns
        - Output: RecordBatch with a single ListArray column
          * ListArray automatically manages offsets internally
          * Each list element contains the outputs for one input row

        Example:
          Input: 3 rows
          UDTF yields: Row 0 -> 5 outputs, Row 1 -> 2 outputs, Row 2 -> 3 outputs
          Output: ListArray with 3 elements (one per input row)
            - Element 0: List of 5 structs
            - Element 1: List of 2 structs
            - Element 2: List of 3 structs
        """
        loader = UDFLoaderFactory.get_loader(python_udtf_meta)
        adaptive_udtf = loader.load()
        udtf_func = adaptive_udtf._eval_func
        started = False

        for chunk in reader:
            if not chunk.data:
                logging.info("Empty chunk received, skipping")
                continue

            input_batch = chunk.data

            # Validate input schema
            check_schema_result, error_msg = self.check_schema(
                input_batch, python_udtf_meta.input_types
            )
            if not check_schema_result:
                logging.error("Schema mismatch: %s", error_msg)
                raise ValueError(f"Schema mismatch: {error_msg}")

            # Process all input rows and build ListArray
            try:
                response_batch = self._process_udtf_with_list_array(
                    udtf_func, input_batch, python_udtf_meta.output_type
                )

                # Send the response batch
                if not started:
                    try:
                        writer.begin(response_batch.schema)
                        started = True
                    except Exception as e:
                        logging.error(
                            "Failed to begin UDTF writer stream (client may have disconnected): %s",
                            e,
                        )
                        return

                try:
                    writer.write_batch(response_batch)
                except Exception as e:
                    logging.error(
                        "Failed to write UDTF response batch (client may have disconnected): %s",
                        e,
                    )
                    return

            except Exception as e:
                logging.error(
                    "Error in UDTF execution: %s\nTraceback: %s",
                    e,
                    traceback.format_exc(),
                )
                raise RuntimeError(f"Error in UDTF execution: {e}") from e

    def _process_udtf_with_list_array(
        self,
        udtf_func: Callable,
        input_batch: pa.RecordBatch,
        expected_output_type: pa.DataType,
    ) -> pa.RecordBatch:
        """
        Process UDTF function on all input rows and generate a ListArray.

        Args:
            udtf_func: The UDTF function to call
            input_batch: Input RecordBatch with N rows
            expected_output_type: Expected Arrow type for output data

        Returns:
            RecordBatch with a single ListArray column where each element
            is a list of outputs for the corresponding input row
        """
        all_results = []  # List of lists: one list per input row

        # Check if output is single-field or multi-field
        # For single-field output, we allow yielding scalar values directly
        is_single_field = not pa.types.is_struct(expected_output_type)

        column_metadata = [
            input_batch.schema.field(col_idx).metadata
            for col_idx in range(input_batch.num_columns)
        ]

        # Process each input row
        for row_idx in range(input_batch.num_rows):
            # Extract row as tuple of arguments with IP conversion
            row_args = tuple(
                convert_arrow_field_to_python(
                    input_batch.column(col_idx)[row_idx],
                    column_metadata[col_idx]
                )
                for col_idx in range(input_batch.num_columns)
            )

            # Call UDTF function - it can yield tuples or scalar values (for single-field output)
            result = udtf_func(*row_args)

            # Collect output rows for this input row
            row_outputs = []
            if inspect.isgenerator(result):
                for output_value in result:
                    if is_single_field:
                        # Single-field output: accept both scalar and tuple
                        if isinstance(output_value, tuple):
                            # User provided tuple (e.g., (value,)) - extract scalar
                            if len(output_value) != 1:
                                raise ValueError(
                                    f"Single-field UDTF should yield 1-tuples or scalars, got {len(output_value)}-tuple"
                                )
                            row_outputs.append(
                                output_value[0]
                            )  # Extract scalar from tuple
                        else:
                            # User provided scalar - use directly
                            row_outputs.append(output_value)
                    else:
                        # Multi-field output: must be tuple
                        if not isinstance(output_value, tuple):
                            raise ValueError(
                                f"Multi-field UDTF must yield tuples, got {type(output_value)}"
                            )
                        row_outputs.append(output_value)
            elif result is not None:
                # Function returned a single value instead of yielding
                if is_single_field:
                    # Single-field: accept scalar or tuple
                    if isinstance(result, tuple):
                        if len(result) != 1:
                            raise ValueError(
                                f"Single-field UDTF should return 1-tuple or scalar, got {len(result)}-tuple"
                            )
                        row_outputs.append(result[0])  # Extract scalar from tuple
                    else:
                        row_outputs.append(result)
                else:
                    # Multi-field: must be tuple
                    if not isinstance(result, tuple):
                        raise ValueError(
                            f"Multi-field UDTF must return tuples, got {type(result)}"
                        )
                    row_outputs.append(result)

            all_results.append(row_outputs)

        all_results = convert_python_to_arrow_value(all_results, expected_output_type)

        try:
            list_array = pa.array(all_results, type=pa.list_(expected_output_type))
        except Exception as e:
            logging.error(
                "Failed to create ListArray: %s, element_type: %s",
                e,
                expected_output_type,
            )
            raise RuntimeError(f"Failed to create ListArray: {e}") from e

        # Create RecordBatch with single ListArray column
        schema = pa.schema([pa.field("results", pa.list_(expected_output_type))])
        response_batch = pa.RecordBatch.from_arrays([list_array], schema=schema)

        return response_batch

    def do_exchange(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
    ) -> None:
        """
        Handle bidirectional streaming for UDF, UDAF, and UDTF execution.

        Determines operation type (UDF vs UDAF vs UDTF) from descriptor metadata.
        """
        python_udf_meta = self.parse_python_udf_meta(descriptor)
        if not python_udf_meta:
            raise ValueError("Invalid or missing metadata in descriptor")

        if python_udf_meta.is_udf():
            self._handle_exchange_udf(python_udf_meta, reader, writer)
        elif python_udf_meta.is_udaf():
            self._handle_exchange_udaf(python_udf_meta, reader, writer)
        elif python_udf_meta.is_udtf():
            self._handle_exchange_udtf(python_udf_meta, reader, writer)
        else:
            raise ValueError(f"Unsupported client type: {python_udf_meta.client_type}")


class UDAFOperationType(Enum):
    """Enum representing UDAF operation types."""

    CREATE = 0
    ACCUMULATE = 1
    SERIALIZE = 2
    MERGE = 3
    FINALIZE = 4
    RESET = 5
    DESTROY = 6


def check_unix_socket_path(unix_socket_path: str) -> bool:
    """Validates the Unix domain socket path format."""
    if not unix_socket_path:
        logging.error("Unix socket path is empty")
        return False

    if not unix_socket_path.startswith("grpc+unix://"):
        raise ValueError("gRPC UDS URL must start with 'grpc+unix://'")

    socket_path = unix_socket_path[len("grpc+unix://") :].strip()
    if not socket_path:
        logging.error("Extracted socket path is empty")
        return False

    return True


def main(unix_socket_path: str) -> None:
    """
    Main entry point for the Python UDF/UDAF/UDTF server.

    The server handles UDF, UDAF, and UDTF operations dynamically.
    Operation type is determined from metadata in each request.

    Args:
        unix_socket_path: Base path for the Unix domain socket

    Raises:
        SystemExit: If socket path is invalid or server fails to start
    """
    try:
        if not check_unix_socket_path(unix_socket_path):
            print(f"ERROR: Invalid socket path: {unix_socket_path}", flush=True)
            sys.exit(1)

        current_pid = os.getpid()
        ServerState.unix_socket_path = f"{unix_socket_path}_{current_pid}.sock"

        # Start unified server that handles UDF, UDAF, and UDTF
        server = FlightServer(ServerState.unix_socket_path)

        print(ServerState.PYTHON_SERVER_START_SUCCESS_MSG, flush=True)
        logging.info(
            "##### PYTHON UDF/UDAF/UDTF SERVER STARTED AT %s #####", datetime.now()
        )
        server.wait()

    except Exception as e:
        print(
            f"ERROR: Failed to start Python server: {type(e).__name__}: {e}",
            flush=True,
        )
        tb_lines = traceback.format_exception(type(e), e, e.__traceback__)
        if len(tb_lines) > 1:
            print(f"DETAIL: {tb_lines[-2].strip()}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run an Arrow Flight UDF/UDAF/UDTF server over Unix socket. "
        "The server handles UDF, UDAF, and UDTF operations dynamically."
    )
    parser.add_argument(
        "unix_socket_path",
        type=str,
        help="Path to the Unix socket (e.g., grpc+unix:///path/to/socket)",
    )
    args = parser.parse_args()
    main(args.unix_socket_path)
