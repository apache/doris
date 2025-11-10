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
import importlib
import inspect
import json
import sys
import os
import traceback
import logging
import time
import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Callable, Optional, Tuple, get_origin
from datetime import datetime
from enum import Enum
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyarrow import flight


class ServerState:
    """Global server state container."""

    unix_socket_path: str = ""

    @staticmethod
    def setup_logging():
        """Setup logging configuration for the UDF server."""
        doris_home = os.getenv("DORIS_HOME")
        if not doris_home:
            # Fallback to current directory if DORIS_HOME is not set
            doris_home = os.getcwd()

        log_dir = os.path.join(doris_home, "lib", "udf", "python")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "python_udf_output.log")

        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
            handlers=[
                logging.FileHandler(log_file, mode="a", encoding="utf-8"),
                logging.StreamHandler(sys.stderr),  # Also log to stderr for debugging
            ],
        )
        logging.info("Logging initialized. Log file: %s", log_file)

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
        logging.debug("Temporarily added to sys.path: %s", path)

    try:
        yield
    finally:
        if path_added and path in sys.path:
            sys.path.remove(path)
            logging.debug("Removed from sys.path: %s", path)


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

    def __str__(self) -> str:
        """Returns a string representation of the UDF metadata."""
        udf_load_type_str = "INLINE" if self.udf_load_type == 0 else "MODULE"
        return (
            f"PythonUDFMeta(name={self.name}, symbol={self.symbol}, "
            f"location={self.location}, udf_load_type={udf_load_type_str}, runtime_version={self.runtime_version}, "
            f"always_nullable={self.always_nullable}, inline_code={self.inline_code}, "
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
            logging.info("Using vectorized mode for UDF: %s", self.python_udf_meta.name)
            return self._vectorized_call(record_batch)

        logging.info("Using scalar mode for UDF: %s", self.python_udf_meta.name)
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

    def _convert_from_arrow_to_py(self, field):
        if field is None:
            return None

        if pa.types.is_map(field.type):
            # pyarrow.lib.MapScalar's as_py() returns a list of tuples, convert to dict
            list_of_tuples = field.as_py()
            return dict(list_of_tuples) if list_of_tuples is not None else None
        return field.as_py()

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

        for i in range(num_rows):
            converted_args = [self._convert_from_arrow_to_py(col[i]) for col in columns]

            try:
                res = self._eval_func(*converted_args)
                # Check if result is None when always_nullable is False
                if res is None and not self.python_udf_meta.always_nullable:
                    raise RuntimeError(
                        f"the result of row {i} is null, but the return type is not nullable, "
                        f"please check the always_nullable property in create function statement, "
                        f"it should be true"
                    )
                result.append(res)
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

        return pa.array(result, type=self._get_output_type(), from_pandas=True)

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

        # Convert result to PyArrow Array
        result_array = None
        if isinstance(result, pa.Array):
            result_array = result
        elif isinstance(result, pa.ChunkedArray):
            # Combine chunks into a single array
            result_array = pa.concat_arrays(result.chunks)
        elif isinstance(result, pd.Series):
            result_array = pa.array(result, type=self._get_output_type())
        elif isinstance(result, list):
            result_array = pa.array(
                result, type=self._get_output_type(), from_pandas=True
            )
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
        logging.info("Loading inline code for function '%s'", symbol)
        logging.debug("Inline code:\n%s", inline_code)

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

        logging.info("Successfully loaded function '%s' from inline code", symbol)
        return AdaptivePythonUDF(self.python_udf_meta, func)


class ModuleUDFLoader(UDFLoader):
    """Loads a UDF from a Python module file (.py)."""

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

        logging.info(
            "Successfully loaded function '%s' from module: %s", symbol, location
        )
        return AdaptivePythonUDF(self.python_udf_meta, func)

    def parse_symbol(self, symbol: str):
        """
        Parse symbol into (package_name, module_name, func_name)

        Supported formats:
        - "module.func"           → (None, module, func)
        - "package.module.func"   → (package, "module", func)
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

        logging.debug(
            "Parsed symbol: package=%s, module=%s, func=%s",
            package_name,
            module_name,
            func_name,
        )
        return package_name, module_name, func_name

    def _validate_location(self, location: str) -> None:
        """Validate that the location is a valid directory."""
        if not os.path.isdir(location):
            raise ValueError(f"Location is not a directory: {location}")

    def _get_or_import_module(self, location: str, full_module_name: str) -> Any:
        """Get module from cache or import it."""
        if full_module_name in sys.modules:
            logging.warning(
                "Module '%s' already loaded, using cached version", full_module_name
            )
            return sys.modules[full_module_name]

        with temporary_sys_path(location):
            return importlib.import_module(full_module_name)

    def _extract_function(
        self, module: Any, func_name: str, module_name: str
    ) -> Callable:
        """Extract and validate function from module."""
        func = getattr(module, func_name, None)
        if func is None:
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
                logging.info("Created __init__.py in %s", package_path)
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
        self._validate_location(location)

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


class UDFFlightServer(flight.FlightServerBase):
    """Arrow Flight server for executing Python UDFs."""

    @staticmethod
    def parse_python_udf_meta(
        descriptor: flight.FlightDescriptor,
    ) -> Optional[PythonUDFMeta]:
        """Parses UDF metadata from a command descriptor."""

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

        return PythonUDFMeta(
            name=name,
            symbol=symbol,
            location=location,
            udf_load_type=udf_load_type,
            runtime_version=runtime_version,
            always_nullable=always_nullable,
            inline_code=inline_code,
            input_types=input_schema,
            output_type=output_type,
        )

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

        logging.info(f"Actual schema: {actual}")
        logging.info(f"Expected schema: {expected}")

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

    def do_exchange(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
    ) -> None:
        """Handles bidirectional streaming UDF execution."""
        logging.info("Received exchange request for UDF: %s", descriptor)

        python_udf_meta = UDFFlightServer.parse_python_udf_meta(descriptor)
        if not python_udf_meta:
            raise ValueError("Invalid or missing UDF metadata in descriptor")

        loader = UDFLoaderFactory.get_loader(python_udf_meta)
        udf = loader.load()
        logging.info("Loaded UDF: %s", udf)

        started = False
        for chunk in reader:
            if not chunk.data:
                logging.info("Empty chunk received, skipping")
                continue

            check_schema_result, error_msg = UDFFlightServer.check_schema(
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
                writer.begin(result_batch.schema)
                started = True
            writer.write_batch(result_batch)


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
    Main entry point for the Python UDF server.

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
        server = UDFFlightServer(ServerState.unix_socket_path)
        print("Start python server successfully", flush=True)
        logging.info("##### PYTHON UDF SERVER STARTED AT %s #####", datetime.now())
        server.wait()

    except Exception as e:
        print(
            f"ERROR: Failed to start Python UDF server: {type(e).__name__}: {e}",
            flush=True,
        )
        tb_lines = traceback.format_exception(type(e), e, e.__traceback__)
        if len(tb_lines) > 1:
            print(f"DETAIL: {tb_lines[-2].strip()}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run an Arrow Flight UDF server over Unix socket."
    )
    parser.add_argument(
        "unix_socket_path",
        type=str,
        help="Path to the Unix socket (e.g., grpc+unix:///path/to/socket)",
    )
    args = parser.parse_args()
    main(args.unix_socket_path)
