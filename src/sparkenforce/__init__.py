"""
sparkenforce: Type validation for PySpark DataFrames.

This module provides a DataFrame type annotation system that allows you to specify
and validate the schema of PySpark DataFrames using Python type hints for both
function arguments and return values.

Example:
    @validate
    def process_data(df: DataFrame["id": int, "name": str, "age": int]) -> DataFrame["result": str]:
        # Function will validate that df has the required columns and types
        # Return value will also be validated against the specified schema
        return spark.createDataFrame([("processed",)], ["result"])

MIT License

Copyright (c) 2026, Agustín Recoba
"""

import datetime
import decimal
import inspect
import logging
from functools import wraps
from typing import Any, Dict, Set, TypeVar, Union, get_type_hints, Tuple, Generic, Type

try:
    from typing import get_args, get_origin
except ImportError:

    def get_args(t: Type):
        return getattr(t, "__args__", ()) if t is not Generic else Generic

    def get_origin(t: Type):
        return getattr(t, "__origin__", None)


from pyspark.sql import DataFrame
from pyspark.sql import types as spark_types

__all__ = [
    "validate",
    "TypedDataFrame",
    "register_type_mapping",
    "infer_dataframe_annotation",
    "DataFrameValidationError",
]


class DataFrameValidationError(TypeError):
    """Raised when DataFrame validation fails."""


T = TypeVar("T", bound=callable)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def validate(func: T) -> T:
    """
    Decorator that validates function arguments and return values annotated with DataFrame types.

    Args:
        func: Function to decorate with validation

    Returns:
        Wrapped function with validation logic

    Raises:
        DataFrameValidationError: When validation fails

    Example:
    ```
        @validate
        def process(data: DataFrame["id", "name"]) -> DataFrame["result": str]:
            return spark.createDataFrame([("success",)], ["result"])
    ```
    """
    signature = inspect.signature(func)
    hints = get_type_hints(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)

        # Validate input arguments
        for argument_name, value in bound.arguments.items():
            if argument_name in hints and isinstance(
                hints[argument_name],
                _TypedDataFrameMeta,
            ):
                hint = hints[argument_name]
                _validate_dataframe(value, hint, argument_name)

        # Execute the function
        result = func(*args, **kwargs)

        # Validate return value if annotated with DataFrame type
        return_annotation = hints.get("return")
        if return_annotation is not None and isinstance(
            return_annotation,
            _TypedDataFrameMeta,
        ):
            _validate_dataframe(result, return_annotation, "return value")

        return result

    return wrapper


def _validate_dataframe(value: object, hint: "_TypedDataFrameMeta", argument_name: str) -> None:
    """
    Validate a single DataFrame against a DataFrame hint.

    Args:
        value: Value to validate
        hint: DataFrame metadata with validation rules
        argument_name: Name of the argument being validated

    Raises:
        DataFrameValidationError: When validation fails
    """
    logger.debug(f"Validating '{argument_name}' with hint: {hint}")
    if not isinstance(value, DataFrame):
        raise DataFrameValidationError(
            f"{argument_name} must be a PySpark DataFrame, got {type(value)}",
        )

    columns = set(value.columns)
    logger.debug(f"DataFrame columns: {columns}")
    logger.debug(f"Hint columns: {hint.columns}")
    logger.debug(f"Hint only_specified: {hint.only_specified}")

    # Check column presence
    required_columns = hint.columns - getattr(hint, "optional", set())
    logger.debug(f"Required columns: {required_columns}")

    if hint.only_specified:
        missing = required_columns - columns
        extra = columns - hint.columns
        logger.debug(f"Missing columns: {missing}")
        logger.debug(f"Extra columns: {extra}")

        if missing or extra:
            msg_parts = []
            if missing:
                msg_parts.append(f"missing required columns: {missing}")
            if extra:
                msg_parts.append(f"unexpected columns: {extra}")
            raise DataFrameValidationError(
                f"{argument_name} columns mismatch. {', '.join(msg_parts)}",
            )
    else:
        missing = required_columns - columns
        if missing:
            raise DataFrameValidationError(
                f"{argument_name} is missing required columns: {missing}",
            )

    # Check data types
    if hint.dtypes:
        logger.debug(f"Validating dtypes for '{argument_name}': {hint.dtypes}")
        _validate_dtypes(value, hint.dtypes, argument_name)


_supported_python_types = (
    str,
    int,
    float,
    bool,
    type(None),
    decimal.Decimal,
    datetime.date,
    datetime.datetime,
    datetime.time,
)


def _validate_dtypes(
    df: DataFrame,
    expected_dtypes: Dict[str, Any],
    argument_name: str,
) -> None:
    """
    Validate DataFrame column types.

    Args:
        df: DataFrame to validate
        expected_dtypes: Expected column types
        argument_name: Name of the argument being validated

    Raises:
        DataFrameValidationError: When type validation fails
    """
    actual_dtypes = dict(df.dtypes)

    for col_name, expected_type in expected_dtypes.items():
        if col_name not in actual_dtypes:
            continue  # Column presence already validated

        actual_type_str = actual_dtypes[col_name]
        if hasattr(spark_types.DataType, "fromDDL"):
            actual_spark_type = spark_types.DataType.fromDDL(actual_type_str)
        elif hasattr(spark_types, "_parse_datatype_string"):
            actual_spark_type = spark_types._parse_datatype_string(actual_type_str)

        if isinstance(expected_type, type) and issubclass(expected_type, spark_types.AtomicType):
            # If expected type is a subclass of Spark DataType, instantiate it
            try:
                expected_spark_type = expected_type()
            except TypeError as e:
                raise TypeError(f"Cannot instantiate Spark type {expected_type.__name__}: {e}") from e
        elif isinstance(expected_type, spark_types.DataType):
            # If expected type is already a Spark type, use it directly
            expected_spark_type = expected_type
        elif not isinstance(expected_type, type):
            raise TypeError(
                f"Expected type for DataFrame column '{col_name}' must be a type, got {expected_type} instead.",
            )
        elif issubclass(expected_type, _supported_python_types) or issubclass(
            expected_type, tuple(_custom_type_mappings.keys())
        ):
            # Convert expected type to Spark type
            expected_spark_type = _convert_to_spark_type(expected_type)
        else:
            raise TypeError(
                f"Unsupported type for DataFrame column '{col_name}': {expected_type}. "
                f"Expected a Spark DataType or a supported Python type.",
            )

        if not _types_compatible(actual_spark_type, expected_spark_type):
            raise DataFrameValidationError(
                f"{argument_name} column '{col_name}' has incorrect type. "
                f"Expected {expected_spark_type}, got {actual_spark_type}",
            )


# Custom type mapping registry for user extensions
_custom_type_mappings: Dict[type, spark_types.DataType] = {}


def register_type_mapping(
    python_type: type,
    spark_type: spark_types.DataType,
) -> None:
    """
    Register a custom mapping from a Python type to a Spark DataType.

    Args:
        python_type: The Python type to map.
        spark_type: The corresponding Spark DataType class.
    """
    _custom_type_mappings[python_type] = spark_type


def _convert_to_spark_type(python_type: Any) -> spark_types.DataType:
    """
    Convert Python type to Spark DataType.

    Args:
        python_type: Python type to convert

    Returns:
        Corresponding Spark DataType class

    Raises:
        TypeError: When python_type is not a supported type
    """
    # Check custom user-registered mappings first
    if python_type in _custom_type_mappings:
        return _custom_type_mappings[python_type]

    if python_type not in _supported_python_types and not isinstance(python_type, type):
        raise TypeError(
            f"Unsupported type for DataFrame column validation: {python_type}. "
            f"Supported types are: {', '.join(t.__name__ for t in _supported_python_types)} "
            f"or any subclass of pyspark.sql.types.DataType",
        )

    if isinstance(python_type, type) and issubclass(python_type, spark_types.DataType):
        try:
            return python_type()
        except TypeError as e:
            raise TypeError(f"Cannot instantiate Spark type {python_type.__name__}: {e}") from e

    if python_type in spark_types._type_mappings:
        return spark_types._type_mappings[python_type]()

    # Explicit mapping for common types
    type_mapping = {
        type(None): spark_types.NullType,
        bool: spark_types.BooleanType,
        int: spark_types.IntegerType,
        float: spark_types.FloatType,
        str: spark_types.StringType,
        bytearray: spark_types.BinaryType,
        decimal.Decimal: spark_types.DecimalType,
        datetime.date: spark_types.DateType,
        datetime.datetime: spark_types.TimestampType,
        datetime.time: spark_types.TimestampType,
    }

    if python_type in type_mapping:
        print(f"Using built-in type mapping for {python_type.__name__} -> {type_mapping[python_type]}")
        return type_mapping[python_type]()

    # Raise error for unsupported types instead of silent fallback
    supported_types = list(type_mapping.keys())
    raise TypeError(
        f"Unsupported type for DataFrame column validation: {python_type}. "
        f"Supported types are: {', '.join(str(t) for t in supported_types)} "
        f"or any subclass of pyspark.sql.types.DataType",
    )


def _types_compatible(actual: spark_types.DataType, expected: spark_types.DataType) -> bool:
    """
    Check if actual Spark type is compatible with expected type.

    Args:
        actual: Actual Spark DataType instance
        expected: Expected Spark DataType instance

    Returns:
        True if types are compatible
    """
    # Direct type match
    if actual == expected:
        return True

    # Define compatible type groups for more flexible validation
    integer_types = (
        spark_types.IntegerType,
        spark_types.LongType,
        spark_types.ShortType,
        spark_types.ByteType,
    )
    float_types = (spark_types.FloatType, spark_types.DoubleType)
    string_types = (spark_types.StringType, spark_types.VarcharType)
    timestamp_types = (spark_types.TimestampType, spark_types.TimestampNTZType)
    date_types = (
        spark_types.TimestampType,
        spark_types.TimestampNTZType,
        spark_types.DateType,
    )

    # Check if actual and expected types are in the same compatibility group
    compatibility_groups = [
        integer_types,
        float_types,
        string_types,
        timestamp_types,
        date_types,
    ]

    return any(issubclass(type(actual), group) and type(expected) in group for group in compatibility_groups)


def _get_columns_dtypes(parameters: Any) -> Tuple[Set[str], Dict[str, Any], Set[str]]:
    """
    Extract column names, types, and optional columns from DataFrame parameters.

    Args:
        parameters: DataFrame parameters to parse

    Returns:
        Tuple of (column_names, column_types, optional_columns)

    Raises:
        TypeError: When parameters are invalid or types are unsupported
    """
    columns: Set[str] = set()
    dtypes: Dict[str, Any] = {}
    optional: Set[str] = set()

    if isinstance(parameters, str):
        columns.add(parameters)
    elif isinstance(parameters, slice):
        if not isinstance(parameters.start, str):
            raise TypeError("Column name must be a string")
        columns.add(parameters.start)
        col_type = parameters.stop
        # Detect Optional[...] type
        origin = get_origin(col_type)
        args = get_args(col_type)
        if origin is Union and type(None) in args:
            # Optional[X] is Union[X, NoneType]
            actual_type = [a for a in args if a is not type(None)][0]
            dtypes[parameters.start] = actual_type
            optional.add(parameters.start)
        else:
            dtypes[parameters.start] = col_type
    elif isinstance(parameters, (list, tuple, set)):
        for element in parameters:
            sub_columns, sub_dtypes, sub_optional = _get_columns_dtypes(element)
            columns.update(sub_columns)
            dtypes.update(sub_dtypes)
            optional.update(sub_optional)
    elif isinstance(parameters, _TypedDataFrameMeta):
        columns.update(parameters.columns)
        dtypes.update(parameters.dtypes)
        if hasattr(parameters, "optional"):
            optional.update(parameters.optional)
    else:
        raise TypeError(
            f"DataFrame parameters must be strings, slices, lists, or DataFrameMeta instances. Got {type(parameters)}",
        )
    return columns, dtypes, optional


class _TypedDataFrameMeta(type):
    """
    Metaclass for DataFrame type annotations.

    This metaclass handles the creation of DataFrame types with column and type specifications.
    """

    only_specified: bool
    columns: Set[str]
    dtypes: Dict[str, Any]
    optional: Set[str]

    def __getitem__(cls, parameters: object) -> "_TypedDataFrameMeta":
        """
        Create a DataFrame type with specified columns and types.

        Args:
            parameters: Column specifications (strings, slices, lists, etc.)

        Returns:
            New DataFrameMeta instance with validation rules

        Example:
            DataFrame["id", "name"]  # Exact columns
            DataFrame["id": int, "name": str]  # With types
            DataFrame["id", "name", ...]  # Minimum columns
        """
        if not isinstance(parameters, tuple):
            parameters = (parameters,)

        parameters = list(parameters)

        # Check for ellipsis (indicates minimum columns, not exact match)
        only_specified = True
        if parameters and parameters[-1] is Ellipsis:
            only_specified = False
            parameters.pop()

        columns, dtypes, optional = _get_columns_dtypes(parameters)

        # Create new metaclass instance
        meta = _TypedDataFrameMeta(
            cls.__name__,
            cls.__bases__ if hasattr(cls, "__bases__") else (),
            {},
        )
        meta.only_specified = only_specified
        meta.columns = columns
        meta.dtypes = dtypes
        meta.optional = optional

        return meta

    def __repr__(cls) -> str:
        """String representation of DataFrame type."""
        if hasattr(cls, "dtypes") and cls.dtypes:
            type_strs = [
                f"{col}: {dt.__name__ if hasattr(dt, '__name__') else str(dt)}" for col, dt in cls.dtypes.items()
            ]
            return f"{cls.__name__}[{', '.join(type_strs)}]"

        if hasattr(cls, "columns") and cls.columns:
            return f"{cls.__name__}[{', '.join(sorted(cls.columns))}]"

        return cls.__name__


class TypedDataFrame(DataFrame, metaclass=_TypedDataFrameMeta):
    """
    Alias for DataFrame with TypedDataFrame metaclass.

    This class exists to provide an alternative name for DataFrame type annotations and
    give the developer the option to be more explicit in their code.
    """

    __class_getitem__ = _TypedDataFrameMeta.__getitem__


DataFrame.__class_getitem__ = classmethod(_TypedDataFrameMeta.__getitem__)


def infer_dataframe_annotation(df: DataFrame) -> str:
    """
    Infer a DataFrame[...] annotation string from a PySpark DataFrame's schema.

    Args:
        df: The PySpark DataFrame to inspect.

    Returns:
        A string representing the DataFrame annotation, e.g.:
        'DataFrame["id": int, "name": str]'
    """
    spark_to_py = {
        spark_types.StringType: str,
        spark_types.BooleanType: bool,
        spark_types.ByteType: int,
        spark_types.ShortType: int,
        spark_types.IntegerType: int,
        spark_types.LongType: int,
        spark_types.FloatType: float,
        spark_types.DoubleType: float,
        spark_types.DecimalType: decimal.Decimal,
        spark_types.DateType: datetime.date,
        spark_types.TimestampType: datetime.datetime,
        spark_types.BinaryType: bytearray,
        spark_types.NullType: type(None),
    }
    fields = []
    for field in df.schema.fields:
        py_type = None
        for spark_type, candidate_py in spark_to_py.items():
            if isinstance(field.dataType, spark_type):
                py_type = candidate_py
                break
        if py_type is None:
            py_type = type(field.dataType)  # fallback to Spark type class
        fields.append(
            f'"{field.name}": {py_type.__name__ if hasattr(py_type, "__name__") else str(py_type)}',
        )
    return f"DataFrame[{', '.join(fields)}]"
