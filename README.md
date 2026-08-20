![PyPI - Version](https://img.shields.io/pypi/v/sparkenforce?label=Latest%20version&link=https%3A%2F%2Fpypi.org%2Fproject%2Fsparkenforce%2F)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/shuffle-works/sparkenforce/pypi-publish.yml?label=Build%20and%20publish%20to%20PyPi)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/shuffle-works/sparkenforce/ci.yml?label=tests)
[![Coverage Status](https://coveralls.io/repos/github/shuffle-works/sparkenforce/badge.svg?branch=feat/cicd)](https://coveralls.io/github/shuffle-works/sparkenforce?branch=feat/cicd)
[![CodeFactor](https://www.codefactor.io/repository/github/shuffle-works/sparkenforce/badge)](https://www.codefactor.io/repository/github/shuffle-works/sparkenforce)

# sparkenforce

**sparkenforce** is a type annotation system that lets you specify and enforce PySpark DataFrame schemas using Python type hints. It validates both function arguments and return values, catching schema mismatches before they cause runtime errors.

## Why sparkenforce?

Working with PySpark DataFrames can be error-prone when schemas don't match expectations. **sparkenforce** helps by:

- **Preventing runtime errors**: Catch schema mismatches early
- **Improving code clarity**: Function signatures show exactly what DataFrame structure is expected
- **Enforcing contracts**: Ensure functions return DataFrames with the promised schema
- **Better debugging**: Clear error messages when validations fail

## Getting Started

### Installation

Install sparkenforce using pip:

```bash
pip install sparkenforce
```

Or if you're using uv:

```bash
uv add sparkenforce
```

### Specify DataFrame schema

Use `sparkenforce` only for type-hinting.

```python
import sparkenforce
from pyspark.sql import functions as fn
from pyspark.sql import DataFrame

def add_length(df: DataFrame['firstname': str, ...]) -> DataFrame['name': str, 'length': int]:
    return df.select(
        df.firstname.alias('name'),
        fn.length(df.firstname).alias('length')
    )
```

### Validating Input DataFrames

Add runtime validation everytime your function is called.

```python
from sparkenforce import validate
from pyspark.sql import functions as fn
from pyspark.sql import DataFrame

@validate
def add_length(df: DataFrame['firstname': str]):
    return df.select(
        fn.col("firstname").alias('name'),
        fn.length("firstname").alias('length')
    )

# If input DataFrame doesn't have 'firstname' column, validation fails with an exception.
```

### Flexible Schemas with Ellipsis

Use `...` to allow additional columns beyond the specified ones:

```python
@validate
def filter_names(df: DataFrame['firstname': str, 'lastname': str, ...]):
    """Requires firstname and lastname, but allows other columns too."""
    return df.filter(df.firstname != "")
```

### Return Value Validation

`sparkenforce` validates that your function returns exactly what you promise:

```python
@validate
def get_summary(df: DataFrame['firstname': str, ...]) -> DataFrame['firstname': str, 'summary': str]:
    return df.select(
        'firstname',
        fn.lit('processed').alias('summary'),
    )
```

### Error Handling

When validation fails, sparkenforce provides clear error messages:

```python
# This will raise DataFrameValidationError with detailed message:
# "return value columns mismatch. Expected exactly {'name', 'length'},
#  got {'lastname', 'firstname'}. missing columns: {'name', 'length'},
#  unexpected columns: {'lastname', 'firstname'}"

@validate
def bad_function(df: DataFrame['firstname': str, ...]) -> DataFrame['name': str, 'length': int]:
    return df.select('firstname', 'lastname')  # Wrong columns!
```

### More Examples

Check out the [examples notebook.](https://github.com/shuffle-works/sparkenforce/blob/main/src/demo/demo_notebook.ipynb)

# API Reference

## Core Components

### `@validate` Decorator

The main decorator for enabling DataFrame schema validation on functions.

```python
@validate
def process_data(df: DataFrame["id": int, "name": str]) -> DataFrame["result": str]:
    return spark.createDataFrame([("processed",)], ["result"])
```

**Signature:** `validate(func: Callable) -> Callable`

**Parameters:**
- `func` - Function to decorate with validation logic

**Returns:**
- Wrapped function that validates DataFrame arguments and return values

**Raises:**
- `DataFrameValidationError` - When schema validation fails

**Validation Rules:**
- Validates all function parameters annotated with `DataFrame[...]` types
- Validates return values if annotated with `DataFrame[...]` types
- Functions without DataFrame annotations are not validated
- Return type `None` or no return annotation skips return validation

### DataFrame Type Annotations

sparkenforce extends PySpark's DataFrame class to support schema specifications using subscript notation.

#### Column-Only Validation
```python
DataFrame["id", "name"]       # Requires exactly these columns
DataFrame["id", "name", ...]  # Requires at least these columns
```

#### Column + Type Validation
```python
DataFrame["id": int, "name": str]            # Exact columns with types
DataFrame["id": int, "name": str, ...]       # Minimum columns with types
DataFrame["id": int, "name": Optional[str]]  # Optional columns (may not be present)
```

#### Supported Types

**Python Types:**
- `int` → `LongType` (with compatibility for `IntegerType`, `ShortType`, `ByteType`)
- `str` → `StringType`
- `float` → `DoubleType` (with compatibility for `FloatType`)
- `bool` → `BooleanType`
- `datetime.datetime` → `TimestampType`
- `datetime.date` → `DateType`
- `decimal.Decimal` → `DecimalType`
- `bytearray` → `BinaryType`

**Spark Types:**
Any `pyspark.sql.types.DataType` subclass can be used directly:
```python
from pyspark.sql.types import IntegerType, StringType
DataFrame["id": IntegerType, "name": StringType]
```

**Custom Types:**
Register custom type mappings for complex types:
```python
@dataclass
class Person:
    name: str
    age: int

person_struct = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

register_type_mapping(Person, person_struct)
DataFrame["person": Person]  # Now supported
```

## Functions

### `register_type_mapping(python_type, spark_type)`

Register custom mappings between Python types and Spark DataTypes.

```python
from dataclasses import dataclass
from pyspark.sql.types import StructType, StructField, StringType

@dataclass
class Name:
    first: str
    last: str

name_type = StructType([
    StructField("first", StringType(), True),
    StructField("last", StringType(), True)
])

register_type_mapping(Name, name_type)

@validate
def process_names(df: DataFrame["person": Name]) -> DataFrame["full_name": str]:
    return df.select(concat(col("person.first"), lit(" "), col("person.last")).alias("full_name"))
```

**Parameters:**
- `python_type` (`type`) - Python type or class to register
- `spark_type` (`pyspark.sql.types.DataType`) - Corresponding Spark DataType instance

**Use Cases:**
- Complex nested structures using dataclasses
- Custom business domain types
- Third-party type integration

### `infer_dataframe_annotation(df)`

Generate DataFrame type annotation strings from existing DataFrames.

```python
df = spark.createDataFrame([
    (1, "Alice", 25.5),
    (2, "Bob", 30.0)
], ["id", "name", "score"])

annotation = infer_dataframe_annotation(df)
# Returns: 'DataFrame["id": int, "name": str, "score": float]'
```

**Parameters:**
- `df` (`pyspark.sql.DataFrame`) - DataFrame to analyze

**Returns:**
- `str` - Type annotation string ready for copy-paste into code

**Use Cases:**
- Reverse engineering schemas from existing DataFrames
- Generating boilerplate for new functions
- Documentation and debugging

## Classes

### `TypedDataFrame`

Alternative name for DataFrame type annotations, providing explicit typing semantics.

```python
from sparkenforce import TypedDataFrame

@validate
def process(data: TypedDataFrame["id": int, "name": str]) -> TypedDataFrame["result": str]:
    # Functionally identical to DataFrame[...] but more explicit
    return spark.createDataFrame([("success",)], ["result"])
```

**Usage:**
- Drop-in replacement for `DataFrame[...]` annotations
- Provides clearer semantic meaning in domain-specific code
- Same validation behavior as DataFrame

## Exceptions

### `DataFrameValidationError`

Raised when DataFrame schema validation fails.

```python
class DataFrameValidationError(TypeError):
    """Raised when DataFrame validation fails."""
```

**Common Scenarios:**

**Missing Columns:**
```
DataFrameValidationError: argument 'df' is missing required columns: {'name'}
```

**Type Mismatches:**
```
DataFrameValidationError: argument 'df' column 'age' has incorrect type. Expected LongType(), got StringType()
```

**Return Value Errors:**
```
DataFrameValidationError: return value must be a PySpark DataFrame, got <class 'str'>
```

## Advanced Usage

### Optional Columns

Use `typing.Optional` to mark columns as not always present:

```python
from typing import Optional

@validate
def process(df: DataFrame["id": int, "name": Optional[str]]) -> DataFrame["result": str]:
    # 'name' column may be missing; function should handle that case
    if 'name' in df.columns:
        return df.select(fn.concat(col("name"), fn.lit(" processed")).alias("result"))
    else:
        return df.select(fn.lit("no name").alias("result"))
```

### Flexible Schemas

Use ellipsis (`...`) for minimum column requirements:

```python
@validate
def add_metadata(df: DataFrame["id": int, ...]) -> DataFrame["id": int, "processed": bool, ...]:
    # Input: requires 'id' column, allows others
    # Output: guarantees 'id' and 'processed' columns, allows others
    return df.withColumn("processed", lit(True))
```

### Type Compatibility

sparkenforce provides intelligent type compatibility:

- Integer types (`ByteType`, `ShortType`, `IntegerType`, `LongType`) are interchangeable
- Float types (`FloatType`, `DoubleType`) are interchangeable
- Timestamp types (`TimestampType`, `TimestampNTZType`) are interchangeable
- String variants (`StringType`, `VarcharType`) are interchangeable


# Inspiration

This project builds on [dataenforce](https://github.com/CedricFR/dataenforce), extending it with additional validation capabilities for PySpark DataFrame workflows.

# License

Apache Software License v2.0

# Contact

Created by [Agustín Recoba](https://github.com/agustin-recoba)
