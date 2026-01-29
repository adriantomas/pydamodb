# PydamoDB

[![Python 3.10 | 3.11 | 3.12 | 3.13 | 3.14](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/pydamodb.svg)](https://pypi.org/project/pydamodb/)
[![codecov](https://codecov.io/github/adriantomas/pydamodb/graph/badge.svg?token=NP5RA8KV66)](https://codecov.io/github/adriantomas/pydamodb)
[![Pydantic v2](https://img.shields.io/badge/pydantic-v2-green.svg)](https://docs.pydantic.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**PydamoDB** is a lightweight Python library that gives your [Pydantic](https://github.com/pydantic/pydantic) models [DynamoDB](https://aws.amazon.com/dynamodb/) superpowers. If you're already using Pydantic for data validation and want a simple, intuitive way to persist your models to DynamoDB, this library is for you.

> **⚠️ API Stability Warning**
>
> PydamoDB is under active development and the API may change significantly between versions. We recommend pinning to a specific version in your dependencies to avoid breaking changes:
>
> ```bash
> pip install pydamodb==0.1.0  # Pin to a specific version
> ```
>
> Or in your `pyproject.toml`:
>
> ```toml
> dependencies = [
>     "pydamodb==0.1.0",  # Pin to a specific version
> ]
> ```

## Features

- 🔄 **Seamless Pydantic Integration** - Your models remain valid Pydantic models with all their features intact.
- 🔑 **Automatic Key Schema Detection** - Reads partition/sort key configuration directly from your DynamoDB table.
- 📝 **Conditional Writes** - Support for conditional save, update, and delete operations.
- 🔍 **Query Support** - Query by partition key with sort key conditions and filters with built-in pagination.
- 🗂️ **Index Support** - Query Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI).
- ⚡ **Async Support** - Full async/await support via `aioboto3` for high-performance applications.

## Limitations

These are some limitations to be aware of:

- **Float attributes**: DynamoDB doesn't support floats. Use `Decimal` instead or [a custom serializer](https://github.com/pydantic/pydantic/discussions/4701).
- **Key schema**: Field names for partition/sort keys must match the table's key schema exactly.
- **Transactions**: Multi-item transactions are not supported.
- **Scan operations**: Full table scans are intentionally not exposed.
- **Batch reads**: Batch get operations are not supported.
- **Update expressions**: Only `SET` updates are supported. For `ADD`, `REMOVE`, or `DELETE`, read-modify-save the full item.

## When to Use PydamoDB

**This library IS for you if:**

- You're already using Pydantic and want to persist models to DynamoDB.
- You want a simple, intuitive API without complex configuration.
- You prefer convention over configuration.

**This library is NOT for you if:**

- You need low-level DynamoDB control.
- You need a full-featured ODM (consider [PynamoDB](https://pynamodb.readthedocs.io/) instead).
- You need complex multi-item transactions.

## Installation

```bash
pip install pydamodb
```

**Note:** PydamoDB requires [boto3](https://github.com/boto/boto3) for sync operations or [aioboto3](https://github.com/terrycain/aioboto3) for async operations. Since PydamoDB doesn't directly import those dependencies, you must install and manage your own version:

```bash
# For synchronous operations
pip install boto3

# For asynchronous operations
pip install aioboto3

# Or both
pip install boto3 aioboto3
```

## Core Concepts

### Model Types

PydamoDB provides two base model classes for different table key configurations:

#### `PrimaryKeyModel` (alias: `PKModel`)

Use for tables with **only a partition key**:

```python
from pydamodb import PrimaryKeyModel


class Character(PrimaryKeyModel):
    name: str  # Partition key
    age: int
    occupation: str
```

#### `PrimaryKeyAndSortKeyModel` (alias: `PKSKModel`)

Use for tables with **both partition key and sort key**:

```python
from pydamodb import PrimaryKeyAndSortKeyModel


class FamilyMember(PrimaryKeyAndSortKeyModel):
    family: str  # Partition key
    name: str  # Sort key
    age: int
    occupation: str
```

### Async Model Types

For async operations, use the async equivalents:

- `AsyncPrimaryKeyModel` (alias: `AsyncPKModel`)
- `AsyncPrimaryKeyAndSortKeyModel` (alias: `AsyncPKSKModel`)

### Configuration

Each model requires a `pydamo_config` class variable with the DynamoDB table resource. Both sync and async models use the same `PydamoConfig` class:

**Sync:**

```python
import boto3
from pydamodb import PrimaryKeyModel, PydamoConfig

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("characters")


class Character(PrimaryKeyModel):
    pydamo_config = PydamoConfig(table=table)

    name: str
    age: int
    occupation: str
```

**Async:**

```python
import aioboto3
from pydamodb import AsyncPrimaryKeyModel, PydamoConfig


async def setup():
    session = aioboto3.Session()
    async with session.resource("dynamodb") as dynamodb:
        table = await dynamodb.Table("characters")

        class Character(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=table)

            name: str
            age: int
            occupation: str
```

PydamoDB automatically reads the key schema from the table to determine which fields are partition/sort keys.

## Quick Start

### Save

Save a model instance to DynamoDB.

**Sync:**

```python
homer = Character(name="Homer", age=39, occupation="Safety Inspector")
homer.save()
```

**Async:**

```python
homer = Character(name="Homer", age=39, occupation="Safety Inspector")
await homer.save()
```

**With conditions:**

```python
from botocore.exceptions import ClientError
from pydamodb import PydamoError

try:
    # Only save if the item doesn't exist
    homer.save(condition=Character.attr.name.not_exists())
except ClientError as e:
    # Handle boto3 ConditionalCheckFailedException
    print(f"Condition failed: {e}")
```

### Get

Retrieve an item by its key.

**Sync:**

```python
# Partition key only table
character = Character.get_item("Homer")
if character is None:
    print("Character not found")

# With consistent read
character = Character.get_item("Homer", consistent_read=True)
```

**Async:**

```python
# Partition key only table
character = await Character.get_item("Homer")
if character is None:
    print("Character not found")

# With consistent read
character = await Character.get_item("Homer", consistent_read=True)
```

**For tables with partition key + sort key:**

**Sync:**

```python
member = FamilyMember.get_item("Simpson", "Homer")
```

**Async:**

```python
member = await FamilyMember.get_item("Simpson", "Homer")
```

### Update

Update specific fields of an item.

**Sync:**

```python
# Update a single field
Character.update_item("Homer", updates={Character.attr.age: 40})

# Update multiple fields
Character.update_item(
    "Homer",
    updates={
        Character.attr.age: 40,
        Character.attr.catchphrase: "Woo-hoo!",
    },
)

# Conditional update
Character.update_item(
    "Homer",
    updates={Character.attr.occupation: "Astronaut"},
    condition=Character.attr.occupation == "Safety Inspector",
)
```

**Async:**

```python
# Update a single field
await Character.update_item("Homer", updates={Character.attr.age: 40})

# Update multiple fields
await Character.update_item(
    "Homer",
    updates={
        Character.attr.age: 40,
        Character.attr.catchphrase: "Woo-hoo!",
    },
)

# Conditional update
await Character.update_item(
    "Homer",
    updates={Character.attr.occupation: "Astronaut"},
    condition=Character.attr.occupation == "Safety Inspector",
)
```

**For tables with partition key + sort key:**

**Sync:**

```python
FamilyMember.update_item("Simpson", "Homer", updates={FamilyMember.attr.age: 40})
```

**Async:**

```python
await FamilyMember.update_item("Simpson", "Homer", updates={FamilyMember.attr.age: 40})
```

### Delete

Delete an item from DynamoDB.

**Sync:**

```python
# Delete by instance
character = Character.get_item("Homer")
if character:
    character.delete()

# Delete by key
Character.delete_item("Homer")

# Conditional delete
Character.delete_item("Homer", condition=Character.attr.age > 50)
```

**Async:**

```python
# Delete by instance
character = await Character.get_item("Homer")
if character:
    await character.delete()

# Delete by key
await Character.delete_item("Homer")

# Conditional delete
await Character.delete_item("Homer", condition=Character.attr.age > 50)
```

**For tables with partition key + sort key:**

**Sync:**

```python
FamilyMember.delete_item("Simpson", "Homer")
```

**Async:**

```python
await FamilyMember.delete_item("Simpson", "Homer")
```

### Query

Query items by partition key (only available for `PrimaryKeyAndSortKeyModel` / `AsyncPrimaryKeyAndSortKeyModel`).

**Sync:**

```python
# Get all members of a family
result = FamilyMember.query("Simpson")
for member in result.items:
    print(member.name, member.occupation)

# With sort key condition
result = FamilyMember.query(
    "Simpson",
    sort_key_condition=FamilyMember.attr.name.begins_with("B"),
)

# With filter condition
result = FamilyMember.query(
    "Simpson",
    filter_condition=FamilyMember.attr.age < 18,
)

# With limit
result = FamilyMember.query("Simpson", limit=2)

# Pagination
result = FamilyMember.query("Simpson")
while result.last_evaluated_key:
    result = FamilyMember.query(
        "Simpson",
        exclusive_start_key=result.last_evaluated_key,
    )
    # Process result.items

# Get all items (handles pagination automatically)
all_simpsons = FamilyMember.query_all("Simpson")
```

**Async:**

```python
# Get all members of a family
result = await FamilyMember.query("Simpson")
for member in result.items:
    print(member.name, member.occupation)

# With sort key condition
result = await FamilyMember.query(
    "Simpson",
    sort_key_condition=FamilyMember.attr.name.begins_with("B"),
)

# With filter condition
result = await FamilyMember.query(
    "Simpson",
    filter_condition=FamilyMember.attr.age < 18,
)

# With limit
result = await FamilyMember.query("Simpson", limit=2)

# Pagination
result = await FamilyMember.query("Simpson")
while result.last_evaluated_key:
    result = await FamilyMember.query(
        "Simpson",
        exclusive_start_key=result.last_evaluated_key,
    )
    # Process result.items

# Get all items (handles pagination automatically)
all_simpsons = await FamilyMember.query_all("Simpson")
```

### Batch Write

PydamoDB wraps boto3's `batch_writer` so you can work directly with models.

**Sync:**

```python
characters = [
    Character(name="Homer", age=39, occupation="Safety Inspector"),
    Character(name="Marge", age=36, occupation="Homemaker"),
]

with Character.batch_writer() as writer:
    for character in characters:
        writer.put(character)
```

**Async:**

```python
characters = [
    Character(name="Homer", age=39, occupation="Safety Inspector"),
    Character(name="Marge", age=36, occupation="Homemaker"),
]

async with Character.batch_writer() as writer:
    for character in characters:
        await writer.put(character)
```

## Conditions

PydamoDB provides a rich set of condition expressions for conditional operations and query filters.

### Comparison Conditions

```python
# Equality
Character.attr.occupation == "Safety Inspector"  # Eq
Character.attr.occupation != "Teacher"  # Ne

# Numeric comparisons
Character.attr.age < 18  # Lt
Character.attr.age <= 39  # Lte
Character.attr.age > 10  # Gt
Character.attr.age >= 21  # Gte

# Between (inclusive)
Character.attr.age.between(10, 50)
```

### Function Conditions

```python
# String begins with
Character.attr.name.begins_with("B")

# Contains (for strings or sets)
Character.attr.catchphrase.contains("D'oh")

# IN - check if value is in a list
Character.attr.occupation.in_("Student", "Teacher", "Principal")
Character.attr.age.in_(10, 38, 39, 8, 1)

# Size - compare the size/length of an attribute
Character.attr.name.size() >= 3  # String length
Character.attr.children.size() > 0  # List item count
Character.attr.traits.size() == 5  # Set element count

# Attribute existence
Character.attr.catchphrase.exists()  # AttributeExists
Character.attr.retired_at.not_exists()  # AttributeNotExists
```

### Logical Operators

Combine conditions using Python operators:

```python
# AND - both conditions must be true
condition = (Character.attr.age >= 18) & (Character.attr.occupation == "Student")

# OR - either condition must be true
condition = (Character.attr.name == "Homer") | (Character.attr.name == "Marge")

# NOT - negate a condition
condition = ~(Character.attr.age < 18)

# Complex combinations
condition = (
    (Character.attr.age >= 10)
    & (Character.attr.occupation != "Baby")
    & ~(Character.attr.name == "Maggie")
)
```

## Working with Indexes

Query Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI).

**Sync:**

```python
class FamilyMember(PrimaryKeyAndSortKeyModel):
    pydamo_config = PydamoConfig(table=family_members_table)

    family: str  # Table partition key
    name: str  # Table sort key
    occupation: str  # GSI partition key (occupation-index)
    created_at: str  # LSI sort key (created-at-index)
    age: int


# Query a GSI
inspectors = FamilyMember.query(
    partition_key_value="Safety Inspector",
    index_name="occupation-index",
)

# Query a LSI
recent_simpsons = FamilyMember.query(
    partition_key_value="Simpson",
    sort_key_condition=FamilyMember.attr.created_at.begins_with("2024-"),
    index_name="created-at-index",
)

# Get all items from an index
all_students = FamilyMember.query_all(
    partition_key_value="Student",
    index_name="occupation-index",
)
```

**Async:**

```python
# Query a GSI
inspectors = await FamilyMember.query(
    partition_key_value="Safety Inspector",
    index_name="occupation-index",
)

# Query a LSI
recent_simpsons = await FamilyMember.query(
    partition_key_value="Simpson",
    sort_key_condition=FamilyMember.attr.created_at.begins_with("2024-"),
    index_name="created-at-index",
)

# Get all items from an index
all_students = await FamilyMember.query_all(
    partition_key_value="Student",
    index_name="occupation-index",
)
```

> **Note:** Consistent reads are not supported on Global Secondary Indexes.

## Type-Safe Field Access

PydamoDB provides type-safe field access through the `attr` descriptor:

```python
class Character(PrimaryKeyModel):
    pydamo_config = PydamoConfig(table=characters_table)

    name: str
    age: int
    occupation: str


# Type-safe field references
Character.attr.name  # ExpressionField[str]
Character.attr.age  # ExpressionField[int]

# Type checking catches errors
Character.update_item(
    "Homer",
    updates={
        Character.attr.age: "not a number",  # Type error!
    },
)

# Non-existent fields raise AttributeError
Character.attr.nonexistent  # AttributeError: 'Character' has no field 'nonexistent'
```

### Mypy Plugin

For full type inference, enable the mypy plugin:

```toml
# pyproject.toml
[tool.mypy]
plugins = ["pydamodb.mypy"]
```

## Error Handling

PydamoDB follows a simple exception philosophy: **we only raise custom exceptions for PydamoDB-specific errors**. boto3 exceptions (like `ConditionalCheckFailedException`, `ProvisionedThroughputExceededException`) and Pydantic validation errors bubble up naturally without wrapping.

This approach:

- **Keeps things simple** - You don't need to learn wrapped versions of familiar exceptions.
- **Uses standard patterns** - Handle boto3 and Pydantic exceptions the same way you always do.
- **Provides clarity** - Custom exceptions are only for PydamoDB-specific issues.

### PydamoDB Exceptions

```python
from pydamodb import (
    PydamoError,
    MissingSortKeyValueError,
    InvalidKeySchemaError,
    IndexNotFoundError,
    InsufficientConditionsError,
    UnknownConditionTypeError,
    EmptyUpdateError,
)

# Catch all PydamoDB errors
try:
    homer.save()
except PydamoError as e:
    print(f"PydamoDB error: {e}")

# Catch specific PydamoDB errors
try:
    FamilyMember.query("Simpson", index_name="nonexistent-index")
except IndexNotFoundError as e:
    print(f"Index not found: {e.index_name}")

try:
    FamilyMember.get_item("Simpson")  # Missing sort key!
except MissingSortKeyValueError:
    print("Sort key is required for this table")
```

**PydamoDB Exception Hierarchy:**

```text
PydamoError (base)
├── MissingSortKeyValueError
├── InvalidKeySchemaError
├── IndexNotFoundError
├── InsufficientConditionsError
├── UnknownConditionTypeError
└── EmptyUpdateError
```

## Integration Example: FastAPI

Here's how to use PydamoDB with FastAPI:

```python
from fastapi import FastAPI, HTTPException
from pydamodb import AsyncPrimaryKeyModel, PydamoConfig
from botocore.exceptions import ClientError
import aioboto3

app = FastAPI()


class Character(AsyncPrimaryKeyModel):
    name: str
    age: int
    occupation: str
    catchphrase: str | None = None


@app.on_event("startup")
async def startup():
    session = aioboto3.Session()
    app.state.dynamodb_session = session
    async with session.resource("dynamodb") as dynamodb:
        table = await dynamodb.Table("characters")
        Character.pydamo_config = PydamoConfig(table=table)


@app.get("/characters/{name}")
async def get_character(name: str):
    try:
        character = await Character.get_item(name)
        if not character:
            raise HTTPException(status_code=404, detail="Character not found")
        return character
    except ClientError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/characters")
async def create_character(character: Character):
    try:
        await character.save(condition=Character.attr.name.not_exists())
        return character
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise HTTPException(status_code=409, detail="Character already exists")
        raise HTTPException(status_code=500, detail=str(e))
```

## Migrating from Pydantic

If you already have Pydantic models, migrating to PydamoDB is straightforward. Your models remain valid Pydantic models with all their features intact.

### Step 1: Choose the Right Base Class

| Your DynamoDB Table | Base Class to Use |
| ------------------------ | --------------------------------------------------------------- |
| Partition key only | `PrimaryKeyModel` or `AsyncPrimaryKeyModel` |
| Partition key + Sort key | `PrimaryKeyAndSortKeyModel` or `AsyncPrimaryKeyAndSortKeyModel` |

### Step 2: Change the Base Class

```python
# Before: Plain Pydantic model
from pydantic import BaseModel


class Character(BaseModel):
    name: str
    age: int
    occupation: str
    catchphrase: str | None = None


# After: PydamoDB model
from pydamodb import PrimaryKeyModel, PydamoConfig


class Character(PrimaryKeyModel):
    pydamo_config = PydamoConfig(table=characters_table)

    name: str  # Now serves as partition key
    age: int
    occupation: str
    catchphrase: str | None = None
```

### Step 3: Match Field Names to Key Schema

Your model field names **must match** the attribute names in your DynamoDB table's key schema:

```python
# If your table has partition key "name":
class Character(PrimaryKeyModel):
    name: str  # ✅ Must match partition key name exactly
    age: int  # Other fields can be named anything
    occupation: str
```

### What Still Works

Everything you love about Pydantic continues to work:

```python
from pydantic import field_validator, computed_field


class Character(PrimaryKeyModel):
    pydamo_config = PydamoConfig(table=characters_table)

    name: str
    age: int
    occupation: str
    catchphrase: str | None = None

    # ✅ Validators still work
    @field_validator("age")
    @classmethod
    def validate_age(cls, v: int) -> int:
        if v < 0:
            raise ValueError("Age cannot be negative")
        return v

    # ✅ Computed fields still work
    @computed_field
    @property
    def display_name(self) -> str:
        return f"{self.name} ({self.occupation})"


# ✅ model_dump() works
homer = Character(name="Homer", age=39, occupation="Safety Inspector")
data = homer.model_dump()

# ✅ model_validate() works
character = Character.model_validate(
    {"name": "Homer", "age": 39, "occupation": "Safety Inspector"}
)

# ✅ JSON serialization works
json_str = homer.model_dump_json()
```

PydamoDB is designed to keep your models as valid Pydantic models. Anything that would break Pydantic functionality is avoided.

### Migration Checklist

- [ ] Change base class from `BaseModel` to `PrimaryKeyModel`/`PrimaryKeyAndSortKeyModel` (or async variants)
- [ ] Install `boto3` (for sync) or `aioboto3` (for async) separately
- [ ] Add `pydamo_config = PydamoConfig(table=your_table)` to the class
- [ ] Ensure field names for keys match your DynamoDB table's key schema

## Philosophy

PydamoDB is built on these principles:

- **Simplicity over features**: We don't implement every DynamoDB feature. The API should be intuitive and easy to learn.
- **Pydantic-first**: Your models should remain valid Pydantic models with all their features.
- **Convention over configuration**: Minimize boilerplate by reading configuration from your table.
- **No magic**: Operations do what they say. No hidden batch operations or automatic retries.
