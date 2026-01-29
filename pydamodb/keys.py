"""Type aliases for DynamoDB key-related values.

This module defines type aliases for working with DynamoDB keys and pagination.

Type aliases:
    KeyValue: The types allowed as partition key or sort key values in DynamoDB.
        Includes str, bytes, bytearray, int, and Decimal.

    DynamoDBKey: A dictionary mapping attribute names to key values. This is the
        format required by boto3 operations like get_item, update_item, delete_item.
        Example: {"user_id": "123", "timestamp": 1234567890}

    LastEvaluatedKey: The pagination token returned by query() and scan() operations.
        Pass this to exclusive_start_key to continue pagination. Has the same structure
        as DynamoDBKey but uses TypeAliasType for better type inference.
"""

from decimal import Decimal
from typing import TypeAlias

from typing_extensions import TypeAliasType

KeyValue: TypeAlias = str | bytes | bytearray | int | Decimal
DynamoDBKey: TypeAlias = dict[str, KeyValue]
LastEvaluatedKey = TypeAliasType("LastEvaluatedKey", DynamoDBKey)


__all__ = [
    "DynamoDBKey",
    "KeyValue",
    "LastEvaluatedKey",
]
