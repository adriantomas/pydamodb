"""Type aliases for DynamoDB key-related values.

These aliases describe:

- `KeyValue`: value types allowed in DynamoDB keys
- `DynamoDBKey`: the key mapping shape passed to boto3 operations
- `LastEvaluatedKey`: the pagination token returned by DynamoDB query/scan APIs
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
