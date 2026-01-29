"""Shared base functionality for PydamoDB models.

This module provides the PydamoSharedMixin class, which contains common logic
shared between synchronous and asynchronous PydamoDB models to eliminate code
duplication while maintaining type safety and Pydantic compatibility.
"""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, NamedTuple, TypeVar

from pydantic import BaseModel
from pydantic_core import to_jsonable_python
from typing_extensions import TypedDict

from pydamodb.conditions import Condition
from pydamodb.exceptions import InvalidKeySchemaError, MissingSortKeyValueError
from pydamodb.expressions import ExpressionBuilder, UpdateMapping
from pydamodb.fields import AttrDescriptor
from pydamodb.keys import DynamoDBKey, KeyValue, LastEvaluatedKey

T = TypeVar("T")

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table as SyncTable
    from mypy_boto3_dynamodb.type_defs import (
        KeySchemaElementTypeDef as SyncKeySchemaElementTypeDef,
    )
    from types_aiobotocore_dynamodb.service_resource import Table as AsyncTable
    from types_aiobotocore_dynamodb.type_defs import (
        KeySchemaElementTypeDef as AsyncKeySchemaElementTypeDef,
    )

    class QueryResult(NamedTuple, Generic[T]):
        """Result of a DynamoDB query operation.

        Attributes:
            items: The returned items (validated model instances).
            last_evaluated_key: Pagination token for the next page, if any.

        """

        items: list[T]
        last_evaluated_key: LastEvaluatedKey | None
else:
    SyncTable = Any
    AsyncTable = Any
    SyncKeySchemaElementTypeDef = Any
    AsyncKeySchemaElementTypeDef = Any

    class QueryResult(NamedTuple):
        """Result of a DynamoDB query operation.

        At runtime this is a non-generic NamedTuple for compatibility. During
        type checking it is treated as `QueryResult[T]`.
        """

        items: list[Any]
        last_evaluated_key: LastEvaluatedKey | None


Table = TypeVar("Table", SyncTable, AsyncTable)
KeySchema = SyncKeySchemaElementTypeDef | AsyncKeySchemaElementTypeDef


class PydamoConfig(TypedDict, Generic[Table]):
    """Configuration required on each model class.

    Attributes:
        table: The DynamoDB Table resource associated with the model.

    """

    table: Table


class _PydamoModelBase(BaseModel, Generic[Table]):
    """Internal base class containing shared logic for sync and async PydamoDB models.

    This base class provides common functionality for:
    - Parsing DynamoDB key schemas
    - Accessing partition and sort key attributes
    - Building DynamoDB key dictionaries
    - Building kwargs for DynamoDB operations (put, update, delete, query)

    Subclasses must provide:
    - A _key_schema() class method that returns the table's key schema
    - A pydamo_config class variable with table configuration
    - A _table() class method that returns the DynamoDB table resource

    This class should not be subclassed directly. Use PrimaryKeyModel,
    PrimaryKeyAndSortKeyModel, AsyncPrimaryKeyModel, or
    AsyncPrimaryKeyAndSortKeyModel instead.
    """

    pydamo_config: ClassVar[PydamoConfig[Table]]
    attr: ClassVar[AttrDescriptor] = AttrDescriptor()

    @classmethod
    def _table(cls) -> Table:
        return cls.pydamo_config["table"]

    @classmethod
    def _key_schema(cls) -> list[KeySchema]:
        raise NotImplementedError

    @staticmethod
    def _parse_key_schema(*, key_schema: Sequence[KeySchema]) -> tuple[str, str | None]:
        """Parse a DynamoDB key schema into partition and sort key attributes.

        Raises:
            InvalidKeySchemaError: If no partition key is present.

        """
        partition_key_attribute: str | None = None
        sort_key_attribute: str | None = None

        for key_element in key_schema:
            if key_element["KeyType"] == "HASH":
                partition_key_attribute = key_element["AttributeName"]
            elif key_element["KeyType"] == "RANGE":
                sort_key_attribute = key_element["AttributeName"]

        if partition_key_attribute is None:
            raise InvalidKeySchemaError()

        return partition_key_attribute, sort_key_attribute

    @classmethod
    def _partition_key_attribute(cls) -> str:
        partition_key_attribute, _ = cls._parse_key_schema(key_schema=cls._key_schema())
        return partition_key_attribute

    @property
    def _partition_key_value(self) -> KeyValue:
        return getattr(self, self._partition_key_attribute())  # type: ignore[no-any-return]

    @classmethod
    def _sort_key_attribute(cls) -> str | None:
        _, sort_key_attribute = cls._parse_key_schema(key_schema=cls._key_schema())
        return sort_key_attribute

    @property
    def _sort_key_value(self) -> KeyValue | None:
        sort_key_attribute = self._sort_key_attribute()
        return getattr(self, sort_key_attribute) if sort_key_attribute else None

    @classmethod
    def _build_dynamodb_key(
        cls,
        *,
        partition_key_value: KeyValue,
        sort_key_value: KeyValue | None = None,
    ) -> DynamoDBKey:
        """Build a DynamoDB key from key values.

        Args:
            partition_key_value: The partition key value.
            sort_key_value: The sort key value (required for tables with sort key).

        Returns:
            The DynamoDB key dictionary.

        Raises:
            MissingSortKeyValueError: If the table has a sort key but no value provided.

        """
        partition_key_attribute = cls._partition_key_attribute()
        key: DynamoDBKey = {partition_key_attribute: to_jsonable_python(partition_key_value)}

        sort_key_attribute = cls._sort_key_attribute()
        if sort_key_attribute is not None:
            if sort_key_value is None:
                raise MissingSortKeyValueError(model_name=cls.__name__)
            key[sort_key_attribute] = to_jsonable_python(sort_key_value)

        return key

    def _build_put_kwargs(self, *, condition: Condition | None) -> dict[str, Any]:
        """Build kwargs dictionary for put_item operation.

        Args:
            condition: Optional condition for conditional save.

        Returns:
            Dictionary of kwargs to pass to table.put_item().

        """
        put_kwargs: dict[str, Any] = {"Item": self.model_dump(mode="json")}

        if condition is not None:
            builder = ExpressionBuilder()
            put_kwargs["ConditionExpression"] = builder.build_condition_expression(condition)
            put_kwargs["ExpressionAttributeNames"] = builder.attribute_names
            if builder.attribute_values:
                put_kwargs["ExpressionAttributeValues"] = builder.attribute_values

        return put_kwargs

    @classmethod
    def _build_update_kwargs(
        cls,
        *,
        key: DynamoDBKey,
        updates: UpdateMapping,
        condition: Condition | None,
    ) -> dict[str, Any]:
        """Build kwargs dictionary for update_item operation.

        Args:
            key: The DynamoDB key identifying the item.
            updates: Field updates mapping.
            condition: Optional condition for conditional update.

        Returns:
            Dictionary of kwargs to pass to table.update_item().

        """
        builder = ExpressionBuilder()
        update_expression = builder.build_update_expression(updates)

        update_kwargs: dict[str, Any] = {}

        if condition is not None:
            update_kwargs["ConditionExpression"] = builder.build_condition_expression(
                condition
            )

        update_kwargs |= {
            "Key": key,
            "UpdateExpression": update_expression,
            "ExpressionAttributeNames": builder.attribute_names,
            "ExpressionAttributeValues": builder.attribute_values,
        }

        return update_kwargs

    @classmethod
    def _build_delete_kwargs(
        cls, *, key: DynamoDBKey, condition: Condition | None
    ) -> dict[str, Any]:
        """Build kwargs dictionary for delete_item operation.

        Args:
            key: The DynamoDB key identifying the item.
            condition: Optional condition for conditional delete.

        Returns:
            Dictionary of kwargs to pass to table.delete_item().

        """
        delete_kwargs: dict[str, Any] = {"Key": key}

        if condition is not None:
            builder = ExpressionBuilder()
            delete_kwargs["ConditionExpression"] = builder.build_condition_expression(
                condition
            )
            delete_kwargs["ExpressionAttributeNames"] = builder.attribute_names
            if builder.attribute_values:
                delete_kwargs["ExpressionAttributeValues"] = builder.attribute_values

        return delete_kwargs

    @classmethod
    def _build_query_kwargs(
        cls,
        *,
        partition_key_attribute: str,
        partition_key_value: KeyValue,
        sort_key_condition: Condition | None,
        filter_condition: Condition | None,
        limit: int | None,
        consistent_read: bool,
        exclusive_start_key: LastEvaluatedKey | None,
        index_name: str | None,
    ) -> dict[str, Any]:
        """Build kwargs dictionary for query operation.

        Args:
            partition_key_attribute: The partition key attribute name (from table or index).
            partition_key_value: The partition key value to query.
            sort_key_condition: Optional condition on sort key.
            filter_condition: Optional filter condition.
            limit: Optional result limit.
            consistent_read: Whether to use consistent reads.
            exclusive_start_key: Pagination token.
            index_name: Optional index name.

        Returns:
            Dictionary of kwargs to pass to table.query().

        """
        builder = ExpressionBuilder()

        pk_placeholder = builder._get_name_placeholder(partition_key_attribute)
        pk_value_placeholder = builder._get_value_placeholder(partition_key_value)
        key_condition = f"{pk_placeholder} = {pk_value_placeholder}"

        if sort_key_condition is not None:
            sk_condition_expr = builder.build_condition_expression(sort_key_condition)
            key_condition = f"{key_condition} AND {sk_condition_expr}"

        query_kwargs: dict[str, Any] = {
            "KeyConditionExpression": key_condition,
            "ConsistentRead": consistent_read,
        }

        if index_name is not None:
            query_kwargs["IndexName"] = index_name

        if filter_condition is not None:
            query_kwargs["FilterExpression"] = builder.build_condition_expression(
                filter_condition
            )

        query_kwargs["ExpressionAttributeNames"] = builder.attribute_names
        if builder.attribute_values:
            query_kwargs["ExpressionAttributeValues"] = builder.attribute_values

        if limit is not None:
            query_kwargs["Limit"] = limit

        if exclusive_start_key is not None:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key

        return query_kwargs


__all__ = [
    "AsyncTable",
    "PydamoConfig",
    "QueryResult",
    "SyncTable",
    "_PydamoModelBase",
]
