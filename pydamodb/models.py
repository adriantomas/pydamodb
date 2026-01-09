"""PydamoDB model base classes and DynamoDB operations.

This module provides the primary public API:

- `PrimaryKeyModel` for tables with a partition key only
- `PrimaryKeyAndSortKeyModel` for tables with a partition + sort key

Models are Pydantic `BaseModel` classes, with DynamoDB helpers like save/delete,
and class-level query/update helpers.
"""

from functools import cache
from typing import TYPE_CHECKING, Any, ClassVar, Generic, NamedTuple, TypedDict, TypeVar

from botocore.exceptions import ClientError
from pydantic import BaseModel
from pydantic_core import to_jsonable_python
from typing_extensions import Self

from pydamodb.conditions import Condition
from pydamodb.exceptions import (
    IndexNotFoundError,
    InvalidKeySchemaError,
    MissingSortKeyValueError,
    wrap_client_error,
)
from pydamodb.expressions import ExpressionBuilder, UpdateMapping
from pydamodb.fields import AttrDescriptor, AttributePath
from pydamodb.keys import (
    DynamoDBKey,
    KeyValue,
    LastEvaluatedKey,
)

T = TypeVar("T")


if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table

    class QueryResult(NamedTuple, Generic[T]):
        """Result of a DynamoDB query operation.

        Attributes:
            items: The returned items (validated model instances).
            last_evaluated_key: Pagination token for the next page, if any.
        """

        items: list[T]
        last_evaluated_key: LastEvaluatedKey | None

else:
    Table = Any

    class QueryResult(NamedTuple):
        """Result of a DynamoDB query operation.

        At runtime this is a non-generic NamedTuple for compatibility. During
        type checking it is treated as `QueryResult[T]`.
        """

        items: list[Any]
        last_evaluated_key: LastEvaluatedKey | None


ModelType = TypeVar("ModelType", bound="_PydamoModelBase")


class _ModelBatchWriter(Generic[ModelType]):
    """Thin wrapper around boto3's batch_writer that accepts PydamoDB models.

    Provides put/delete helpers that serialize models using model_dump(mode="json")
    and build keys via the model's configured key schema.
    """

    def __init__(
        self, model_cls: type[ModelType], overwrite_by_pkeys: list[str] | None = None
    ) -> None:
        self._model_cls = model_cls
        self._table = model_cls._table()
        self._writer = self._table.batch_writer(overwrite_by_pkeys=overwrite_by_pkeys or [])

    def __enter__(self) -> Self:
        self._writer.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._writer.__exit__(exc_type, exc_val, exc_tb)

    def put(self, model: ModelType) -> None:
        """Put a model instance using the batch writer."""

        self._writer.put_item(Item=model.model_dump(mode="json"))

    def delete(self, model: ModelType) -> None:
        """Delete a model instance using the batch writer."""

        key = self._model_cls._build_dynamodb_key(
            partition_key_value=model._partition_key_value,
            sort_key_value=model._sort_key_value,
        )
        self._writer.delete_item(Key=key)


class PydamoConfig(TypedDict):
    """Configuration required on each model class.

    Attributes:
        table: A boto3 DynamoDB Table resource.
    """

    table: Table


class _PydamoModelBase(BaseModel):
    """Internal base class for PydamoDB models.

    This class contains shared implementation for both PrimaryKeyModel and
    PrimaryKeyAndSortKeyModel. It reads the key schema directly from the
    DynamoDB table resource.

    Do not subclass this directly. Use PrimaryKeyModel or PrimaryKeyAndSortKeyModel.
    """

    pydamo_config: ClassVar[PydamoConfig]
    attr: ClassVar[AttributePath] = AttrDescriptor()  # type: ignore[assignment]

    @classmethod
    def _table(cls) -> Table:
        return cls.pydamo_config["table"]

    @classmethod
    def batch_writer(
        cls: type[ModelType], overwrite_by_pkeys: list[str] | None = None
    ) -> _ModelBatchWriter[ModelType]:
        """Return a batch writer that works with PydamoDB models.

        Args:
            overwrite_by_pkeys: List of partition key attribute names to use for
                de-duplication within the batch. If multiple items with the same
                partition key are added to the batch, only the last one will be written.

        Example:
            with User.batch_writer() as writer:
                writer.put(User(id="1", name="Homer"))
                writer.put(User(id="2", name="Marge"))
                writer.delete(User(id="3", name="Bart"))
        """

        return _ModelBatchWriter(cls, overwrite_by_pkeys=overwrite_by_pkeys)

    @classmethod
    @cache
    def _get_keys_attributes(cls) -> tuple[str, str | None]:
        """Get the partition key and sort key attribute names from the table schema.

        Returns:
            A tuple of (partition_key_attribute, sort_key_attribute).
            sort_key_attribute will be None if the table doesn't have a sort key.

        Raises:
            InvalidKeySchemaError: If the table schema is invalid (no partition key found).
        """
        table = cls._table()
        return cls._parse_key_schema(key_schema=table.key_schema)

    @classmethod
    def _get_index_key_attributes(cls, *, index_name: str) -> tuple[str, str | None]:
        """Get the partition key and sort key attribute names for an index.

        Args:
            index_name: The name of the GSI or LSI.

        Returns:
            A tuple of (partition_key_attribute, sort_key_attribute).
            sort_key_attribute will be None if the index doesn't have a sort key.

        Raises:
            IndexNotFoundError: If the index is not found on the table.
        """
        table = cls._table()

        # Check GSIs
        gsis = table.global_secondary_indexes or []
        for gsi in gsis:
            if gsi.get("IndexName") == index_name:
                key_schema = gsi.get("KeySchema")
                if key_schema is not None:
                    return cls._parse_key_schema(key_schema=key_schema)

        # Check LSIs
        lsis = table.local_secondary_indexes or []
        for lsi in lsis:
            if lsi.get("IndexName") == index_name:
                key_schema = lsi.get("KeySchema")
                if key_schema is not None:
                    return cls._parse_key_schema(key_schema=key_schema)

        raise IndexNotFoundError(index_name=index_name, model_name=cls.__name__)

    @staticmethod
    def _parse_key_schema(*, key_schema: Any) -> tuple[str, str | None]:
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
        partition_key_attribute, _ = cls._get_keys_attributes()
        return partition_key_attribute

    @property
    def _partition_key_value(self) -> KeyValue:
        return getattr(self, self._partition_key_attribute())  # type: ignore[no-any-return]

    @classmethod
    def _sort_key_attribute(cls) -> str | None:
        _, sort_key_attribute = cls._get_keys_attributes()
        return sort_key_attribute

    @property
    def _sort_key_value(self) -> KeyValue | None:
        sort_key_attribute = self._sort_key_attribute()
        return getattr(self, sort_key_attribute) if sort_key_attribute else None

    def save(self, *, condition: Condition | None = None) -> None:
        """Save the model to DynamoDB.

        Args:
            condition: Optional condition that must be satisfied for the save.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.
        """
        table = self._table()

        put_kwargs: dict[str, Any] = {"Item": self.model_dump(mode="json")}

        if condition is not None:
            builder = ExpressionBuilder()
            put_kwargs["ConditionExpression"] = builder.build_condition_expression(condition)
            put_kwargs["ExpressionAttributeNames"] = builder.attribute_names
            if builder.attribute_values:
                put_kwargs["ExpressionAttributeValues"] = builder.attribute_values

        try:
            table.put_item(**put_kwargs)
        except ClientError as e:
            raise wrap_client_error(
                e, operation="save", model_name=self.__class__.__name__
            ) from e

    @classmethod
    def _update_item_key(
        cls,
        *,
        key: DynamoDBKey,
        updates: UpdateMapping,
        condition: Condition | None = None,
    ) -> None:
        """Update an item by its key with the given field updates.

        Args:
            key: The DynamoDB key identifying the item to update.
            updates: A mapping of ExpressionField to new values.
            condition: Optional condition that must be satisfied for the update.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.
        """
        table = cls._table()
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

        try:
            table.update_item(**update_kwargs)
        except ClientError as e:
            raise wrap_client_error(e, operation="update", model_name=cls.__name__) from e

    @classmethod
    def _delete_item_key(cls, *, key: DynamoDBKey, condition: Condition | None = None) -> None:
        table = cls._table()

        delete_kwargs: dict[str, Any] = {"Key": key}

        if condition is not None:
            builder = ExpressionBuilder()
            delete_kwargs["ConditionExpression"] = builder.build_condition_expression(
                condition
            )
            delete_kwargs["ExpressionAttributeNames"] = builder.attribute_names
            if builder.attribute_values:
                delete_kwargs["ExpressionAttributeValues"] = builder.attribute_values

        try:
            table.delete_item(**delete_kwargs)
        except ClientError as e:
            raise wrap_client_error(e, operation="delete", model_name=cls.__name__) from e

    @classmethod
    def _get_item_key(
        cls,
        *,
        key: DynamoDBKey,
        consistent_read: bool = False,
    ) -> Self | None:
        """Get an item by its key.

        Args:
            key: The DynamoDB key identifying the item to get.
            consistent_read: Whether to use strongly consistent reads.

        Returns:
            The model instance if found, None otherwise.
        """
        table = cls._table()

        try:
            response = table.get_item(Key=key, ConsistentRead=consistent_read)
            item = response.get("Item")
            if item is None:
                return None
        except ClientError as e:
            raise wrap_client_error(e, operation="get", model_name=cls.__name__) from e

        return cls.model_validate(item)

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

    def delete(self, *, condition: Condition | None = None) -> None:
        """Delete this item from DynamoDB.

        Args:
            condition: Optional condition that must be satisfied for the delete.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.
        """
        key = self._build_dynamodb_key(
            partition_key_value=self._partition_key_value,
            sort_key_value=self._sort_key_value,
        )
        self._delete_item_key(key=key, condition=condition)


class PrimaryKeyModel(_PydamoModelBase):
    """Base model for DynamoDB tables with partition key only.

    Use this for tables that have only a partition key (no sort key).
    The model reads the key schema directly from the DynamoDB table resource.

    Example:
        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=users_table)

            user_id: str
            name: str
            email: str

        The field name `user_id` must match the partition key attribute name
        in the DynamoDB table.

        user = User.get_item("user-123")

        User.delete_item("user-123")

        User.update_item("user-123", updates={User.attr.name: "New Name"})
    """

    @classmethod
    def get_item(
        cls,
        partition_key_value: KeyValue,
        *,
        consistent_read: bool = False,
    ) -> Self | None:
        """Get an item by its partition key.

        Args:
            partition_key_value: The partition key value.
            consistent_read: Whether to use strongly consistent reads.

        Returns:
            The model instance if found, None otherwise.
        """
        key = cls._build_dynamodb_key(partition_key_value=partition_key_value)
        return cls._get_item_key(key=key, consistent_read=consistent_read)

    @classmethod
    def update_item(
        cls,
        partition_key_value: KeyValue,
        *,
        updates: UpdateMapping,
        condition: Condition | None = None,
    ) -> None:
        """Update an item by its partition key.

        Args:
            partition_key_value: The partition key value.
            updates: A mapping of ExpressionField to new values.
            condition: Optional condition that must be satisfied for the update.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        Example:
            User.update_item("user-123", updates={User.attr.name: "New Name"})
        """
        key = cls._build_dynamodb_key(partition_key_value=partition_key_value)
        cls._update_item_key(key=key, updates=updates, condition=condition)

    @classmethod
    def delete_item(
        cls,
        partition_key_value: KeyValue,
        *,
        condition: Condition | None = None,
    ) -> None:
        """Delete an item by its partition key.

        Args:
            partition_key_value: The partition key value.
            condition: Optional condition that must be satisfied for the delete.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        Example:
            User.delete_item("user-123")
            User.delete_item("user-123", condition=User.attr.status == "inactive")
        """
        key = cls._build_dynamodb_key(partition_key_value=partition_key_value)
        cls._delete_item_key(key=key, condition=condition)


class PrimaryKeyAndSortKeyModel(_PydamoModelBase):
    """Base model for DynamoDB tables with partition key and sort key.

    Use this for tables that have both a partition key and a sort key.
    The model reads the key schema directly from the DynamoDB table resource.

    Example:
        class Order(PrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=orders_table)

            user_id: str
            order_id: str
            status: str
            total: Decimal

        The field names `user_id` and `order_id` must match the partition and
        sort key attribute names in the DynamoDB table.

        order = Order.get_item("user-123", "order-456")

        orders = Order.query("user-123")

        Order.delete_item("user-123", "order-456")
    """

    @classmethod
    def get_item(
        cls,
        partition_key_value: KeyValue,
        sort_key_value: KeyValue,
        *,
        consistent_read: bool = False,
    ) -> Self | None:
        """Get an item by its composite key.

        Args:
            partition_key_value: The partition key value.
            sort_key_value: The sort key value.
            consistent_read: Whether to use strongly consistent reads.

        Returns:
            The model instance if found, None otherwise.
        """
        key = cls._build_dynamodb_key(
            partition_key_value=partition_key_value, sort_key_value=sort_key_value
        )
        return cls._get_item_key(key=key, consistent_read=consistent_read)

    @classmethod
    def update_item(
        cls,
        partition_key_value: KeyValue,
        sort_key_value: KeyValue,
        *,
        updates: UpdateMapping,
        condition: Condition | None = None,
    ) -> None:
        """Update an item by its composite key.

        Args:
            partition_key_value: The partition key value.
            sort_key_value: The sort key value.
            updates: A mapping of ExpressionField to new values.
            condition: Optional condition that must be satisfied for the update.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        Example:
            Order.update_item("user-123", "order-456", updates={Order.attr.status: "shipped"})
        """
        key = cls._build_dynamodb_key(
            partition_key_value=partition_key_value, sort_key_value=sort_key_value
        )
        cls._update_item_key(key=key, updates=updates, condition=condition)

    @classmethod
    def delete_item(
        cls,
        partition_key_value: KeyValue,
        sort_key_value: KeyValue,
        *,
        condition: Condition | None = None,
    ) -> None:
        """Delete an item by its composite key.

        Args:
            partition_key_value: The partition key value.
            sort_key_value: The sort key value.
            condition: Optional condition that must be satisfied for the delete.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        Example:
            Order.delete_item("user-123", "order-456")
        """
        key = cls._build_dynamodb_key(
            partition_key_value=partition_key_value, sort_key_value=sort_key_value
        )
        cls._delete_item_key(key=key, condition=condition)

    @classmethod
    def query(
        cls,
        partition_key_value: KeyValue,
        *,
        sort_key_condition: Condition | None = None,
        filter_condition: Condition | None = None,
        limit: int | None = None,
        consistent_read: bool = False,
        exclusive_start_key: LastEvaluatedKey | None = None,
        index_name: str | None = None,
    ) -> QueryResult[Self]:
        """Query items by partition key with optional sort key and filter conditions.

        Args:
            partition_key_value: The partition key value to query. When querying an
                index, this should be the index's partition key value.
            sort_key_condition: Optional condition on the sort key.
            filter_condition: Optional filter condition applied after the query.
            limit: Maximum number of items to return.
            consistent_read: Whether to use strongly consistent reads.
                Note: Consistent reads are not supported on global secondary indexes.
            exclusive_start_key: Key to start from for pagination.
            index_name: Optional name of a GSI or LSI to query instead of the table.

        Returns:
            QueryResult containing items and last_evaluated_key.

        Raises:
            IndexNotFoundError: If the specified index does not exist on the table.

        Example:
            Query the base table:
            result = Order.query(partition_key_value="user-123")
            for order in result.items:
                print(order.status)

            Paginate with the last evaluated key:
            if result.last_evaluated_key:
                next_page = Order.query(
                    partition_key_value="user-123",
                    exclusive_start_key=result.last_evaluated_key,
                )

            Query a Global Secondary Index:
            by_status = Order.query(
                partition_key_value="pending",
                index_name="status-index",
            )
        """
        table = cls._table()
        builder = ExpressionBuilder()

        # Determine which partition key attribute to use
        if index_name is not None:
            pk_attr, _ = cls._get_index_key_attributes(index_name=index_name)
        else:
            pk_attr = cls._partition_key_attribute()

        pk_placeholder = builder._get_name_placeholder(pk_attr)
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

        try:
            response = table.query(**query_kwargs)
        except ClientError as e:
            raise wrap_client_error(e, operation="query", model_name=cls.__name__) from e

        items = [cls.model_validate(item) for item in response.get("Items", [])]
        last_evaluated_key: LastEvaluatedKey | None = response.get("LastEvaluatedKey")  # type: ignore[assignment]

        return QueryResult(
            items=items,
            last_evaluated_key=last_evaluated_key,
        )

    @classmethod
    def query_all(
        cls,
        partition_key_value: KeyValue,
        *,
        sort_key_condition: Condition | None = None,
        filter_condition: Condition | None = None,
        consistent_read: bool = False,
        index_name: str | None = None,
    ) -> list[Self]:
        """Query all items matching the partition key, handling pagination automatically.

        This method repeatedly calls query() until all matching items are retrieved.
        Use this when you need all results and don't want to handle pagination manually.

        Args:
            partition_key_value: The partition key value to query. When querying an
                index, this should be the index's partition key value.
            sort_key_condition: Optional condition on the sort key.
            filter_condition: Optional filter condition applied after the query.
            consistent_read: Whether to use strongly consistent reads.
                Note: Consistent reads are not supported on global secondary indexes.
            index_name: Optional name of a GSI or LSI to query instead of the table.

        Returns:
            List of all matching model instances.

        Raises:
            IndexNotFoundError: If the specified index does not exist on the table.

        Example:
            Query all from the base table:
            all_orders = Order.query_all(partition_key_value="user-123")

            Query all from a GSI:
            pending_orders = Order.query_all(
                partition_key_value="pending",
                index_name="status-index",
            )
        """
        all_items: list[Self] = []
        last_key: LastEvaluatedKey | None = None

        while True:
            items, last_key = cls.query(
                partition_key_value=partition_key_value,
                sort_key_condition=sort_key_condition,
                filter_condition=filter_condition,
                consistent_read=consistent_read,
                exclusive_start_key=last_key,
                index_name=index_name,
            )
            all_items.extend(items)

            if last_key is None:
                break

        return all_items


# Type aliases for convenience
PKModel = PrimaryKeyModel
PKSKModel = PrimaryKeyAndSortKeyModel


__all__ = [
    "PrimaryKeyModel",
    "PrimaryKeyAndSortKeyModel",
    "PKModel",
    "PKSKModel",
    "QueryResult",
    "PydamoConfig",
]
