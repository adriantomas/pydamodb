"""PydamoDB model base classes and DynamoDB operations.

This module provides the primary public API:

- `PrimaryKeyModel` for tables with a partition key only
- `PrimaryKeyAndSortKeyModel` for tables with a partition + sort key

Models are Pydantic `BaseModel` classes, with DynamoDB helpers like save/delete,
and class-level query/update helpers.
"""

from types import TracebackType
from typing import ClassVar, Generic, TypeVar

from typing_extensions import Self

from pydamodb.base import KeySchema, PydamoConfig, QueryResult, SyncTable, _PydamoModelBase
from pydamodb.conditions import Condition
from pydamodb.exceptions import (
    IndexNotFoundError,
)
from pydamodb.expressions import UpdateMapping
from pydamodb.keys import (
    DynamoDBKey,
    KeyValue,
    LastEvaluatedKey,
)

ModelType = TypeVar("ModelType", bound="_SyncPydamoModelBase")


class _ModelBatchWriter(Generic[ModelType]):
    """Context manager for batch writing PydamoDB models to DynamoDB.

    This class wraps boto3's batch_writer to accept PydamoDB model instances
    instead of raw dictionaries. It automatically handles model serialization
    and key building based on the model's key schema.

    Use via Model.batch_writer() context manager for efficient batch operations.

    Example:
        with User.batch_writer() as writer:
            writer.put(User(id="1", name="Homer"))
            writer.put(User(id="2", name="Marge"))
            writer.delete(User(id="3", name="Bart"))

    """

    def __init__(
        self,
        model_cls: type[ModelType],
        overwrite_by_pkeys: list[str] | None = None,
    ) -> None:
        self._model_cls = model_cls
        self._table = model_cls._table()
        self._writer = self._table.batch_writer(overwrite_by_pkeys=overwrite_by_pkeys or [])

    def __enter__(self) -> Self:
        self._writer.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
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


class _SyncPydamoModelBase(_PydamoModelBase[SyncTable]):
    """Internal base class for synchronous PydamoDB models.

    This class contains shared implementation for both PrimaryKeyModel and
    PrimaryKeyAndSortKeyModel. It provides synchronous DynamoDB operations
    and reads the key schema directly from the DynamoDB table resource.

    Features:
    - Batch writing with automatic serialization
    - Index key schema lookup for GSIs and LSIs
    - Internal CRUD operations (_get_item_key, _update_item_key, _delete_item_key)

    Do not subclass this directly. Use PrimaryKeyModel or PrimaryKeyAndSortKeyModel.
    """

    pydamo_config: ClassVar[PydamoConfig[SyncTable]]

    @classmethod
    def _key_schema(cls) -> list[KeySchema]:
        """Get key schema, auto-loading from table if not cached."""
        return cls._table().key_schema

    @classmethod
    def batch_writer(
        cls,
        overwrite_by_pkeys: list[str] | None = None,
    ) -> _ModelBatchWriter[Self]:
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

        raise IndexNotFoundError(index_name=index_name)

    def save(self, *, condition: Condition | None = None) -> None:
        """Save the model to DynamoDB.

        Args:
            condition: Optional condition that must be satisfied for the save.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        """
        table = self._table()
        put_kwargs = self._build_put_kwargs(condition=condition)

        table.put_item(**put_kwargs)

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
        update_kwargs = cls._build_update_kwargs(key=key, updates=updates, condition=condition)

        table.update_item(**update_kwargs)

    @classmethod
    def _delete_item_key(cls, *, key: DynamoDBKey, condition: Condition | None = None) -> None:
        """Delete an item by its key.

        Args:
            key: The DynamoDB key identifying the item to delete.
            condition: Optional condition that must be satisfied for the delete.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        """
        table = cls._table()
        delete_kwargs = cls._build_delete_kwargs(key=key, condition=condition)

        table.delete_item(**delete_kwargs)

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

        response = table.get_item(Key=key, ConsistentRead=consistent_read)
        item = response.get("Item")
        if item is None:
            return None

        return cls.model_validate(item)

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


class PrimaryKeyModel(_SyncPydamoModelBase):
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


class PrimaryKeyAndSortKeyModel(_SyncPydamoModelBase):
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
            partition_key_value=partition_key_value,
            sort_key_value=sort_key_value,
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
            partition_key_value=partition_key_value,
            sort_key_value=sort_key_value,
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
            partition_key_value=partition_key_value,
            sort_key_value=sort_key_value,
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

        # Determine which partition key attribute to use
        if index_name is not None:
            partition_key_attribute, _ = cls._get_index_key_attributes(index_name=index_name)
        else:
            partition_key_attribute = cls._partition_key_attribute()

        # Build query kwargs using mixin method
        query_kwargs = cls._build_query_kwargs(
            partition_key_attribute=partition_key_attribute,
            partition_key_value=partition_key_value,
            sort_key_condition=sort_key_condition,
            filter_condition=filter_condition,
            limit=limit,
            consistent_read=consistent_read,
            exclusive_start_key=exclusive_start_key,
            index_name=index_name,
        )

        response = table.query(**query_kwargs)

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
    "PKModel",
    "PKSKModel",
    "PrimaryKeyAndSortKeyModel",
    "PrimaryKeyModel",
]
