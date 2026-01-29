"""Async PydamoDB model base classes and DynamoDB operations.

This module provides async versions of the primary public API:

- `AsyncPrimaryKeyModel` for tables with a partition key only
- `AsyncPrimaryKeyAndSortKeyModel` for tables with a partition + sort key

Models are Pydantic `BaseModel` classes, with async DynamoDB helpers like save/delete,
and class-level async query/update helpers.
"""

from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

if TYPE_CHECKING:
    from aioboto3.dynamodb.table import BatchWriter
else:
    BatchWriter = Any

from typing_extensions import Self

from pydamodb.base import AsyncTable, KeySchema, PydamoConfig, QueryResult, _PydamoModelBase
from pydamodb.conditions import Condition
from pydamodb.exceptions import IndexNotFoundError
from pydamodb.expressions import UpdateMapping
from pydamodb.keys import (
    DynamoDBKey,
    KeyValue,
    LastEvaluatedKey,
)

ModelType = TypeVar("ModelType", bound="_AsyncPydamoModelBase")


class _AsyncModelBatchWriter(Generic[ModelType]):
    """Async context manager for batch writing PydamoDB models to DynamoDB.

    This class wraps aioboto3's batch_writer to accept PydamoDB model instances
    instead of raw dictionaries. It automatically handles model serialization
    and key building based on the model's key schema.

    Use via Model.batch_writer() async context manager for efficient batch operations.

    Example:
        async with User.batch_writer() as writer:
            await writer.put(User(id="1", name="Homer"))
            await writer.put(User(id="2", name="Marge"))
            await writer.delete(User(id="3", name="Bart"))

    """

    def __init__(
        self,
        model_cls: type[ModelType],
        overwrite_by_pkeys: list[str] | None = None,
    ) -> None:
        self._model_cls = model_cls
        self._table = model_cls._table()
        self._overwrite_by_pkeys = overwrite_by_pkeys or []
        self._writer: BatchWriter | None = None

    async def __aenter__(self) -> Self:
        self._writer = self._table.batch_writer(
            overwrite_by_pkeys=self._overwrite_by_pkeys,
        )
        await self._writer.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._writer is not None:
            await self._writer.__aexit__(exc_type, exc_val, exc_tb)

    async def put(self, model: ModelType) -> None:
        """Put a model instance using the batch writer."""
        if self._writer is None:
            msg = "Batch writer not initialized. Use 'async with' context manager."
            raise RuntimeError(msg)
        await self._writer.put_item(Item=model.model_dump(mode="json"))

    async def delete(self, model: ModelType) -> None:
        """Delete a model instance using the batch writer."""
        if self._writer is None:
            msg = "Batch writer not initialized. Use 'async with' context manager."
            raise RuntimeError(msg)
        key = self._model_cls._build_dynamodb_key(
            partition_key_value=model._partition_key_value,
            sort_key_value=model._sort_key_value,
        )
        await self._writer.delete_item(Key=key)


class _AsyncPydamoModelBase(_PydamoModelBase[AsyncTable]):
    """Internal base class for asynchronous PydamoDB models.

    This class contains shared implementation for both AsyncPrimaryKeyModel and
    AsyncPrimaryKeyAndSortKeyModel. It provides asynchronous DynamoDB operations
    and reads the key schema from the DynamoDB table resource with caching.

    Features:
    - Async batch writing with automatic serialization
    - Cached key schema loading to avoid repeated async calls
    - Index key schema lookup for GSIs and LSIs
    - Internal async CRUD operations (_async_get_item_key, _async_update_item_key...)

    Do not subclass this directly. Use AsyncPrimaryKeyModel or
    AsyncPrimaryKeyAndSortKeyModel.
    """

    pydamo_config: ClassVar[PydamoConfig[AsyncTable]]
    _cached_key_schema: ClassVar[list[KeySchema] | None] = None

    @classmethod
    async def _load_key_schema(cls) -> None:
        """Load and cache the key schema from the table."""
        if cls._cached_key_schema is None:
            cls._cached_key_schema = await cls._table().key_schema

    @classmethod
    def _key_schema(cls) -> list[KeySchema]:
        """Get cached key schema.

        The schema must be pre-loaded using _load_key_schema() before calling this method.
        """
        if cls._cached_key_schema is None:
            raise RuntimeError(
                f"{cls.__name__}._key_schema() called before schema was loaded. "
                "Ensure async methods call await cls._load_key_schema() first."
            )
        return cls._cached_key_schema

    @classmethod
    def batch_writer(
        cls,
        overwrite_by_pkeys: list[str] | None = None,
    ) -> _AsyncModelBatchWriter[Self]:
        """Return an async batch writer that works with PydamoDB models.

        Args:
            overwrite_by_pkeys: List of partition key attribute names to use for
                de-duplication within the batch. If multiple items with the same
                partition key are added to the batch, only the last one will be written.

        Example:
            async with User.batch_writer() as writer:
                await writer.put(User(id="1", name="Homer"))
                await writer.put(User(id="2", name="Marge"))
                await writer.delete(User(id="3", name="Bart"))

        """
        return _AsyncModelBatchWriter(cls, overwrite_by_pkeys=overwrite_by_pkeys)

    @classmethod
    async def _get_index_key_attributes(cls, *, index_name: str) -> tuple[str, str | None]:
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
        gsis = await table.global_secondary_indexes or []
        for gsi in gsis:
            if gsi.get("IndexName") == index_name:
                key_schema = gsi.get("KeySchema")
                if key_schema is not None:
                    return cls._parse_key_schema(key_schema=key_schema)

        # Check LSIs
        lsis = await table.local_secondary_indexes or []
        for lsi in lsis:
            if lsi.get("IndexName") == index_name:
                key_schema = lsi.get("KeySchema")
                if key_schema is not None:
                    return cls._parse_key_schema(key_schema=key_schema)

        raise IndexNotFoundError(index_name=index_name)

    async def save(self, *, condition: Condition | None = None) -> None:
        """Save the model to DynamoDB.

        Args:
            condition: Optional condition that must be satisfied for the save.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        """
        await self._load_key_schema()
        table = self._table()
        put_kwargs = self._build_put_kwargs(condition=condition)

        await table.put_item(**put_kwargs)

    @classmethod
    async def _async_update_item_key(
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
        await cls._load_key_schema()
        table = cls._table()
        update_kwargs = cls._build_update_kwargs(key=key, updates=updates, condition=condition)

        await table.update_item(**update_kwargs)

    @classmethod
    async def _async_delete_item_key(
        cls,
        *,
        key: DynamoDBKey,
        condition: Condition | None = None,
    ) -> None:
        """Delete an item by its key (async).

        Args:
            key: The DynamoDB key identifying the item to delete.
            condition: Optional condition that must be satisfied for the delete.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        """
        await cls._load_key_schema()
        table = cls._table()
        delete_kwargs = cls._build_delete_kwargs(key=key, condition=condition)

        await table.delete_item(**delete_kwargs)

    @classmethod
    async def _async_get_item_key(
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
        await cls._load_key_schema()
        table = cls._table()

        response = await table.get_item(Key=key, ConsistentRead=consistent_read)
        item = response.get("Item")
        if item is None:
            return None

        return cls.model_validate(item)

    async def delete(self, *, condition: Condition | None = None) -> None:
        """Delete this item from DynamoDB.

        Args:
            condition: Optional condition that must be satisfied for the delete.

        Raises:
            ConditionCheckFailedError: If the condition is not satisfied.

        """
        await self._load_key_schema()
        key = self._build_dynamodb_key(
            partition_key_value=self._partition_key_value,
            sort_key_value=self._sort_key_value,
        )
        await self._async_delete_item_key(key=key, condition=condition)


class AsyncPrimaryKeyModel(_AsyncPydamoModelBase):
    """Base model for DynamoDB tables with partition key only (async version).

    Use this for tables that have only a partition key (no sort key).
    The model reads the key schema directly from the DynamoDB table resource.

    Requires aioboto3 to be installed. Install with: pip install 'pydamodb[async]'

    Example:
        import aioboto3
        from pydamodb import AsyncPrimaryKeyModel, PydamoConfig

        class User(AsyncPrimaryKeyModel):
            user_id: str
            name: str
            email: str

        async def main():
            session = aioboto3.Session()
            async with session.resource("dynamodb") as dynamodb:
                table = await dynamodb.Table("users")
                User.pydamo_config = PydamoConfig(table=table)

                user = await User.get_item("user-123")
                await User.delete_item("user-123")
                await User.update_item("user-123", updates={User.attr.name: "New Name"})

    """

    @classmethod
    async def get_item(
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
        await cls._load_key_schema()
        key = cls._build_dynamodb_key(partition_key_value=partition_key_value)
        return await cls._async_get_item_key(key=key, consistent_read=consistent_read)

    @classmethod
    async def update_item(
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
            await User.update_item("user-123", updates={User.attr.name: "New Name"})

        """
        await cls._load_key_schema()
        key = cls._build_dynamodb_key(partition_key_value=partition_key_value)
        await cls._async_update_item_key(key=key, updates=updates, condition=condition)

    @classmethod
    async def delete_item(
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
            await User.delete_item("user-123")
            await User.delete_item("user-123", condition=User.attr.status == "inactive")

        """
        await cls._load_key_schema()
        key = cls._build_dynamodb_key(partition_key_value=partition_key_value)
        await cls._async_delete_item_key(key=key, condition=condition)


class AsyncPrimaryKeyAndSortKeyModel(_AsyncPydamoModelBase):
    """Base model for DynamoDB tables with partition key and sort key (async version).

    Use this for tables that have both a partition key and a sort key.
    The model reads the key schema directly from the DynamoDB table resource.

    Requires aioboto3 to be installed. Install with: pip install 'pydamodb[async]'

    Example:
        import aioboto3
        from pydamodb import AsyncPrimaryKeyAndSortKeyModel, PydamoConfig

        class Order(AsyncPrimaryKeyAndSortKeyModel):
            user_id: str
            order_id: str
            status: str
            total: Decimal

        async def main():
            session = aioboto3.Session()
            async with session.resource("dynamodb") as dynamodb:
                table = await dynamodb.Table("orders")
                Order.pydamo_config = PydamoConfig(table=table)

                order = await Order.get_item("user-123", "order-456")
                orders = await Order.query("user-123")
                await Order.delete_item("user-123", "order-456")

    """

    @classmethod
    async def get_item(
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
        await cls._load_key_schema()
        key = cls._build_dynamodb_key(
            partition_key_value=partition_key_value,
            sort_key_value=sort_key_value,
        )
        return await cls._async_get_item_key(key=key, consistent_read=consistent_read)

    @classmethod
    async def update_item(
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
            await Order.update_item(
                "user-123",
                "order-456",
                updates={Order.attr.status: "shipped"},
            )

        """
        await cls._load_key_schema()
        key = cls._build_dynamodb_key(
            partition_key_value=partition_key_value,
            sort_key_value=sort_key_value,
        )
        await cls._async_update_item_key(key=key, updates=updates, condition=condition)

    @classmethod
    async def delete_item(
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
            await Order.delete_item("user-123", "order-456")

        """
        await cls._load_key_schema()
        key = cls._build_dynamodb_key(
            partition_key_value=partition_key_value,
            sort_key_value=sort_key_value,
        )
        await cls._async_delete_item_key(key=key, condition=condition)

    @classmethod
    async def query(
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
            result = await Order.query(partition_key_value="user-123")
            for order in result.items:
                print(order.status)

            Paginate with the last evaluated key:
            if result.last_evaluated_key:
                next_page = await Order.query(
                    partition_key_value="user-123",
                    exclusive_start_key=result.last_evaluated_key,
                )

            Query a Global Secondary Index:
            by_status = await Order.query(
                partition_key_value="pending",
                index_name="status-index",
            )

        """
        await cls._load_key_schema()
        table = cls._table()

        # Determine which partition key attribute to use
        if index_name is not None:
            pk_attr, _ = await cls._get_index_key_attributes(index_name=index_name)
        else:
            pk_attr = cls._partition_key_attribute()

        # Build query kwargs using mixin method
        query_kwargs = cls._build_query_kwargs(
            partition_key_attribute=pk_attr,
            partition_key_value=partition_key_value,
            sort_key_condition=sort_key_condition,
            filter_condition=filter_condition,
            limit=limit,
            consistent_read=consistent_read,
            exclusive_start_key=exclusive_start_key,
            index_name=index_name,
        )

        response = await table.query(**query_kwargs)

        items = [cls.model_validate(item) for item in response.get("Items", [])]
        last_evaluated_key: LastEvaluatedKey | None = response.get("LastEvaluatedKey")  # type: ignore[assignment]

        return QueryResult(
            items=items,
            last_evaluated_key=last_evaluated_key,
        )

    @classmethod
    async def query_all(
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
            all_orders = await Order.query_all(partition_key_value="user-123")

            Query all from a GSI:
            pending_orders = await Order.query_all(
                partition_key_value="pending",
                index_name="status-index",
            )

        """
        all_items: list[Self] = []
        last_key: LastEvaluatedKey | None = None

        while True:
            items, last_key = await cls.query(
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
AsyncPKModel = AsyncPrimaryKeyModel
AsyncPKSKModel = AsyncPrimaryKeyAndSortKeyModel


__all__ = [
    "AsyncPKModel",
    "AsyncPKSKModel",
    "AsyncPrimaryKeyAndSortKeyModel",
    "AsyncPrimaryKeyModel",
]
