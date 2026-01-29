import pytest
from pytest_asyncio import fixture as async_fixture
from types_aiobotocore_dynamodb.service_resource import Table as AsyncTable

from pydamodb.async_models import AsyncPrimaryKeyAndSortKeyModel, AsyncPrimaryKeyModel
from pydamodb.base import PydamoConfig
from pydamodb.exceptions import IndexNotFoundError


class AsyncPKModel(AsyncPrimaryKeyModel):
    id: str
    name: str


@async_fixture
async def async_pk_model(async_pk_table: AsyncTable) -> AsyncPKModel:
    AsyncPKModel.pydamo_config = PydamoConfig(table=async_pk_table)
    return AsyncPKModel(id="partition_value", name="Test Item")


@pytest.mark.asyncio
async def test_async_primary_key_model_save(
    async_pk_table: AsyncTable,
    async_pk_model: AsyncPKModel,
) -> None:
    await async_pk_model.save()

    response = await async_pk_table.get_item(Key={"id": "partition_value"})
    assert "Item" in response
    assert response["Item"]["name"] == "Test Item"


@pytest.mark.asyncio
async def test_async_primary_key_model_get_item(async_pk_model: AsyncPKModel) -> None:
    await async_pk_model.save()

    fetched_item = await AsyncPKModel.get_item("partition_value")
    assert fetched_item is not None
    assert fetched_item.name == "Test Item"


@pytest.mark.asyncio
async def test_async_primary_key_model_get_item_non_existent(
    async_pk_model: AsyncPKModel,
) -> None:
    non_existent = await AsyncPKModel.get_item("non_existent")
    assert non_existent is None


@pytest.mark.asyncio
async def test_async_primary_key_model_get_item_with_consistent_read(
    async_pk_model: AsyncPKModel,
) -> None:
    await async_pk_model.save()

    fetched_item = await AsyncPKModel.get_item(async_pk_model.id, consistent_read=True)
    assert fetched_item is not None
    assert fetched_item.name == async_pk_model.name


@pytest.mark.asyncio
async def test_async_primary_key_model_get_item_with_consistent_read_false(
    async_pk_model: AsyncPKModel,
) -> None:
    await async_pk_model.save()

    fetched_item = await AsyncPKModel.get_item(async_pk_model.id, consistent_read=False)
    assert fetched_item is not None
    assert fetched_item.name == async_pk_model.name


@pytest.mark.asyncio
async def test_async_primary_key_model_delete(async_pk_model: AsyncPKModel) -> None:
    await async_pk_model.save()

    fetched_item = await AsyncPKModel.get_item("partition_value")
    assert fetched_item is not None

    await async_pk_model.delete()

    fetched_item_after_delete = await AsyncPKModel.get_item("partition_value")
    assert fetched_item_after_delete is None


@pytest.mark.asyncio
async def test_async_primary_key_model_delete_item(async_pk_model: AsyncPKModel) -> None:
    await async_pk_model.save()

    fetched_item = await AsyncPKModel.get_item("partition_value")
    assert fetched_item is not None

    await AsyncPKModel.delete_item("partition_value")

    fetched_item_after_delete = await AsyncPKModel.get_item("partition_value")
    assert fetched_item_after_delete is None


@pytest.mark.asyncio
async def test_async_primary_key_model_batch_writer(async_pk_table: AsyncTable) -> None:
    AsyncPKModel.pydamo_config = PydamoConfig(table=async_pk_table)
    items = [
        AsyncPKModel(id="batch-1", name="Batch One"),
        AsyncPKModel(id="batch-2", name="Batch Two"),
    ]

    async with AsyncPKModel.batch_writer() as writer:
        for item in items:
            await writer.put(item)

    fetched_one = await AsyncPKModel.get_item("batch-1")
    fetched_two = await AsyncPKModel.get_item("batch-2")

    assert fetched_one is not None and fetched_one.name == "Batch One"
    assert fetched_two is not None and fetched_two.name == "Batch Two"


class AsyncPKSKModel(AsyncPrimaryKeyAndSortKeyModel):
    id: str
    sort: str
    name: str


@async_fixture
async def async_pk_sk_model(async_pk_sk_table: AsyncTable) -> AsyncPKSKModel:
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    return AsyncPKSKModel(id="partition_value", sort="sort_value", name="Test Item")


@pytest.mark.asyncio
async def test_async_primary_key_and_sort_key_model_save(
    async_pk_sk_table: AsyncTable,
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    await async_pk_sk_model.save()

    response = await async_pk_sk_table.get_item(
        Key={"id": "partition_value", "sort": "sort_value"}
    )
    assert "Item" in response
    assert response["Item"]["name"] == "Test Item"


@pytest.mark.asyncio
async def test_async_primary_key_and_sort_key_model_get_item(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    await async_pk_sk_model.save()

    fetched_item = await AsyncPKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item is not None
    assert fetched_item.name == "Test Item"


@pytest.mark.asyncio
async def test_async_primary_key_and_sort_key_model_get_item_non_existent(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    non_existent = await AsyncPKSKModel.get_item("non_existent", "non_existent")
    assert non_existent is None


@pytest.mark.asyncio
async def test_async_primary_key_and_sort_key_model_get_item_with_consistent_read(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    await async_pk_sk_model.save()

    fetched_item = await AsyncPKSKModel.get_item(
        async_pk_sk_model.id,
        async_pk_sk_model.sort,
        consistent_read=True,
    )
    assert fetched_item is not None
    assert fetched_item.name == async_pk_sk_model.name


@pytest.mark.asyncio
async def test_async_primary_key_and_sort_key_model_get_item_with_consistent_read_false(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    await async_pk_sk_model.save()

    fetched_item = await AsyncPKSKModel.get_item(
        async_pk_sk_model.id,
        async_pk_sk_model.sort,
        consistent_read=False,
    )
    assert fetched_item is not None
    assert fetched_item.name == async_pk_sk_model.name


@pytest.mark.asyncio
async def test_async_primary_key_and_sort_key_model_delete(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    await async_pk_sk_model.save()

    fetched_item = await AsyncPKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item is not None

    await async_pk_sk_model.delete()

    fetched_item_after_delete = await AsyncPKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item_after_delete is None


@pytest.mark.asyncio
async def test_async_primary_key_and_sort_key_model_delete_item(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    await async_pk_sk_model.save()

    fetched_item = await AsyncPKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item is not None

    await AsyncPKSKModel.delete_item("partition_value", "sort_value")

    fetched_item_after_delete = await AsyncPKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item_after_delete is None


@pytest.mark.asyncio
async def test_async_primary_key_and_sort_key_model_batch_writer(
    async_pk_sk_table: AsyncTable,
) -> None:
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    items = [
        AsyncPKSKModel(id="user-1", sort="order-1", name="Order One"),
        AsyncPKSKModel(id="user-1", sort="order-2", name="Order Two"),
    ]

    async with AsyncPKSKModel.batch_writer() as writer:
        for item in items:
            await writer.put(item)

    fetched_one = await AsyncPKSKModel.get_item("user-1", "order-1")
    fetched_two = await AsyncPKSKModel.get_item("user-1", "order-2")

    assert fetched_one is not None and fetched_one.name == "Order One"
    assert fetched_two is not None and fetched_two.name == "Order Two"


# pyright: reportAttributeAccessIssue=false
@pytest.mark.asyncio
async def test_async_pk_model_save_with_not_exists_condition_succeeds(
    async_pk_model: AsyncPKModel,
) -> None:
    # Should succeed - item doesn't exist
    await async_pk_model.save(condition=AsyncPKModel.attr.id.not_exists())

    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == async_pk_model.name


@pytest.mark.asyncio
async def test_async_pk_model_save_with_not_exists_condition_fails_when_exists(
    async_pk_model: AsyncPKModel,
) -> None:
    # First save without condition
    await async_pk_model.save()

    # Try to save again with not_exists condition - should fail
    pk_model_v2 = AsyncPKModel(id=async_pk_model.id, name="Updated")
    with pytest.raises(
        AsyncPKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await pk_model_v2.save(condition=AsyncPKModel.attr.id.not_exists())

    # Original should still be there
    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == async_pk_model.name


@pytest.mark.asyncio
async def test_async_pk_model_save_with_equality_condition_succeeds(
    async_pk_model: AsyncPKModel,
) -> None:
    """Save succeeds when equality condition is met."""
    # First save
    await async_pk_model.save()

    # Update with correct condition
    pk_model_v2 = AsyncPKModel(id=async_pk_model.id, name="Updated")
    await pk_model_v2.save(condition=AsyncPKModel.attr.name == async_pk_model.name)

    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == "Updated"


@pytest.mark.asyncio
async def test_async_pk_model_save_with_equality_condition_fails(
    async_pk_model: AsyncPKModel,
) -> None:
    """Save fails when equality condition is not met."""
    # First save
    await async_pk_model.save()

    # Try to update with wrong condition
    pk_model_v2 = AsyncPKModel(id=async_pk_model.id, name="Updated")
    with pytest.raises(
        AsyncPKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await pk_model_v2.save(condition=AsyncPKModel.attr.name == "WrongName")

    # Original should still be there
    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == async_pk_model.name


@pytest.mark.asyncio
async def test_async_pk_model_save_with_exists_condition_succeeds(
    async_pk_model: AsyncPKModel,
) -> None:
    """Save succeeds when item exists and condition requires exists."""
    # First save without condition
    await async_pk_model.save()

    # Update with exists condition - should succeed
    pk_model_v2 = AsyncPKModel(id=async_pk_model.id, name="Updated")
    await pk_model_v2.save(condition=AsyncPKModel.attr.id.exists())

    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == "Updated"


@pytest.mark.asyncio
async def test_async_pk_model_save_with_exists_condition_fails_when_not_exists(
    async_pk_model: AsyncPKModel,
) -> None:
    """Save fails when item doesn't exist and condition requires exists."""
    with pytest.raises(
        AsyncPKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await async_pk_model.save(condition=AsyncPKModel.attr.id.exists())

    # Item should not exist
    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is None


@pytest.mark.asyncio
async def test_async_pk_sk_model_save_with_not_exists_condition_succeeds(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """Save succeeds when item doesn't exist and condition requires not_exists."""
    # Should succeed - item doesn't exist
    await async_pk_sk_model.save(condition=AsyncPKSKModel.attr.id.not_exists())

    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == async_pk_sk_model.name


@pytest.mark.asyncio
async def test_async_pk_sk_model_save_with_not_exists_condition_fails_when_exists(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """Save fails when item exists and condition requires not_exists."""
    # First save
    await async_pk_sk_model.save()

    # Try to save again with not_exists condition - should fail
    pk_sk_model_v2 = AsyncPKSKModel(
        id=async_pk_sk_model.id,
        sort=async_pk_sk_model.sort,
        name="Updated",
    )
    with pytest.raises(
        AsyncPKSKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await pk_sk_model_v2.save(condition=AsyncPKSKModel.attr.id.not_exists())

    # Original should still be there
    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == async_pk_sk_model.name


@pytest.mark.asyncio
async def test_async_pk_sk_model_save_with_combined_condition(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """Save with AND condition combining multiple checks."""
    # First save
    await async_pk_sk_model.save()

    # Update with combined condition
    pk_sk_model_v2 = AsyncPKSKModel(
        id=async_pk_sk_model.id,
        sort=async_pk_sk_model.sort,
        name="Updated",
    )
    condition = (AsyncPKSKModel.attr.id.exists()) & (
        AsyncPKSKModel.attr.name == async_pk_sk_model.name
    )
    await pk_sk_model_v2.save(condition=condition)

    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == "Updated"


@pytest.mark.asyncio
async def test_async_pk_model_delete_with_condition_succeeds(
    async_pk_model: AsyncPKModel,
) -> None:
    """Delete succeeds when condition is met."""
    await async_pk_model.save()

    # Delete with correct condition
    await async_pk_model.delete(condition=AsyncPKModel.attr.name == async_pk_model.name)

    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is None


@pytest.mark.asyncio
async def test_async_pk_model_delete_with_condition_fails(
    async_pk_model: AsyncPKModel,
) -> None:
    """Delete fails when condition is not met."""
    await async_pk_model.save()

    # Try to delete with wrong condition
    with pytest.raises(
        AsyncPKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await async_pk_model.delete(condition=AsyncPKModel.attr.name == "WrongName")

    # Item should still exist
    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == async_pk_model.name


@pytest.mark.asyncio
async def test_async_pk_model_delete_with_exists_condition_succeeds(
    async_pk_model: AsyncPKModel,
) -> None:
    """Delete succeeds when exists condition is met."""
    await async_pk_model.save()

    await async_pk_model.delete(condition=AsyncPKModel.attr.id.exists())

    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is None


@pytest.mark.asyncio
async def test_async_pk_model_delete_item_with_condition_succeeds(
    async_pk_model: AsyncPKModel,
) -> None:
    """delete_item succeeds when condition is met."""
    await async_pk_model.save()

    await AsyncPKModel.delete_item(
        async_pk_model.id,
        condition=AsyncPKModel.attr.name == async_pk_model.name,
    )

    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is None


@pytest.mark.asyncio
async def test_async_pk_model_delete_item_with_condition_fails(
    async_pk_model: AsyncPKModel,
) -> None:
    """delete_item fails when condition is not met."""
    await async_pk_model.save()

    with pytest.raises(
        AsyncPKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await AsyncPKModel.delete_item(
            async_pk_model.id,
            condition=AsyncPKModel.attr.name == "WrongName",
        )

    # Item should still exist
    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None


@pytest.mark.asyncio
async def test_async_pk_sk_model_delete_with_condition_succeeds(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """Delete succeeds when condition is met."""
    await async_pk_sk_model.save()

    await async_pk_sk_model.delete(
        condition=AsyncPKSKModel.attr.name == async_pk_sk_model.name
    )

    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is None


@pytest.mark.asyncio
async def test_async_pk_sk_model_delete_with_condition_fails(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """Delete fails when condition is not met."""
    await async_pk_sk_model.save()

    with pytest.raises(
        AsyncPKSKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await async_pk_sk_model.delete(condition=AsyncPKSKModel.attr.name == "WrongName")

    # Item should still exist
    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == async_pk_sk_model.name


@pytest.mark.asyncio
async def test_async_pk_sk_model_delete_item_with_condition_succeeds(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """delete_item succeeds when condition is met."""
    await async_pk_sk_model.save()

    await AsyncPKSKModel.delete_item(
        async_pk_sk_model.id,
        async_pk_sk_model.sort,
        condition=AsyncPKSKModel.attr.name == async_pk_sk_model.name,
    )

    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is None


@pytest.mark.asyncio
async def test_async_pk_sk_model_delete_item_with_condition_fails(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """delete_item fails when condition is not met."""
    await async_pk_sk_model.save()

    with pytest.raises(
        AsyncPKSKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await AsyncPKSKModel.delete_item(
            async_pk_sk_model.id,
            async_pk_sk_model.sort,
            condition=AsyncPKSKModel.attr.name == "WrongName",
        )

    # Item should still exist
    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is not None


@pytest.mark.asyncio
async def test_async_pk_sk_model_query_returns_items(async_pk_sk_table: AsyncTable) -> None:
    """Query returns all items with same partition key."""
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    # Create multiple items with same partition key
    await AsyncPKSKModel(id="user-1", sort="order-1", name="Order 1").save()
    await AsyncPKSKModel(id="user-1", sort="order-2", name="Order 2").save()
    await AsyncPKSKModel(id="user-2", sort="order-1", name="Other User Order").save()

    items, last_key = await AsyncPKSKModel.query("user-1")

    assert len(items) == 2
    assert all(item.id == "user-1" for item in items)
    assert last_key is None


@pytest.mark.asyncio
async def test_async_pk_sk_model_query_with_sort_key_condition(
    async_pk_sk_table: AsyncTable,
) -> None:
    """Query with sort key condition filters by sort key."""
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    await AsyncPKSKModel(id="user-1", sort="2024-01-order", name="Jan Order").save()
    await AsyncPKSKModel(id="user-1", sort="2024-02-order", name="Feb Order").save()
    await AsyncPKSKModel(id="user-1", sort="2023-12-order", name="Dec Order").save()

    items, _ = await AsyncPKSKModel.query(
        "user-1",
        sort_key_condition=AsyncPKSKModel.attr.sort.begins_with("2024-"),
    )

    assert len(items) == 2
    assert all("2024-" in item.sort for item in items)


@pytest.mark.asyncio
async def test_async_pk_sk_model_query_with_filter_condition(
    async_pk_sk_table: AsyncTable,
) -> None:
    """Query with filter condition filters results after query."""
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    await AsyncPKSKModel(id="user-1", sort="order-1", name="Pending").save()
    await AsyncPKSKModel(id="user-1", sort="order-2", name="Completed").save()
    await AsyncPKSKModel(id="user-1", sort="order-3", name="Pending").save()

    items, _ = await AsyncPKSKModel.query(
        "user-1",
        filter_condition=AsyncPKSKModel.attr.name == "Pending",
    )

    assert len(items) == 2
    assert all(item.name == "Pending" for item in items)


@pytest.mark.asyncio
async def test_async_pk_sk_model_query_with_limit(async_pk_sk_table: AsyncTable) -> None:
    """Query with limit returns limited results."""
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    for i in range(5):
        await AsyncPKSKModel(id="user-1", sort=f"order-{i}", name=f"Order {i}").save()

    items, _ = await AsyncPKSKModel.query("user-1", limit=2)

    assert len(items) == 2


@pytest.mark.asyncio
async def test_async_pk_sk_model_query_pagination(async_pk_sk_table: AsyncTable) -> None:
    """Query supports pagination with exclusive_start_key."""
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    for i in range(5):
        await AsyncPKSKModel(id="user-1", sort=f"order-{i}", name=f"Order {i}").save()

    # First page
    page1, last_key = await AsyncPKSKModel.query("user-1", limit=2)
    assert len(page1) == 2

    if last_key:
        # Second page
        page2, _ = await AsyncPKSKModel.query("user-1", limit=2, exclusive_start_key=last_key)
        assert len(page2) >= 1  # At least one more item


@pytest.mark.asyncio
async def test_async_pk_sk_model_query_all_returns_all_items(
    async_pk_sk_table: AsyncTable,
) -> None:
    """query_all returns all matching items handling pagination."""
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    for i in range(5):
        await AsyncPKSKModel(id="user-1", sort=f"order-{i}", name=f"Order {i}").save()

    items = await AsyncPKSKModel.query_all("user-1")

    assert len(items) == 5


@pytest.mark.asyncio
async def test_async_pk_sk_model_query_all_with_sort_key_condition(
    async_pk_sk_table: AsyncTable,
) -> None:
    """query_all with sort key condition returns filtered results."""
    AsyncPKSKModel.pydamo_config = PydamoConfig(table=async_pk_sk_table)
    await AsyncPKSKModel(id="user-1", sort="2024-01", name="Jan").save()
    await AsyncPKSKModel(id="user-1", sort="2024-02", name="Feb").save()
    await AsyncPKSKModel(id="user-1", sort="2023-12", name="Dec").save()

    items = await AsyncPKSKModel.query_all(
        "user-1",
        sort_key_condition=AsyncPKSKModel.attr.sort.begins_with("2024-"),
    )

    assert len(items) == 2


@pytest.mark.asyncio
async def test_async_pk_sk_model_query_with_consistent_read(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """Query supports consistent read option."""
    await async_pk_sk_model.save()

    items, _ = await AsyncPKSKModel.query(async_pk_sk_model.id, consistent_read=True)

    assert len(items) == 1
    assert items[0].name == async_pk_sk_model.name


# GSI/LSI Tests


class AsyncGSIModel(AsyncPrimaryKeyAndSortKeyModel):
    """Model for testing GSI queries."""

    id: str
    sort: str
    status: str
    name: str


@async_fixture
async def async_gsi_model(async_pk_sk_table_with_gsi: AsyncTable) -> type[AsyncGSIModel]:
    """Configure AsyncGSIModel with the GSI table."""
    AsyncGSIModel.pydamo_config = PydamoConfig(table=async_pk_sk_table_with_gsi)
    return AsyncGSIModel


@pytest.mark.asyncio
async def test_async_query_gsi_returns_items_by_index_partition_key(
    async_gsi_model: type[AsyncGSIModel],
) -> None:
    """Query GSI returns items matching the index partition key."""
    # Create items with different statuses
    await async_gsi_model(
        id="order-1",
        sort="item-1",
        status="pending",
        name="Order 1",
    ).save()
    await async_gsi_model(
        id="order-2",
        sort="item-1",
        status="pending",
        name="Order 2",
    ).save()
    await async_gsi_model(
        id="order-3",
        sort="item-1",
        status="completed",
        name="Order 3",
    ).save()

    # Query by status using GSI
    items, _ = await async_gsi_model.query(
        partition_key_value="pending",
        index_name="status-index",
    )

    assert len(items) == 2
    assert all(item.status == "pending" for item in items)


@pytest.mark.asyncio
async def test_async_query_gsi_with_filter_condition(
    async_gsi_model: type[AsyncGSIModel],
) -> None:
    """Query GSI with filter condition filters results."""
    await async_gsi_model(
        id="order-1",
        sort="item-1",
        status="pending",
        name="Important",
    ).save()
    await async_gsi_model(
        id="order-2",
        sort="item-1",
        status="pending",
        name="Normal",
    ).save()
    await async_gsi_model(
        id="order-3",
        sort="item-1",
        status="pending",
        name="Important",
    ).save()

    items, _ = await async_gsi_model.query(
        partition_key_value="pending",
        index_name="status-index",
        filter_condition=async_gsi_model.attr.name == "Important",
    )

    assert len(items) == 2
    assert all(item.name == "Important" for item in items)


@pytest.mark.asyncio
async def test_async_query_gsi_with_limit(async_gsi_model: type[AsyncGSIModel]) -> None:
    """Query GSI with limit returns limited results."""
    for i in range(5):
        await async_gsi_model(
            id=f"order-{i}",
            sort="item-1",
            status="pending",
            name=f"Order {i}",
        ).save()

    items, _ = await async_gsi_model.query(
        partition_key_value="pending",
        index_name="status-index",
        limit=2,
    )

    assert len(items) == 2


@pytest.mark.asyncio
async def test_async_query_all_gsi_returns_all_items(
    async_gsi_model: type[AsyncGSIModel],
) -> None:
    """query_all on GSI returns all matching items."""
    for i in range(5):
        await async_gsi_model(
            id=f"order-{i}",
            sort="item-1",
            status="active",
            name=f"Order {i}",
        ).save()

    items = await async_gsi_model.query_all(
        partition_key_value="active",
        index_name="status-index",
    )

    assert len(items) == 5
    assert all(item.status == "active" for item in items)


@pytest.mark.asyncio
async def test_async_query_without_index_uses_table_partition_key(
    async_gsi_model: type[AsyncGSIModel],
) -> None:
    """Query without index_name uses the base table partition key."""
    await async_gsi_model(
        id="user-1",
        sort="order-1",
        status="pending",
        name="Order 1",
    ).save()
    await async_gsi_model(
        id="user-1",
        sort="order-2",
        status="completed",
        name="Order 2",
    ).save()
    await async_gsi_model(
        id="user-2",
        sort="order-1",
        status="pending",
        name="Other Order",
    ).save()

    # Query by base table partition key
    items, _ = await async_gsi_model.query(partition_key_value="user-1")

    assert len(items) == 2
    assert all(item.id == "user-1" for item in items)


@pytest.mark.asyncio
async def test_async_query_nonexistent_index_raises_error(
    async_gsi_model: type[AsyncGSIModel],
) -> None:
    """Query with nonexistent index raises IndexNotFoundError."""
    with pytest.raises(IndexNotFoundError) as exc_info:
        await async_gsi_model.query(
            partition_key_value="test",
            index_name="nonexistent-index",
        )

    assert exc_info.value.index_name == "nonexistent-index"


class AsyncLSIModel(AsyncPrimaryKeyAndSortKeyModel):
    """Model for testing LSI queries."""

    id: str
    sort: str
    created_at: str
    name: str


@async_fixture
async def async_lsi_model(async_pk_sk_table_with_lsi: AsyncTable) -> type[AsyncLSIModel]:
    """Configure AsyncLSIModel with the LSI table."""
    AsyncLSIModel.pydamo_config = PydamoConfig(table=async_pk_sk_table_with_lsi)
    return AsyncLSIModel


@pytest.mark.asyncio
async def test_async_query_lsi_returns_items_sorted_by_index_sort_key(
    async_lsi_model: type[AsyncLSIModel],
) -> None:
    """Query LSI returns items with same partition key, sorted by index sort key."""
    # Create items with same partition key but different created_at values
    await async_lsi_model(
        id="user-1",
        sort="order-1",
        created_at="2024-01-01",
        name="Jan Order",
    ).save()
    await async_lsi_model(
        id="user-1",
        sort="order-2",
        created_at="2024-03-01",
        name="Mar Order",
    ).save()
    await async_lsi_model(
        id="user-1",
        sort="order-3",
        created_at="2024-02-01",
        name="Feb Order",
    ).save()

    # Query by LSI - should get items ordered by created_at
    items, _ = await async_lsi_model.query(
        partition_key_value="user-1",
        index_name="created-at-index",
    )

    assert len(items) == 3
    # Items should be sorted by created_at ascending
    assert items[0].created_at == "2024-01-01"
    assert items[1].created_at == "2024-02-01"
    assert items[2].created_at == "2024-03-01"


@pytest.mark.asyncio
async def test_async_query_lsi_with_sort_key_condition(
    async_lsi_model: type[AsyncLSIModel],
) -> None:
    """Query LSI with sort key condition on index sort key."""
    await async_lsi_model(
        id="user-1",
        sort="order-1",
        created_at="2024-01-15",
        name="Jan Order",
    ).save()
    await async_lsi_model(
        id="user-1",
        sort="order-2",
        created_at="2024-02-15",
        name="Feb Order",
    ).save()
    await async_lsi_model(
        id="user-1",
        sort="order-3",
        created_at="2024-03-15",
        name="Mar Order",
    ).save()

    # Query items created in 2024-02 or later
    items, _ = await async_lsi_model.query(
        partition_key_value="user-1",
        index_name="created-at-index",
        sort_key_condition=async_lsi_model.attr.created_at >= "2024-02-01",
    )

    assert len(items) == 2
    assert all(item.created_at >= "2024-02-01" for item in items)


@pytest.mark.asyncio
async def test_async_query_all_lsi_returns_all_items(
    async_lsi_model: type[AsyncLSIModel],
) -> None:
    """query_all on LSI returns all matching items."""
    for i in range(5):
        await async_lsi_model(
            id="user-1",
            sort=f"order-{i}",
            created_at=f"2024-0{i + 1}-01",
            name=f"Order {i}",
        ).save()

    items = await async_lsi_model.query_all(
        partition_key_value="user-1",
        index_name="created-at-index",
    )

    assert len(items) == 5


@pytest.mark.asyncio
async def test_async_get_index_key_attributes_for_gsi(
    async_gsi_model: type[AsyncGSIModel],
) -> None:
    """_get_index_key_attributes returns correct keys for GSI."""
    pk_attr, sk_attr = await async_gsi_model._get_index_key_attributes(
        index_name="status-index"
    )

    assert pk_attr == "status"
    assert sk_attr is None


@pytest.mark.asyncio
async def test_async_get_index_key_attributes_for_lsi(
    async_lsi_model: type[AsyncLSIModel],
) -> None:
    """_get_index_key_attributes returns correct keys for LSI."""
    pk_attr, sk_attr = await async_lsi_model._get_index_key_attributes(
        index_name="created-at-index"
    )

    assert pk_attr == "id"
    assert sk_attr == "created_at"


# Test update operations


@pytest.mark.asyncio
async def test_async_pk_model_update_item(async_pk_model: AsyncPKModel) -> None:
    """update_item modifies an existing item."""
    await async_pk_model.save()

    await AsyncPKModel.update_item(
        async_pk_model.id,
        updates={AsyncPKModel.attr.name: "Updated Name"},
    )

    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == "Updated Name"


@pytest.mark.asyncio
async def test_async_pk_sk_model_update_item(async_pk_sk_model: AsyncPKSKModel) -> None:
    """update_item modifies an existing item."""
    await async_pk_sk_model.save()

    await AsyncPKSKModel.update_item(
        async_pk_sk_model.id,
        async_pk_sk_model.sort,
        updates={AsyncPKSKModel.attr.name: "Updated Name"},
    )

    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == "Updated Name"


@pytest.mark.asyncio
async def test_async_pk_model_update_item_with_condition_succeeds(
    async_pk_model: AsyncPKModel,
) -> None:
    """update_item with condition succeeds when condition is met."""
    await async_pk_model.save()

    await AsyncPKModel.update_item(
        async_pk_model.id,
        updates={AsyncPKModel.attr.name: "Updated Name"},
        condition=AsyncPKModel.attr.name == async_pk_model.name,
    )

    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == "Updated Name"


@pytest.mark.asyncio
async def test_async_pk_model_update_item_with_condition_fails(
    async_pk_model: AsyncPKModel,
) -> None:
    """update_item with condition fails when condition is not met."""
    await async_pk_model.save()

    with pytest.raises(
        AsyncPKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await AsyncPKModel.update_item(
            async_pk_model.id,
            updates={AsyncPKModel.attr.name: "Updated Name"},
            condition=AsyncPKModel.attr.name == "WrongName",
        )

    # Original should still be there
    fetched = await AsyncPKModel.get_item(async_pk_model.id)
    assert fetched is not None
    assert fetched.name == async_pk_model.name


@pytest.mark.asyncio
async def test_async_pk_sk_model_update_item_with_condition_succeeds(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """update_item with condition succeeds when condition is met."""
    await async_pk_sk_model.save()

    await AsyncPKSKModel.update_item(
        async_pk_sk_model.id,
        async_pk_sk_model.sort,
        updates={AsyncPKSKModel.attr.name: "Updated Name"},
        condition=AsyncPKSKModel.attr.name == async_pk_sk_model.name,
    )

    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == "Updated Name"


@pytest.mark.asyncio
async def test_async_pk_sk_model_update_item_with_condition_fails(
    async_pk_sk_model: AsyncPKSKModel,
) -> None:
    """update_item with condition fails when condition is not met."""
    await async_pk_sk_model.save()

    with pytest.raises(
        AsyncPKSKModel._table().meta.client.exceptions.ConditionalCheckFailedException
    ):
        await AsyncPKSKModel.update_item(
            async_pk_sk_model.id,
            async_pk_sk_model.sort,
            updates={AsyncPKSKModel.attr.name: "Updated Name"},
            condition=AsyncPKSKModel.attr.name == "WrongName",
        )

    # Original should still be there
    fetched = await AsyncPKSKModel.get_item(async_pk_sk_model.id, async_pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == async_pk_sk_model.name
