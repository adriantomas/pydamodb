import pytest
from mypy_boto3_dynamodb.service_resource import Table

from pydamodb.exceptions import ConditionCheckFailedError, IndexNotFoundError
from pydamodb.models import PrimaryKeyAndSortKeyModel, PrimaryKeyModel, PydamoConfig


class PKModel(PrimaryKeyModel):
    pydamo_config = PydamoConfig(table=None)  # type: ignore[typeddict-item]
    id: str
    name: str


@pytest.fixture
def pk_model(pk_table: Table) -> PKModel:
    PKModel.pydamo_config = PydamoConfig(table=pk_table)
    return PKModel(id="partition_value", name="Test Item")


def test_primary_key_model_save(pk_table: Table, pk_model: PKModel) -> None:
    pk_model.save()

    response = pk_table.get_item(Key={"id": "partition_value"})
    assert "Item" in response
    assert response["Item"]["name"] == "Test Item"


def test_primary_key_model_get_item(pk_model: PKModel) -> None:
    pk_model.save()

    fetched_item = PKModel.get_item("partition_value")
    assert fetched_item is not None
    assert fetched_item.name == "Test Item"


def test_primary_key_model_get_item_non_existent(pk_model: PKModel) -> None:
    non_existent = PKModel.get_item("non_existent")
    assert non_existent is None


def test_primary_key_model_get_item_with_consistent_read(pk_model: PKModel) -> None:
    pk_model.save()

    fetched_item = PKModel.get_item(pk_model.id, consistent_read=True)
    assert fetched_item is not None
    assert fetched_item.name == pk_model.name


def test_primary_key_model_get_item_with_consistent_read_false(
    pk_model: PKModel,
) -> None:
    pk_model.save()

    fetched_item = PKModel.get_item(pk_model.id, consistent_read=False)
    assert fetched_item is not None
    assert fetched_item.name == pk_model.name


def test_primary_key_model_delete(pk_model: PKModel) -> None:
    pk_model.save()

    fetched_item = PKModel.get_item("partition_value")
    assert fetched_item is not None

    pk_model.delete()

    fetched_item_after_delete = PKModel.get_item("partition_value")
    assert fetched_item_after_delete is None


def test_primary_key_model_delete_item(pk_model: PKModel) -> None:
    pk_model.save()

    fetched_item = PKModel.get_item("partition_value")
    assert fetched_item is not None

    PKModel.delete_item("partition_value")

    fetched_item_after_delete = PKModel.get_item("partition_value")
    assert fetched_item_after_delete is None


def test_primary_key_model_batch_writer(pk_table: Table) -> None:
    PKModel.pydamo_config = PydamoConfig(table=pk_table)

    items = [
        PKModel(id="batch-1", name="Batch One"),
        PKModel(id="batch-2", name="Batch Two"),
    ]

    with PKModel.batch_writer() as writer:
        for item in items:
            writer.put(item)

    fetched_one = PKModel.get_item("batch-1")
    fetched_two = PKModel.get_item("batch-2")

    assert fetched_one is not None and fetched_one.name == "Batch One"
    assert fetched_two is not None and fetched_two.name == "Batch Two"


class PKSKModel(PrimaryKeyAndSortKeyModel):
    id: str
    sort: str
    name: str


@pytest.fixture
def pk_sk_model(pk_sk_table: Table) -> PKSKModel:
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)
    return PKSKModel(id="partition_value", sort="sort_value", name="Test Item")


def test_primary_key_and_sort_key_model_save(
    pk_sk_table: Table,
    pk_sk_model: PKSKModel,
) -> None:
    pk_sk_model.save()

    response = pk_sk_table.get_item(Key={"id": "partition_value", "sort": "sort_value"})
    assert "Item" in response
    assert response["Item"]["name"] == "Test Item"


def test_primary_key_and_sort_key_model_get_item(pk_sk_model: PKSKModel) -> None:
    pk_sk_model.save()

    fetched_item = PKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item is not None
    assert fetched_item.name == "Test Item"


def test_primary_key_and_sort_key_model_get_item_non_existent(
    pk_sk_model: PKSKModel,
) -> None:
    non_existent = PKSKModel.get_item("non_existent", "non_existent")
    assert non_existent is None


def test_primary_key_and_sort_key_model_get_item_with_consistent_read(
    pk_sk_model: PKSKModel,
) -> None:
    pk_sk_model.save()

    fetched_item = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort, consistent_read=True)
    assert fetched_item is not None
    assert fetched_item.name == pk_sk_model.name


def test_primary_key_and_sort_key_model_get_item_with_consistent_read_false(
    pk_sk_model: PKSKModel,
) -> None:
    pk_sk_model.save()

    fetched_item = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort, consistent_read=False)
    assert fetched_item is not None
    assert fetched_item.name == pk_sk_model.name


def test_primary_key_and_sort_key_model_delete(pk_sk_model: PKSKModel) -> None:
    pk_sk_model.save()

    fetched_item = PKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item is not None

    pk_sk_model.delete()

    fetched_item_after_delete = PKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item_after_delete is None


def test_primary_key_and_sort_key_model_delete_item(pk_sk_model: PKSKModel) -> None:
    pk_sk_model.save()

    fetched_item = PKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item is not None

    PKSKModel.delete_item("partition_value", "sort_value")

    fetched_item_after_delete = PKSKModel.get_item("partition_value", "sort_value")
    assert fetched_item_after_delete is None


def test_primary_key_and_sort_key_model_batch_writer(pk_sk_table: Table) -> None:
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)

    items = [
        PKSKModel(id="user-1", sort="order-1", name="Order One"),
        PKSKModel(id="user-1", sort="order-2", name="Order Two"),
    ]

    with PKSKModel.batch_writer() as writer:
        for item in items:
            writer.put(item)

    fetched_one = PKSKModel.get_item("user-1", "order-1")
    fetched_two = PKSKModel.get_item("user-1", "order-2")

    assert fetched_one is not None and fetched_one.name == "Order One"
    assert fetched_two is not None and fetched_two.name == "Order Two"


# pyright: reportAttributeAccessIssue=false
def test_pk_model_save_with_not_exists_condition_succeeds(
    pk_model: PKModel,
) -> None:
    # Should succeed - item doesn't exist
    pk_model.save(condition=PKModel.attr.id.not_exists())

    fetched = PKModel.get_item(pk_model.id)
    assert fetched is not None
    assert fetched.name == pk_model.name


def test_pk_model_save_with_not_exists_condition_fails_when_exists(
    pk_model: PKModel,
) -> None:
    # First save without condition
    pk_model.save()

    # Try to save again with not_exists condition - should fail
    pk_model_v2 = PKModel(id=pk_model.id, name="Updated")
    with pytest.raises(ConditionCheckFailedError):
        pk_model_v2.save(condition=PKModel.attr.id.not_exists())

    # Original should still be there
    fetched = PKModel.get_item(pk_model.id)
    assert fetched is not None
    assert fetched.name == pk_model.name


def test_pk_model_save_with_equality_condition_succeeds(
    pk_model: PKModel,
) -> None:
    """Save succeeds when equality condition is met."""
    # First save
    pk_model.save()

    # Update with correct condition
    pk_model_v2 = PKModel(id=pk_model.id, name="Updated")
    pk_model_v2.save(condition=PKModel.attr.name == pk_model.name)

    fetched = PKModel.get_item(pk_model.id)
    assert fetched is not None
    assert fetched.name == "Updated"


def test_pk_model_save_with_equality_condition_fails(
    pk_model: PKModel,
) -> None:
    """Save fails when equality condition is not met."""
    # First save
    pk_model.save()

    # Try to update with wrong condition
    pk_model_v2 = PKModel(id=pk_model.id, name="Updated")
    with pytest.raises(ConditionCheckFailedError):
        pk_model_v2.save(condition=PKModel.attr.name == "WrongName")

    # Original should still be there
    fetched = PKModel.get_item(pk_model.id)
    assert fetched is not None
    assert fetched.name == pk_model.name


def test_pk_model_save_with_exists_condition_succeeds(
    pk_model: PKModel,
) -> None:
    """Save succeeds when item exists and condition requires exists."""
    # First save without condition
    pk_model.save()

    # Update with exists condition - should succeed
    pk_model_v2 = PKModel(id=pk_model.id, name="Updated")
    pk_model_v2.save(condition=PKModel.attr.id.exists())

    fetched = PKModel.get_item(pk_model.id)
    assert fetched is not None
    assert fetched.name == "Updated"


def test_pk_model_save_with_exists_condition_fails_when_not_exists(
    pk_model: PKModel,
) -> None:
    """Save fails when item doesn't exist and condition requires exists."""
    with pytest.raises(ConditionCheckFailedError):
        pk_model.save(condition=PKModel.attr.id.exists())

    # Item should not exist
    fetched = PKModel.get_item(pk_model.id)
    assert fetched is None


def test_pk_sk_model_save_with_not_exists_condition_succeeds(
    pk_sk_model: PKSKModel,
) -> None:
    """Save succeeds when item doesn't exist and condition requires not_exists."""
    # Should succeed - item doesn't exist
    pk_sk_model.save(condition=PKSKModel.attr.id.not_exists())

    fetched = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == pk_sk_model.name


def test_pk_sk_model_save_with_not_exists_condition_fails_when_exists(
    pk_sk_model: PKSKModel,
) -> None:
    """Save fails when item exists and condition requires not_exists."""
    # First save
    pk_sk_model.save()

    # Try to save again with not_exists condition - should fail
    pk_sk_model_v2 = PKSKModel(id=pk_sk_model.id, sort=pk_sk_model.sort, name="Updated")
    with pytest.raises(ConditionCheckFailedError):
        pk_sk_model_v2.save(condition=PKSKModel.attr.id.not_exists())

    # Original should still be there
    fetched = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == pk_sk_model.name


def test_pk_sk_model_save_with_combined_condition(
    pk_sk_model: PKSKModel,
) -> None:
    """Save with AND condition combining multiple checks."""
    # First save
    pk_sk_model.save()

    # Update with combined condition
    pk_sk_model_v2 = PKSKModel(id=pk_sk_model.id, sort=pk_sk_model.sort, name="Updated")
    condition = (PKSKModel.attr.id.exists()) & (PKSKModel.attr.name == pk_sk_model.name)
    pk_sk_model_v2.save(condition=condition)

    fetched = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == "Updated"


def test_pk_model_delete_with_condition_succeeds(pk_model: PKModel) -> None:
    """Delete succeeds when condition is met."""
    pk_model.save()

    # Delete with correct condition
    pk_model.delete(condition=PKModel.attr.name == pk_model.name)

    fetched = PKModel.get_item(pk_model.id)
    assert fetched is None


def test_pk_model_delete_with_condition_fails(pk_model: PKModel) -> None:
    """Delete fails when condition is not met."""
    pk_model.save()

    # Try to delete with wrong condition
    with pytest.raises(ConditionCheckFailedError):
        pk_model.delete(condition=PKModel.attr.name == "WrongName")

    # Item should still exist
    fetched = PKModel.get_item(pk_model.id)
    assert fetched is not None
    assert fetched.name == pk_model.name


def test_pk_model_delete_with_exists_condition_succeeds(pk_model: PKModel) -> None:
    """Delete succeeds when exists condition is met."""
    pk_model.save()

    pk_model.delete(condition=PKModel.attr.id.exists())

    fetched = PKModel.get_item(pk_model.id)
    assert fetched is None


def test_pk_model_delete_item_with_condition_succeeds(pk_model: PKModel) -> None:
    """delete_item succeeds when condition is met."""
    pk_model.save()

    PKModel.delete_item(pk_model.id, condition=PKModel.attr.name == pk_model.name)

    fetched = PKModel.get_item(pk_model.id)
    assert fetched is None


def test_pk_model_delete_item_with_condition_fails(pk_model: PKModel) -> None:
    """delete_item fails when condition is not met."""
    pk_model.save()

    with pytest.raises(ConditionCheckFailedError):
        PKModel.delete_item(pk_model.id, condition=PKModel.attr.name == "WrongName")

    # Item should still exist
    fetched = PKModel.get_item(pk_model.id)
    assert fetched is not None


def test_pk_sk_model_delete_with_condition_succeeds(pk_sk_model: PKSKModel) -> None:
    """Delete succeeds when condition is met."""
    pk_sk_model.save()

    pk_sk_model.delete(condition=PKSKModel.attr.name == pk_sk_model.name)

    fetched = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort)
    assert fetched is None


def test_pk_sk_model_delete_with_condition_fails(pk_sk_model: PKSKModel) -> None:
    """Delete fails when condition is not met."""
    pk_sk_model.save()

    with pytest.raises(ConditionCheckFailedError):
        pk_sk_model.delete(condition=PKSKModel.attr.name == "WrongName")

    # Item should still exist
    fetched = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort)
    assert fetched is not None
    assert fetched.name == pk_sk_model.name


def test_pk_sk_model_delete_item_with_condition_succeeds(
    pk_sk_model: PKSKModel,
) -> None:
    """delete_item succeeds when condition is met."""
    pk_sk_model.save()

    PKSKModel.delete_item(
        pk_sk_model.id,
        pk_sk_model.sort,
        condition=PKSKModel.attr.name == pk_sk_model.name,
    )

    fetched = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort)
    assert fetched is None


def test_pk_sk_model_delete_item_with_condition_fails(pk_sk_model: PKSKModel) -> None:
    """delete_item fails when condition is not met."""
    pk_sk_model.save()

    with pytest.raises(ConditionCheckFailedError):
        PKSKModel.delete_item(
            pk_sk_model.id,
            pk_sk_model.sort,
            condition=PKSKModel.attr.name == "WrongName",
        )

    # Item should still exist
    fetched = PKSKModel.get_item(pk_sk_model.id, pk_sk_model.sort)
    assert fetched is not None


def test_pk_sk_model_query_returns_items(pk_sk_table: Table) -> None:
    """Query returns all items with same partition key."""
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)

    # Create multiple items with same partition key
    PKSKModel(id="user-1", sort="order-1", name="Order 1").save()
    PKSKModel(id="user-1", sort="order-2", name="Order 2").save()
    PKSKModel(id="user-2", sort="order-1", name="Other User Order").save()

    items, last_key = PKSKModel.query("user-1")

    assert len(items) == 2
    assert all(item.id == "user-1" for item in items)
    assert last_key is None


def test_pk_sk_model_query_with_sort_key_condition(pk_sk_table: Table) -> None:
    """Query with sort key condition filters by sort key."""
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)

    PKSKModel(id="user-1", sort="2024-01-order", name="Jan Order").save()
    PKSKModel(id="user-1", sort="2024-02-order", name="Feb Order").save()
    PKSKModel(id="user-1", sort="2023-12-order", name="Dec Order").save()

    items, _ = PKSKModel.query(
        "user-1",
        sort_key_condition=PKSKModel.attr.sort.begins_with("2024-"),
    )

    assert len(items) == 2
    assert all("2024-" in item.sort for item in items)


def test_pk_sk_model_query_with_filter_condition(pk_sk_table: Table) -> None:
    """Query with filter condition filters results after query."""
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)

    PKSKModel(id="user-1", sort="order-1", name="Pending").save()
    PKSKModel(id="user-1", sort="order-2", name="Completed").save()
    PKSKModel(id="user-1", sort="order-3", name="Pending").save()

    items, _ = PKSKModel.query("user-1", filter_condition=PKSKModel.attr.name == "Pending")

    assert len(items) == 2
    assert all(item.name == "Pending" for item in items)


def test_pk_sk_model_query_with_limit(pk_sk_table: Table) -> None:
    """Query with limit returns limited results."""
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)

    for i in range(5):
        PKSKModel(id="user-1", sort=f"order-{i}", name=f"Order {i}").save()

    items, _ = PKSKModel.query("user-1", limit=2)

    assert len(items) == 2


def test_pk_sk_model_query_pagination(pk_sk_table: Table) -> None:
    """Query supports pagination with exclusive_start_key."""
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)

    for i in range(5):
        PKSKModel(id="user-1", sort=f"order-{i}", name=f"Order {i}").save()

    # First page
    page1, last_key = PKSKModel.query("user-1", limit=2)
    assert len(page1) == 2

    if last_key:
        # Second page
        page2, _ = PKSKModel.query("user-1", limit=2, exclusive_start_key=last_key)
        assert len(page2) >= 1  # At least one more item


def test_pk_sk_model_query_all_returns_all_items(pk_sk_table: Table) -> None:
    """query_all returns all matching items handling pagination."""
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)

    for i in range(5):
        PKSKModel(id="user-1", sort=f"order-{i}", name=f"Order {i}").save()

    items = PKSKModel.query_all("user-1")

    assert len(items) == 5


def test_pk_sk_model_query_all_with_sort_key_condition(pk_sk_table: Table) -> None:
    """query_all with sort key condition returns filtered results."""
    PKSKModel.pydamo_config = PydamoConfig(table=pk_sk_table)

    PKSKModel(id="user-1", sort="2024-01", name="Jan").save()
    PKSKModel(id="user-1", sort="2024-02", name="Feb").save()
    PKSKModel(id="user-1", sort="2023-12", name="Dec").save()

    items = PKSKModel.query_all(
        "user-1",
        sort_key_condition=PKSKModel.attr.sort.begins_with("2024-"),
    )

    assert len(items) == 2


def test_pk_sk_model_query_with_consistent_read(pk_sk_model: PKSKModel) -> None:
    """Query supports consistent read option."""
    pk_sk_model.save()

    items, _ = PKSKModel.query(pk_sk_model.id, consistent_read=True)

    assert len(items) == 1
    assert items[0].name == pk_sk_model.name


# GSI/LSI Tests


class GSIModel(PrimaryKeyAndSortKeyModel):
    """Model for testing GSI queries."""

    id: str
    sort: str
    status: str
    name: str


@pytest.fixture
def gsi_model(pk_sk_table_with_gsi: Table) -> type[GSIModel]:
    """Configure GSIModel with the GSI table."""
    GSIModel.pydamo_config = PydamoConfig(table=pk_sk_table_with_gsi)
    return GSIModel


def test_query_gsi_returns_items_by_index_partition_key(
    gsi_model: type[GSIModel],
) -> None:
    """Query GSI returns items matching the index partition key."""
    # Create items with different statuses
    gsi_model(id="order-1", sort="item-1", status="pending", name="Order 1").save()
    gsi_model(id="order-2", sort="item-1", status="pending", name="Order 2").save()
    gsi_model(id="order-3", sort="item-1", status="completed", name="Order 3").save()

    # Query by status using GSI
    items, _ = gsi_model.query(partition_key_value="pending", index_name="status-index")

    assert len(items) == 2
    assert all(item.status == "pending" for item in items)


def test_query_gsi_with_filter_condition(gsi_model: type[GSIModel]) -> None:
    """Query GSI with filter condition filters results."""
    gsi_model(id="order-1", sort="item-1", status="pending", name="Important").save()
    gsi_model(id="order-2", sort="item-1", status="pending", name="Normal").save()
    gsi_model(id="order-3", sort="item-1", status="pending", name="Important").save()

    items, _ = gsi_model.query(
        partition_key_value="pending",
        index_name="status-index",
        filter_condition=gsi_model.attr.name == "Important",
    )

    assert len(items) == 2
    assert all(item.name == "Important" for item in items)


def test_query_gsi_with_limit(gsi_model: type[GSIModel]) -> None:
    """Query GSI with limit returns limited results."""
    for i in range(5):
        gsi_model(id=f"order-{i}", sort="item-1", status="pending", name=f"Order {i}").save()

    items, _ = gsi_model.query(
        partition_key_value="pending",
        index_name="status-index",
        limit=2,
    )

    assert len(items) == 2


def test_query_all_gsi_returns_all_items(gsi_model: type[GSIModel]) -> None:
    """query_all on GSI returns all matching items."""
    for i in range(5):
        gsi_model(id=f"order-{i}", sort="item-1", status="active", name=f"Order {i}").save()

    items = gsi_model.query_all(partition_key_value="active", index_name="status-index")

    assert len(items) == 5
    assert all(item.status == "active" for item in items)


def test_query_without_index_uses_table_partition_key(
    gsi_model: type[GSIModel],
) -> None:
    """Query without index_name uses the base table partition key."""
    gsi_model(id="user-1", sort="order-1", status="pending", name="Order 1").save()
    gsi_model(id="user-1", sort="order-2", status="completed", name="Order 2").save()
    gsi_model(id="user-2", sort="order-1", status="pending", name="Other Order").save()

    # Query by base table partition key
    items, _ = gsi_model.query(partition_key_value="user-1")

    assert len(items) == 2
    assert all(item.id == "user-1" for item in items)


def test_query_nonexistent_index_raises_error(gsi_model: type[GSIModel]) -> None:
    """Query with nonexistent index raises IndexNotFoundError."""
    with pytest.raises(IndexNotFoundError) as exc_info:
        gsi_model.query(partition_key_value="test", index_name="nonexistent-index")

    assert exc_info.value.index_name == "nonexistent-index"


class LSIModel(PrimaryKeyAndSortKeyModel):
    """Model for testing LSI queries."""

    id: str
    sort: str
    created_at: str
    name: str


@pytest.fixture
def lsi_model(pk_sk_table_with_lsi: Table) -> type[LSIModel]:
    """Configure LSIModel with the LSI table."""
    LSIModel.pydamo_config = PydamoConfig(table=pk_sk_table_with_lsi)
    return LSIModel


def test_query_lsi_returns_items_sorted_by_index_sort_key(
    lsi_model: type[LSIModel],
) -> None:
    """Query LSI returns items with same partition key, sorted by index sort key."""
    # Create items with same partition key but different created_at values
    lsi_model(id="user-1", sort="order-1", created_at="2024-01-01", name="Jan Order").save()
    lsi_model(id="user-1", sort="order-2", created_at="2024-03-01", name="Mar Order").save()
    lsi_model(id="user-1", sort="order-3", created_at="2024-02-01", name="Feb Order").save()

    # Query by LSI - should get items ordered by created_at
    items, _ = lsi_model.query(partition_key_value="user-1", index_name="created-at-index")

    assert len(items) == 3
    # Items should be sorted by created_at ascending
    assert items[0].created_at == "2024-01-01"
    assert items[1].created_at == "2024-02-01"
    assert items[2].created_at == "2024-03-01"


def test_query_lsi_with_sort_key_condition(lsi_model: type[LSIModel]) -> None:
    """Query LSI with sort key condition on index sort key."""
    lsi_model(id="user-1", sort="order-1", created_at="2024-01-15", name="Jan Order").save()
    lsi_model(id="user-1", sort="order-2", created_at="2024-02-15", name="Feb Order").save()
    lsi_model(id="user-1", sort="order-3", created_at="2024-03-15", name="Mar Order").save()

    # Query items created in 2024-02 or later
    items, _ = lsi_model.query(
        partition_key_value="user-1",
        index_name="created-at-index",
        sort_key_condition=lsi_model.attr.created_at >= "2024-02-01",
    )

    assert len(items) == 2
    assert all(item.created_at >= "2024-02-01" for item in items)


def test_query_all_lsi_returns_all_items(lsi_model: type[LSIModel]) -> None:
    """query_all on LSI returns all matching items."""
    for i in range(5):
        lsi_model(
            id="user-1",
            sort=f"order-{i}",
            created_at=f"2024-0{i + 1}-01",
            name=f"Order {i}",
        ).save()

    items = lsi_model.query_all(partition_key_value="user-1", index_name="created-at-index")

    assert len(items) == 5


def test_get_index_key_attributes_for_gsi(gsi_model: type[GSIModel]) -> None:
    """_get_index_key_attributes returns correct keys for GSI."""
    pk_attr, sk_attr = gsi_model._get_index_key_attributes(index_name="status-index")

    assert pk_attr == "status"
    assert sk_attr is None


def test_get_index_key_attributes_for_lsi(lsi_model: type[LSIModel]) -> None:
    """_get_index_key_attributes returns correct keys for LSI."""
    pk_attr, sk_attr = lsi_model._get_index_key_attributes(index_name="created-at-index")

    assert pk_attr == "id"
    assert sk_attr == "created_at"
