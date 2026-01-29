from typing import Any
from unittest.mock import AsyncMock

import pytest

from pydamodb.async_models import AsyncPrimaryKeyAndSortKeyModel, AsyncPrimaryKeyModel
from pydamodb.base import PydamoConfig
from pydamodb.exceptions import InvalidKeySchemaError, MissingSortKeyValueError


def _create_mock_async_table(pk_name: str = "id", sk_name: str | None = None) -> AsyncMock:
    """Create a mock async DynamoDB table with the specified key schema.

    Note: For unit tests, we set key_schema as a plain list since unit tests
    don't actually use async I/O and test internal methods directly.
    Integration tests should use actual coroutines.
    """
    mock_table = AsyncMock()
    key_schema = [{"AttributeName": pk_name, "KeyType": "HASH"}]
    if sk_name is not None:
        key_schema.append({"AttributeName": sk_name, "KeyType": "RANGE"})

    # For unit tests, set key_schema as a plain list for synchronous access
    # This allows internal methods to work without calling _ensure_schema_loaded()
    mock_table.key_schema = key_schema

    return mock_table


def _cache_schema_for_mock(model_class: type[Any]) -> None:
    """Pre-cache key schema for mock tables in unit tests."""
    model_class._cached_key_schema = model_class._table().key_schema


class TestAsyncPrimaryKeyModelInit:
    """Test AsyncPrimaryKeyModel initialization."""

    def test_model_with_pk_only_table(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            title: str
            description: str | None = None

        item = TestModel(id="partition_value", title="Hello, world!")
        assert item.id == "partition_value"
        assert item.title == "Hello, world!"
        assert item.description is None


class TestAsyncPrimaryKeyAndSortKeyModelInit:
    """Test AsyncPrimaryKeyAndSortKeyModel initialization."""

    def test_model_with_pk_and_sk_table(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id", sk_name="sort")

        class TestModel(AsyncPrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            title: str
            description: str | None = None

        item = TestModel(id="partition_value", sort="sort_value", title="Hello, world!")
        assert item.id == "partition_value"
        assert item.sort == "sort_value"
        assert item.title == "Hello, world!"
        assert item.description is None


class TestAsyncPrimaryKeyModelKeyAttributes:
    """Test key attribute detection from table schema for AsyncPrimaryKeyModel."""

    def test_get_keys_attributes_pk_only(self) -> None:
        mock_table = _create_mock_async_table(pk_name="user_id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            user_id: str
            name: str

        _cache_schema_for_mock(TestModel)

        pk_attr = TestModel._partition_key_attribute()
        sk_attr = TestModel._sort_key_attribute()
        assert pk_attr == "user_id"
        assert sk_attr is None

    def test_partition_key_attribute(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        _cache_schema_for_mock(TestModel)

        assert TestModel._partition_key_attribute() == "id"

    def test_sort_key_attribute_is_none(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        _cache_schema_for_mock(TestModel)

        assert TestModel._sort_key_attribute() is None


class TestAsyncPrimaryKeyAndSortKeyModelKeyAttributes:
    """Test key attribute detection from table schema for AsyncPrimaryKeyAndSortKeyModel."""

    def test_get_keys_attributes_pk_and_sk(self) -> None:
        mock_table = _create_mock_async_table(pk_name="user_id", sk_name="order_id")

        class TestModel(AsyncPrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            user_id: str
            order_id: str
            name: str

        _cache_schema_for_mock(TestModel)

        pk_attr = TestModel._partition_key_attribute()
        sk_attr = TestModel._sort_key_attribute()
        assert pk_attr == "user_id"
        assert sk_attr == "order_id"

    def test_partition_key_attribute(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id", sk_name="sort")

        class TestModel(AsyncPrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        _cache_schema_for_mock(TestModel)

        assert TestModel._partition_key_attribute() == "id"

    def test_sort_key_attribute(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id", sk_name="sort")

        class TestModel(AsyncPrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        _cache_schema_for_mock(TestModel)

        assert TestModel._sort_key_attribute() == "sort"


class TestAsyncPrimaryKeyModelBuildDynamoDBKey:
    """Test building DynamoDB keys for AsyncPrimaryKeyModel."""

    def test_build_dynamodb_key(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        _cache_schema_for_mock(TestModel)

        key = TestModel._build_dynamodb_key(partition_key_value="partition_value")
        assert key == {"id": "partition_value"}


class TestAsyncPrimaryKeyAndSortKeyModelBuildDynamoDBKey:
    """Test building DynamoDB keys for AsyncPrimaryKeyAndSortKeyModel."""

    def test_build_dynamodb_key(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id", sk_name="sort")

        class TestModel(AsyncPrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        _cache_schema_for_mock(TestModel)

        key = TestModel._build_dynamodb_key(
            partition_key_value="partition_value",
            sort_key_value="sort_value",
        )
        assert key == {"id": "partition_value", "sort": "sort_value"}


class TestAsyncPrimaryKeyModelKeyValues:
    """Test partition key value properties for AsyncPrimaryKeyModel."""

    def test_partition_key_value(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        _cache_schema_for_mock(TestModel)

        item = TestModel(id="my-id", name="test")
        assert item._partition_key_value == "my-id"

    def test_sort_key_value_is_none(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        _cache_schema_for_mock(TestModel)

        item = TestModel(id="my-id", name="test")
        assert item._sort_key_value is None


class TestAsyncPrimaryKeyAndSortKeyModelKeyValues:
    """Test partition and sort key value properties for AsyncPrimaryKeyAndSortKeyModel."""

    def test_partition_key_value(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id", sk_name="sort")

        class TestModel(AsyncPrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        _cache_schema_for_mock(TestModel)

        item = TestModel(id="my-id", sort="my-sort", name="test")
        assert item._partition_key_value == "my-id"

    def test_sort_key_value(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id", sk_name="sort")

        class TestModel(AsyncPrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        _cache_schema_for_mock(TestModel)

        item = TestModel(id="my-id", sort="my-sort", name="test")
        assert item._sort_key_value == "my-sort"


class TestInvalidAsyncTableSchema:
    """Test error handling for invalid table schemas."""

    def test_invalid_table_schema_no_partition_key(self) -> None:
        mock_table = AsyncMock()
        mock_table.key_schema = [{"AttributeName": "sort", "KeyType": "RANGE"}]

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        _cache_schema_for_mock(TestModel)

        with pytest.raises(InvalidKeySchemaError):
            TestModel._partition_key_attribute()


class TestBuildAsyncDynamoDBKeyErrors:
    """Test error handling when building DynamoDB keys."""

    def test_missing_sort_key_value_raises_error(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id", sk_name="sort")

        class TestModel(AsyncPrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        _cache_schema_for_mock(TestModel)

        with pytest.raises(MissingSortKeyValueError) as exc_info:
            TestModel._build_dynamodb_key(partition_key_value="pk_value", sort_key_value=None)

        assert "TestModel" in str(exc_info.value)


class TestAsyncParseKeySchema:
    """Test _parse_key_schema static method for async models."""

    def test_parse_key_schema_with_partition_key_only(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str

        key_schema = [{"AttributeName": "pk", "KeyType": "HASH"}]
        pk, sk = TestModel._parse_key_schema(key_schema=key_schema)  # type: ignore[arg-type]
        assert pk == "pk"
        assert sk is None

    def test_parse_key_schema_with_partition_and_sort_key(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str

        key_schema = [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ]
        pk, sk = TestModel._parse_key_schema(key_schema=key_schema)  # type: ignore[arg-type]
        assert pk == "pk"
        assert sk == "sk"

    def test_parse_key_schema_invalid_no_partition_key(self) -> None:
        mock_table = _create_mock_async_table(pk_name="id")

        class TestModel(AsyncPrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str

        # Key schema with only a sort key, no partition key
        key_schema = [{"AttributeName": "sk", "KeyType": "RANGE"}]
        with pytest.raises(InvalidKeySchemaError):
            TestModel._parse_key_schema(key_schema=key_schema)  # type: ignore[arg-type]
