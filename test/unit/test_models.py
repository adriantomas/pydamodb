from unittest.mock import MagicMock

import pytest

from pydamodb.exceptions import InvalidKeySchemaError, MissingSortKeyValueError
from pydamodb.models import PrimaryKeyAndSortKeyModel, PrimaryKeyModel, PydamoConfig


def _create_mock_table(pk_name: str = "id", sk_name: str | None = None) -> MagicMock:
    """Create a mock DynamoDB table with the specified key schema."""
    mock_table = MagicMock()
    key_schema = [{"AttributeName": pk_name, "KeyType": "HASH"}]
    if sk_name is not None:
        key_schema.append({"AttributeName": sk_name, "KeyType": "RANGE"})
    mock_table.key_schema = key_schema
    return mock_table


class TestPrimaryKeyModelInit:
    """Test PrimaryKeyModel initialization."""

    def test_model_with_pk_only_table(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            title: str
            description: str | None = None

        item = TestModel(id="partition_value", title="Hello, world!")
        assert item.id == "partition_value"
        assert item.title == "Hello, world!"
        assert item.description is None


class TestPrimaryKeyAndSortKeyModelInit:
    """Test PrimaryKeyAndSortKeyModel initialization."""

    def test_model_with_pk_and_sk_table(self) -> None:
        mock_table = _create_mock_table(pk_name="id", sk_name="sort")

        class TestModel(PrimaryKeyAndSortKeyModel):
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


class TestPrimaryKeyModelKeyAttributes:
    """Test key attribute detection from table schema for PrimaryKeyModel."""

    def test_get_keys_attributes_pk_only(self) -> None:
        mock_table = _create_mock_table(pk_name="user_id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            user_id: str
            name: str

        pk_attr, sk_attr = TestModel._get_keys_attributes()
        assert pk_attr == "user_id"
        assert sk_attr is None

    def test_partition_key_attribute(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        assert TestModel._partition_key_attribute() == "id"

    def test_sort_key_attribute_is_none(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        assert TestModel._sort_key_attribute() is None


class TestPrimaryKeyAndSortKeyModelKeyAttributes:
    """Test key attribute detection from table schema for PrimaryKeyAndSortKeyModel."""

    def test_get_keys_attributes_pk_and_sk(self) -> None:
        mock_table = _create_mock_table(pk_name="user_id", sk_name="order_id")

        class TestModel(PrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            user_id: str
            order_id: str
            name: str

        pk_attr, sk_attr = TestModel._get_keys_attributes()
        assert pk_attr == "user_id"
        assert sk_attr == "order_id"

    def test_partition_key_attribute(self) -> None:
        mock_table = _create_mock_table(pk_name="id", sk_name="sort")

        class TestModel(PrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        assert TestModel._partition_key_attribute() == "id"

    def test_sort_key_attribute(self) -> None:
        mock_table = _create_mock_table(pk_name="id", sk_name="sort")

        class TestModel(PrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        assert TestModel._sort_key_attribute() == "sort"


class TestPrimaryKeyModelBuildDynamoDBKey:
    """Test building DynamoDB keys for PrimaryKeyModel."""

    def test_build_dynamodb_key(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        key = TestModel._build_dynamodb_key(partition_key_value="partition_value")
        assert key == {"id": "partition_value"}


class TestPrimaryKeyAndSortKeyModelBuildDynamoDBKey:
    """Test building DynamoDB keys for PrimaryKeyAndSortKeyModel."""

    def test_build_dynamodb_key(self) -> None:
        mock_table = _create_mock_table(pk_name="id", sk_name="sort")

        class TestModel(PrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        key = TestModel._build_dynamodb_key(
            partition_key_value="partition_value", sort_key_value="sort_value"
        )
        assert key == {"id": "partition_value", "sort": "sort_value"}


class TestPrimaryKeyModelKeyValues:
    """Test partition key value properties for PrimaryKeyModel."""

    def test_partition_key_value(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        item = TestModel(id="my-id", name="test")
        assert item._partition_key_value == "my-id"

    def test_sort_key_value_is_none(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        item = TestModel(id="my-id", name="test")
        assert item._sort_key_value is None


class TestPrimaryKeyAndSortKeyModelKeyValues:
    """Test partition and sort key value properties for PrimaryKeyAndSortKeyModel."""

    def test_partition_key_value(self) -> None:
        mock_table = _create_mock_table(pk_name="id", sk_name="sort")

        class TestModel(PrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        item = TestModel(id="my-id", sort="my-sort", name="test")
        assert item._partition_key_value == "my-id"

    def test_sort_key_value(self) -> None:
        mock_table = _create_mock_table(pk_name="id", sk_name="sort")

        class TestModel(PrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        item = TestModel(id="my-id", sort="my-sort", name="test")
        assert item._sort_key_value == "my-sort"


class TestInvalidTableSchema:
    """Test error handling for invalid table schemas."""

    def test_invalid_table_schema_no_partition_key(self) -> None:
        mock_table = MagicMock()
        mock_table.key_schema = [{"AttributeName": "sort", "KeyType": "RANGE"}]

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(InvalidKeySchemaError):
            TestModel._get_keys_attributes()


class TestBuildDynamoDBKeyErrors:
    """Test error handling when building DynamoDB keys."""

    def test_missing_sort_key_value_raises_error(self) -> None:
        mock_table = _create_mock_table(pk_name="id", sk_name="sort")

        class TestModel(PrimaryKeyAndSortKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            sort: str
            name: str

        with pytest.raises(MissingSortKeyValueError) as exc_info:
            TestModel._build_dynamodb_key(partition_key_value="pk_value", sort_key_value=None)

        assert "TestModel" in str(exc_info.value)


class TestParseKeySchema:
    """Test _parse_key_schema static method."""

    def test_parse_key_schema_with_partition_key_only(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str

        key_schema = [{"AttributeName": "pk", "KeyType": "HASH"}]
        pk, sk = TestModel._parse_key_schema(key_schema=key_schema)
        assert pk == "pk"
        assert sk is None

    def test_parse_key_schema_with_partition_and_sort_key(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str

        key_schema = [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ]
        pk, sk = TestModel._parse_key_schema(key_schema=key_schema)
        assert pk == "pk"
        assert sk == "sk"

    def test_parse_key_schema_invalid_no_partition_key(self) -> None:
        mock_table = _create_mock_table(pk_name="id")

        class TestModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str

        # Key schema with only a sort key, no partition key
        key_schema = [{"AttributeName": "sk", "KeyType": "RANGE"}]
        with pytest.raises(InvalidKeySchemaError):
            TestModel._parse_key_schema(key_schema=key_schema)
