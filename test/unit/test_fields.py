"""Tests for the attr() classmethod."""

from unittest.mock import MagicMock

import pytest
from pydantic import Field

from pydamodb.base import PydamoConfig
from pydamodb.expressions import ExpressionField
from pydamodb.sync_models import PrimaryKeyModel


def _create_mock_table(pk_name: str = "id") -> MagicMock:
    """Create a mock DynamoDB table with the specified key schema."""
    mock_table = MagicMock()
    mock_table.key_schema = [{"AttributeName": pk_name, "KeyType": "HASH"}]
    return mock_table


class TestAttrPrivateAttributeAccess:
    """Test that private attribute access raises AttributeError."""

    def test_private_attribute_raises_error(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(AttributeError) as exc_info:
            _ = User.attr("_private")

        assert "private" in str(exc_info.value)
        assert "_private" in str(exc_info.value)

    def test_dunder_attribute_raises_error(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(AttributeError) as exc_info:
            _ = User.attr("__something__")

        assert "private" in str(exc_info.value)

    def test_private_root_in_nested_path_raises_error(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(AttributeError) as exc_info:
            _ = User.attr("_private.nested")

        assert "private" in str(exc_info.value)


class TestAttrNonExistentField:
    """Test that non-existent field access raises AttributeError."""

    def test_nonexistent_field_raises_error(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(AttributeError) as exc_info:
            _ = User.attr("nonexistent_field")

        # Error should mention model name and field name
        assert "User" in str(exc_info.value)
        assert "nonexistent_field" in str(exc_info.value)

    def test_nonexistent_root_in_nested_path_raises_error(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(AttributeError) as exc_info:
            _ = User.attr("nonexistent.nested")

        assert "User" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)


class TestAttrAliasResolution:
    """Test that field aliases are used in ExpressionField."""

    def test_field_with_alias_uses_alias(self) -> None:
        mock_table = _create_mock_table(pk_name="pk")

        class Item(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            pk: str = Field(alias="PK")
            sort_key: str = Field(alias="SK")
            display_name: str = Field(alias="displayName")

        # Access via Python field name, but ExpressionField should use alias
        pk_field = Item.attr("pk")
        sk_field = Item.attr("sort_key")
        name_field = Item.attr("display_name")

        assert isinstance(pk_field, ExpressionField)
        assert isinstance(sk_field, ExpressionField)
        assert isinstance(name_field, ExpressionField)

        # The field path should be the alias, not the Python attribute name
        assert pk_field.field == "PK"
        assert sk_field.field == "SK"
        assert name_field.field == "displayName"

    def test_field_without_alias_uses_field_name(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            username: str

        username_field = User.attr("username")

        assert isinstance(username_field, ExpressionField)
        assert username_field.field == "username"


class TestAttrNestedPath:
    """Test dot-separated nested attribute path access."""

    def test_simple_nested_path(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: dict  # type: ignore[type-arg]

        field = User.attr("address.city")

        assert isinstance(field, ExpressionField)
        assert field.field == "address.city"

    def test_deeply_nested_path(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: dict  # type: ignore[type-arg]

        field = User.attr("address.city.zip")

        assert field.field == "address.city.zip"

    def test_list_index_in_path(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            tags: list  # type: ignore[type-arg]

        field = User.attr("tags[0]")

        assert field.field == "tags[0]"

    def test_list_index_mid_path(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: dict  # type: ignore[type-arg]

        field = User.attr("address.city[0].id")

        assert field.field == "address.city[0].id"

    def test_consecutive_list_indices(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            matrix: list  # type: ignore[type-arg]

        field = User.attr("matrix[0][1]")

        assert field.field == "matrix[0][1]"

    def test_alias_applies_only_to_root(self) -> None:
        mock_table = _create_mock_table(pk_name="pk")

        class Item(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            pk: str = Field(alias="PK")
            data: dict = Field(alias="Data")  # type: ignore[type-arg]

        field = Item.attr("data.nested[0].key")

        # Root uses alias, nested segments are literal
        assert field.field == "Data.nested[0].key"
