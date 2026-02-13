"""Tests for AttributePath and AttrDescriptor."""

from unittest.mock import MagicMock

import pytest
from pydantic import Field

from pydamodb.base import PydamoConfig
from pydamodb.expressions import ExpressionField
from pydamodb.fields import AttributePath
from pydamodb.sync_models import PrimaryKeyModel


def _create_mock_table(pk_name: str = "id") -> MagicMock:
    """Create a mock DynamoDB table with the specified key schema."""
    mock_table = MagicMock()
    mock_table.key_schema = [{"AttributeName": pk_name, "KeyType": "HASH"}]
    return mock_table


class TestAttributePathPrivateAttributeAccess:
    """Test that private attribute access raises AttributeError."""

    def test_private_attribute_raises_error(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(AttributeError) as exc_info:
            _ = User.attr._private

        assert "private" in str(exc_info.value)
        assert "_private" in str(exc_info.value)

    def test_dunder_attribute_raises_error(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(AttributeError) as exc_info:
            _ = User.attr.__something__

        assert "private" in str(exc_info.value)


class TestAttributePathNonExistentField:
    """Test that non-existent field access raises AttributeError."""

    def test_nonexistent_field_raises_error(self) -> None:
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            name: str

        with pytest.raises(AttributeError) as exc_info:
            _ = User.attr.nonexistent_field  # type: ignore[misc]

        # Error should mention model name and field name
        assert "User" in str(exc_info.value)
        assert "nonexistent_field" in str(exc_info.value)


class TestAttributePathAliasResolution:
    """Test that field aliases are used in ExpressionField."""

    def test_field_with_alias_uses_alias(self) -> None:
        mock_table = _create_mock_table(pk_name="pk")

        class Item(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            pk: str = Field(alias="PK")
            sort_key: str = Field(alias="SK")
            display_name: str = Field(alias="displayName")

        # Access via Python field name, but ExpressionField should use alias
        pk_field = Item.attr.pk
        sk_field = Item.attr.sort_key
        name_field = Item.attr.display_name

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

        username_field = User.attr.username

        assert isinstance(username_field, ExpressionField)
        assert username_field.field == "username"


class TestAttributePathRepr:
    """Test AttributePath string representation."""

    def test_repr_includes_model_name(self) -> None:
        mock_table = _create_mock_table()

        class MyModel(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str

        attr_path = AttributePath(MyModel)

        assert "MyModel" in repr(attr_path)
        assert "AttributePath" in repr(attr_path)
