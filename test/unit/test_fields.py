"""Tests for the attr() classmethod."""

from typing import Any as AnyType
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

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


class TestAttrNestedPydanticModel:
    """Validate nested-path resolution through Pydantic BaseModel fields."""

    def test_valid_nested_pydantic_field(self) -> None:
        mock_table = _create_mock_table()

        class Address(BaseModel):
            city: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: Address

        field = User.attr("address.city")
        assert field.field == "address.city"

    def test_invalid_nested_pydantic_field_raises(self) -> None:
        mock_table = _create_mock_table()

        class Address(BaseModel):
            city: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: Address

        with pytest.raises(AttributeError) as exc_info:
            User.attr("address.nonexistent")
        assert "Address" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

    def test_optional_nested_pydantic_field(self) -> None:
        """typing.Optional[X] unwraps to X in _resolve_annotation."""
        mock_table = _create_mock_table()

        class Address(BaseModel):
            city: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: Address | None = None

        field = User.attr("address.city")
        assert field.field == "address.city"

    def test_native_union_optional_nested_pydantic_field(self) -> None:
        """Python 3.10+ X | None union unwraps to X in _resolve_annotation."""
        mock_table = _create_mock_table()

        class Address(BaseModel):
            city: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: Address | None = None

        field = User.attr("address.city")
        assert field.field == "address.city"

    def test_list_of_pydantic_model_field(self) -> None:
        """list[X] unwraps to X so nested fields are validated."""
        mock_table = _create_mock_table()

        class Contact(BaseModel):
            email: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            contacts: list[Contact]

        field = User.attr("contacts[0].email")
        assert field.field == "contacts[0].email"

    def test_invalid_nested_field_in_list_model_raises(self) -> None:
        mock_table = _create_mock_table()

        class Contact(BaseModel):
            email: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            contacts: list[Contact]

        with pytest.raises(AttributeError) as exc_info:
            User.attr("contacts[0].nonexistent")
        assert "Contact" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

    def test_multi_union_field_stops_validation_silently(self) -> None:
        """Union[X, Y] with two non-None types returns None → validation stops."""
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            data: str | int

        field = User.attr("data.anything")
        assert field.field == "data.anything"

    def test_any_typed_field_stops_validation_silently(self) -> None:
        """Any annotation is not a plain type → _resolve_annotation returns None."""
        mock_table = _create_mock_table()

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            data: AnyType

        field = User.attr("data.anything.nested")
        assert field.field == "data.anything.nested"

    def test_primitive_nested_type_stops_validation_silently(self) -> None:
        """After traversing into a str field, validation stops without error."""
        mock_table = _create_mock_table()

        class Address(BaseModel):
            city: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: Address

        # city is str — _resolve_annotation(str) returns None, loop stops
        field = User.attr("address.city.anything")
        assert field.field == "address.city.anything"


class TestAttrNestedTypedDict:
    """Validate nested-path resolution through TypedDict fields."""

    def test_valid_nested_typeddict_field(self) -> None:
        mock_table = _create_mock_table()

        class AddressDict(TypedDict):
            city: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: AddressDict

        field = User.attr("address.city")
        assert field.field == "address.city"

    def test_invalid_nested_typeddict_field_raises(self) -> None:
        mock_table = _create_mock_table()

        class AddressDict(TypedDict):
            city: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: AddressDict

        with pytest.raises(AttributeError) as exc_info:
            User.attr("address.nonexistent")
        assert "AddressDict" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

    def test_typeddict_unresolvable_hints_stops_validation_silently(self) -> None:
        """TypeError from get_type_hints is caught and validation stops."""
        mock_table = _create_mock_table()

        class AddressDict(TypedDict):
            city: str

        class User(PrimaryKeyModel):
            pydamo_config = PydamoConfig(table=mock_table)
            id: str
            address: AddressDict

        with patch("pydamodb.base.get_type_hints", side_effect=TypeError):
            field = User.attr("address.city")

        assert field.field == "address.city"


class TestExpressionFieldMethods:
    """Unit tests for ExpressionField helper methods."""

    def test_str(self) -> None:
        f = ExpressionField("name")
        assert str(f) == "name"

    def test_repr(self) -> None:
        f = ExpressionField("age")
        assert repr(f) == "ExpressionField('age')"

    def test_exists(self) -> None:
        from pydamodb.conditions import AttributeExists

        f = ExpressionField("optional_field")
        cond = f.exists()
        assert isinstance(cond, AttributeExists)
        assert cond.field == "optional_field"

    def test_not_exists(self) -> None:
        from pydamodb.conditions import AttributeNotExists

        f = ExpressionField("optional_field")
        cond = f.not_exists()
        assert isinstance(cond, AttributeNotExists)
        assert cond.field == "optional_field"

    def test_in_(self) -> None:
        from pydamodb.conditions import In

        f = ExpressionField("status")
        cond = f.in_("active", "pending")
        assert isinstance(cond, In)
        assert cond.field == "status"
        assert cond.values == ["active", "pending"]

    def test_size(self) -> None:
        from pydamodb.conditions import Size

        f = ExpressionField("tags")
        size_wrapper = f.size()
        assert isinstance(size_wrapper, Size)
        assert size_wrapper.field == "tags"
