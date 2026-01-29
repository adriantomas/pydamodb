"""Tests for PydamoDB exceptions."""

import pytest

from pydamodb.exceptions import (
    EmptyUpdateError,
    InsufficientConditionsError,
    InvalidKeySchemaError,
    MissingSortKeyValueError,
    PydamoError,
    UnknownConditionTypeError,
)


class TestExceptionHierarchy:
    """Test that exception hierarchy is correctly structured."""

    def test_validation_errors_inherit_from_pydamo_error(self) -> None:
        assert issubclass(MissingSortKeyValueError, PydamoError)
        assert issubclass(InvalidKeySchemaError, PydamoError)
        assert issubclass(InsufficientConditionsError, PydamoError)
        assert issubclass(UnknownConditionTypeError, PydamoError)
        assert issubclass(EmptyUpdateError, PydamoError)

    def test_can_catch_all_with_pydamo_error(self) -> None:
        """Test that all exceptions can be caught with PydamoError."""
        exceptions_to_test = [
            MissingSortKeyValueError(),
            InvalidKeySchemaError(),
            InsufficientConditionsError(operator="And", count=1),
            UnknownConditionTypeError(str),
            EmptyUpdateError(),
        ]

        for exc in exceptions_to_test:
            with pytest.raises(PydamoError):
                raise exc


class TestOperationErrors:
    """Test operation error messages and attributes."""

    def test_missing_sort_key_value_error(self) -> None:
        exc = MissingSortKeyValueError(operation="get", model_name="MyModel")
        assert "Sort key value must be provided" in str(exc)
        assert "get" in str(exc)
        assert "MyModel" in str(exc)


class TestValidationErrors:
    """Test validation error messages and attributes."""

    def test_invalid_key_schema_error(self) -> None:
        exc = InvalidKeySchemaError()
        assert "no partition key found" in str(exc)

    def test_invalid_key_schema_error_custom_message(self) -> None:
        exc = InvalidKeySchemaError("Custom message")
        assert "Custom message" in str(exc)

    def test_insufficient_conditions_error(self) -> None:
        exc = InsufficientConditionsError(operator="And", count=1)
        assert "And" in str(exc)
        assert "2" in str(exc)
        assert "1" in str(exc)
        assert exc.operator == "And"
        assert exc.count == 1

    def test_insufficient_conditions_error_or(self) -> None:
        exc = InsufficientConditionsError(operator="Or", count=0)
        assert "Or" in str(exc)
        assert exc.operator == "Or"
        assert exc.count == 0

    def test_unknown_condition_type_error(self) -> None:
        exc = UnknownConditionTypeError(str)
        assert "str" in str(exc)
        assert exc.condition_type is str

    def test_empty_update_error(self) -> None:
        exc = EmptyUpdateError()
        assert "No updates provided" in str(exc)
