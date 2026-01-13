"""Tests for PydamoDB exceptions."""

import pytest

from pydamodb.exceptions import (
    ConditionCheckFailedError,
    DynamoDBClientError,
    EmptyUpdateError,
    InsufficientConditionsError,
    InvalidKeySchemaError,
    MissingSortKeyValueError,
    OperationError,
    PydamoError,
    TableNotFoundError,
    ThroughputExceededError,
    UnknownConditionTypeError,
    ValidationError,
    wrap_client_error,
)


class TestExceptionHierarchy:
    """Test that exception hierarchy is correctly structured."""

    def test_operation_errors_inherit_from_pydamo_error(self) -> None:
        assert issubclass(OperationError, PydamoError)
        assert issubclass(ConditionCheckFailedError, OperationError)
        assert issubclass(MissingSortKeyValueError, OperationError)
        assert issubclass(TableNotFoundError, OperationError)
        assert issubclass(ThroughputExceededError, OperationError)
        assert issubclass(DynamoDBClientError, OperationError)

    def test_validation_errors_inherit_from_pydamo_error(self) -> None:
        assert issubclass(ValidationError, PydamoError)
        assert issubclass(InvalidKeySchemaError, ValidationError)
        assert issubclass(InsufficientConditionsError, ValidationError)
        assert issubclass(UnknownConditionTypeError, ValidationError)
        assert issubclass(EmptyUpdateError, ValidationError)

    def test_can_catch_all_with_pydamo_error(self) -> None:
        """Test that all exceptions can be caught with PydamoError."""
        exceptions_to_test = [
            ConditionCheckFailedError(),
            MissingSortKeyValueError(),
            TableNotFoundError(),
            ThroughputExceededError(),
            DynamoDBClientError("test"),
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

    def test_condition_check_failed_error(self) -> None:
        exc = ConditionCheckFailedError(
            operation="save",
            model_name="MyModel",
            condition="attribute_not_exists(id)",
        )
        assert "save" in str(exc)
        assert "MyModel" in str(exc)
        assert exc.condition == "attribute_not_exists(id)"
        assert exc.operation == "save"
        assert exc.model_name == "MyModel"

    def test_missing_sort_key_value_error(self) -> None:
        exc = MissingSortKeyValueError(operation="get", model_name="MyModel")
        assert "Sort key value must be provided" in str(exc)
        assert "get" in str(exc)
        assert "MyModel" in str(exc)

    def test_table_not_found_error(self) -> None:
        exc = TableNotFoundError(table_name="my-table", model_name="MyModel")
        assert "my-table" in str(exc)
        assert exc.table_name == "my-table"

    def test_throughput_exceeded_error(self) -> None:
        exc = ThroughputExceededError(operation="query", model_name="MyModel")
        assert "throughput" in str(exc).lower()
        assert "query" in str(exc)

    def test_dynamodb_client_error(self) -> None:
        exc = DynamoDBClientError(
            "Something went wrong",
            error_code="ValidationException",
            operation="save",
            model_name="MyModel",
        )
        assert "ValidationException" in str(exc)
        assert "Something went wrong" in str(exc)
        assert exc.error_code == "ValidationException"


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


class TestWrapClientError:
    """Test the wrap_client_error helper function."""

    def test_wrap_conditional_check_failed(self) -> None:
        # Mock ClientError for ConditionalCheckFailedException
        mock_error = _create_mock_client_error(
            "ConditionalCheckFailedException",
            "The conditional request failed",
        )
        result = wrap_client_error(mock_error, operation="save", model_name="MyModel")  # type: ignore[arg-type]

        assert isinstance(result, ConditionCheckFailedError)
        assert result.original_error is mock_error  # type: ignore[comparison-overlap]
        assert result.operation == "save"
        assert result.model_name == "MyModel"

    def test_wrap_resource_not_found(self) -> None:
        mock_error = _create_mock_client_error(
            "ResourceNotFoundException",
            "Requested resource not found",
        )
        result = wrap_client_error(mock_error, model_name="MyModel")  # type: ignore[arg-type]

        assert isinstance(result, TableNotFoundError)
        assert result.original_error is mock_error  # type: ignore[comparison-overlap]

    def test_wrap_throughput_exceeded(self) -> None:
        mock_error = _create_mock_client_error(
            "ProvisionedThroughputExceededException",
            "Throughput exceeded",
        )
        result = wrap_client_error(mock_error, operation="query", model_name="MyModel")  # type: ignore[arg-type]

        assert isinstance(result, ThroughputExceededError)
        assert result.original_error is mock_error  # type: ignore[comparison-overlap]

    def test_wrap_throttling_exception(self) -> None:
        mock_error = _create_mock_client_error(
            "ThrottlingException",
            "Rate exceeded",
        )
        result = wrap_client_error(mock_error, operation="save", model_name="MyModel")  # type: ignore[arg-type]

        assert isinstance(result, ThroughputExceededError)

    def test_wrap_unknown_error(self) -> None:
        mock_error = _create_mock_client_error(
            "SomeUnknownError",
            "Something went wrong",
        )
        result = wrap_client_error(mock_error, operation="save", model_name="MyModel")  # type: ignore[arg-type]

        assert isinstance(result, DynamoDBClientError)
        assert result.error_code == "SomeUnknownError"
        assert result.original_error is mock_error  # type: ignore[comparison-overlap]


class _MockClientError(Exception):
    """Mock boto3 ClientError for testing."""

    def __init__(self, error_code: str, error_message: str) -> None:
        self.response = {
            "Error": {
                "Code": error_code,
                "Message": error_message,
            },
        }
        super().__init__(error_message)


def _create_mock_client_error(code: str, message: str) -> _MockClientError:
    """Create a mock ClientError for testing."""
    return _MockClientError(code, message)
