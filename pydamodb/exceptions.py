"""PydamoDB exceptions.

This module defines the exception hierarchy for the PydamoDB library.
Exceptions are raised during DynamoDB operations (e.g., condition check
failures, throughput exceeded).

Pydantic validation errors are intentionally not wrapped and will bubble up
as `pydantic.ValidationError` since they are well-documented and users
likely already handle them.
"""

from botocore.exceptions import ClientError


class PydamoError(Exception):
    """Base exception for all PydamoDB errors.

    All custom exceptions in this library inherit from this class,
    allowing users to catch all PydamoDB-specific errors with a single
    except clause if desired.

    Example:
        try:
            model.save()
        except PydamoError as e:
            pass
    """


# =============================================================================
# Operation Errors
# =============================================================================


class OperationError(PydamoError):
    """Base exception for DynamoDB operation errors.

    Raised when an error occurs during a DynamoDB operation such as
    save, update, delete, or query.
    """

    def __init__(
        self,
        message: str,
        *,
        operation: str | None = None,
        model_name: str | None = None,
    ) -> None:
        self.operation = operation
        self.model_name = model_name

        parts = []
        if model_name:
            parts.append(model_name)
        if operation:
            parts.append(operation)

        if parts:
            message = f"[{'.'.join(parts)}] {message}"

        super().__init__(message)


class ConditionCheckFailedError(OperationError):
    """Raised when a conditional expression fails.

    This occurs when a condition passed to save(), update(), or delete()
    is not satisfied. Common scenarios include:
    - Saving with attribute_not_exists() when item already exists
    - Updating with a condition that doesn't match the current item state

    Example:
        try:
            model.save(condition=Model.attr.id.not_exists())
        except ConditionCheckFailedError:
            pass

    Attributes:
        condition: The condition expression that failed (if available)
    """

    def __init__(
        self,
        message: str = "Condition check failed",
        *,
        operation: str | None = None,
        model_name: str | None = None,
        condition: str | None = None,
        original_error: ClientError | None = None,
    ) -> None:
        self.condition = condition
        self.original_error = original_error
        super().__init__(message, operation=operation, model_name=model_name)


class MissingSortKeyValueError(OperationError):
    """Raised when a sort key value is required but not provided.

    For models with a composite key (partition + sort), the sort key
    value must be provided for operations that require a complete key.

    Example:
        For a model with both partition and sort key:

        PKSKModel.get_item("pk_value")
        Raises MissingSortKeyValueError.

        PKSKModel.get_item("pk_value", "sk_value")
    """

    def __init__(
        self,
        *,
        operation: str | None = None,
        model_name: str | None = None,
    ) -> None:
        super().__init__(
            "Sort key value must be provided for models with a sort key",
            operation=operation,
            model_name=model_name,
        )


class IndexNotFoundError(OperationError):
    """Raised when a specified index does not exist on the table.

    Ensure the index name matches a GSI or LSI defined on the table.

    Example:
        If "nonexistent-index" is not defined on the table:
        Order.query("pk_value", index_name="nonexistent-index")
        Raises IndexNotFoundError.

    Attributes:
        index_name: Name of the index that was not found.
    """

    def __init__(
        self,
        *,
        index_name: str,
        model_name: str | None = None,
    ) -> None:
        self.index_name = index_name
        super().__init__(
            f"Index '{index_name}' not found on table",
            operation="query",
            model_name=model_name,
        )


# =============================================================================
# Validation Errors
# =============================================================================


class ValidationError(PydamoError):
    """Base exception for validation errors.

    Raised when input validation fails, such as invalid schemas,
    missing required values, or malformed conditions.
    """


class InvalidKeySchemaError(ValidationError):
    """Raised when a DynamoDB key schema is invalid.

    This typically occurs when the table's key schema doesn't contain
    a partition key (HASH key).

    Example:
        If table schema is missing a partition key:
        class MyModel(PrimaryKeyModel):
            pydamo_config = {"table": invalid_table}
        Raises InvalidKeySchemaError when the schema is parsed.
    """

    def __init__(self, message: str = "Invalid key schema: no partition key found") -> None:
        super().__init__(message)


class InsufficientConditionsError(ValidationError):
    """Raised when a logical condition has insufficient operands.

    The And and Or conditions require at least 2 conditions to combine.

    Example:
        And(single_condition)
        Or(single_condition)

        These raise InsufficientConditionsError.

    Attributes:
        operator: The logical operator (And/Or) that failed.
        count: The number of conditions provided.
    """

    def __init__(
        self,
        *,
        operator: str,
        count: int,
    ) -> None:
        self.operator = operator
        self.count = count
        super().__init__(f"{operator} requires at least 2 conditions, got {count}")


class UnknownConditionTypeError(ValidationError):
    """Raised when an unknown condition type is encountered.

    This occurs when building a condition expression with an unsupported
    condition class.

    Attributes:
        condition_type: The type of the unknown condition.
    """

    def __init__(self, condition_type: type) -> None:
        self.condition_type = condition_type
        super().__init__(f"Unknown condition type: {condition_type}")


class EmptyUpdateError(ValidationError):
    """Raised when an update operation has no fields to update.

    At least one field must be specified for an update operation.

    Example:
        model.update({})
    """

    def __init__(self) -> None:
        super().__init__("No updates provided")


class TableNotFoundError(OperationError):
    """Raised when the DynamoDB table does not exist.

    Ensure the table is created before performing operations, or check
    that pydamo_config is correctly configured with the table reference.

    Attributes:
        table_name: Name of the table that was not found (if available)
    """

    def __init__(
        self,
        message: str = "DynamoDB table not found",
        *,
        table_name: str | None = None,
        model_name: str | None = None,
        original_error: ClientError | None = None,
    ) -> None:
        self.table_name = table_name
        self.original_error = original_error
        if table_name:
            message = f"{message}: {table_name}"
        super().__init__(message, model_name=model_name)


class ThroughputExceededError(OperationError):
    """Raised when provisioned throughput is exceeded.

    This can happen when:
    - Read/write capacity is insufficient for the request rate
    - Burst capacity is exhausted

    Consider implementing exponential backoff and retry logic,
    or switching to on-demand capacity mode.

    Attributes:
        original_error: The underlying boto3 ClientError
    """

    def __init__(
        self,
        message: str = "Provisioned throughput exceeded",
        *,
        operation: str | None = None,
        model_name: str | None = None,
        original_error: ClientError | None = None,
    ) -> None:
        self.original_error = original_error
        super().__init__(message, operation=operation, model_name=model_name)


class DynamoDBClientError(OperationError):
    """Raised for unhandled DynamoDB client errors.

    This is a fallback exception for boto3 ClientErrors that don't
    map to a more specific PydamoDB exception.

    Attributes:
        error_code: The DynamoDB error code (e.g., 'ValidationException')
        original_error: The underlying boto3 ClientError
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        operation: str | None = None,
        model_name: str | None = None,
        original_error: ClientError | None = None,
    ) -> None:
        self.error_code = error_code
        self.original_error = original_error
        if error_code:
            message = f"[{error_code}] {message}"
        super().__init__(message, operation=operation, model_name=model_name)


# =============================================================================
# Helper Functions
# =============================================================================


def wrap_client_error(
    error: ClientError,
    *,
    operation: str | None = None,
    model_name: str | None = None,
) -> OperationError:
    """Convert a boto3 ClientError to an appropriate PydamoDB exception.

    This function examines the error code and returns a specific PydamoDB
    exception type when possible, falling back to DynamoDBClientError
    for unrecognized errors.

    Args:
        error: The boto3 ClientError to wrap
        operation: The operation being performed (e.g., 'save', 'update')
        model_name: The name of the model class

    Returns:
        An appropriate OperationError subclass

    Example:
        try:
            table.put_item(...)
        except ClientError as e:
            raise wrap_client_error(e, operation='save', model_name='MyModel')
    """
    error_code = error.response.get("Error", {}).get("Code", "")
    error_message = error.response.get("Error", {}).get("Message", str(error))

    if error_code == "ConditionalCheckFailedException":
        return ConditionCheckFailedError(
            error_message,
            operation=operation,
            model_name=model_name,
            original_error=error,
        )

    if error_code == "ResourceNotFoundException":
        return TableNotFoundError(
            error_message,
            model_name=model_name,
            original_error=error,
        )

    if error_code in (
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "ThrottlingException",
    ):
        return ThroughputExceededError(
            error_message,
            operation=operation,
            model_name=model_name,
            original_error=error,
        )

    return DynamoDBClientError(
        error_message,
        error_code=error_code,
        operation=operation,
        model_name=model_name,
        original_error=error,
    )


__all__ = [
    # Base exceptions
    "PydamoError",
    # Operation errors
    "OperationError",
    "ConditionCheckFailedError",
    "MissingSortKeyValueError",
    "IndexNotFoundError",
    "TableNotFoundError",
    "ThroughputExceededError",
    "DynamoDBClientError",
    # Validation errors
    "ValidationError",
    "InvalidKeySchemaError",
    "InsufficientConditionsError",
    "UnknownConditionTypeError",
    "EmptyUpdateError",
    # Helper functions
    "wrap_client_error",
]
