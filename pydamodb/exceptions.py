"""PydamoDB exceptions.

This module defines the exception hierarchy for the PydamoDB library.
All custom exceptions inherit from PydamoError, allowing users to catch
all library-specific errors with a single except clause.

Exception categories:
- PydamoError: Base exception for all PydamoDB errors
- MissingSortKeyValueError: Sort key required but not provided
- InvalidKeySchemaError: Table key schema is invalid
- IndexNotFoundError: Specified index doesn't exist
- InsufficientConditionsError: Logical condition needs more operands
- UnknownConditionTypeError: Unsupported condition type
- EmptyUpdateError: Update operation has no fields

Note: Pydantic validation errors are intentionally not wrapped and will bubble up
as pydantic.ValidationError since they are well-documented and users likely
already handle them. DynamoDB API errors (e.g., ConditionalCheckFailedException,
ProvisionedThroughputExceededException) are also not wrapped and come directly
from boto3/botocore.
"""


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


class MissingSortKeyValueError(PydamoError):
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
        message = "Sort key value must be provided for models with a sort key"
        if model_name and operation:
            message = (
                f"Sort key value must be provided for {model_name} in {operation} operation"
            )
        elif model_name:
            message = f"Sort key value must be provided for {model_name}"
        elif operation:
            message = f"Sort key value must be provided in {operation} operation"
        super().__init__(message)


class InvalidKeySchemaError(PydamoError):
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


class IndexNotFoundError(PydamoError):
    """Raised when a specified index does not exist on the table.

    This error occurs when querying with an index_name that doesn't match any
    Global Secondary Index (GSI) or Local Secondary Index (LSI) on the table.
    Ensure the index name matches exactly and the index exists in your DynamoDB table.

    Attributes:
        index_name: Name of the index that was not found.

    Example:
        Order.query("pk_value", index_name="nonexistent-index")
        Raises IndexNotFoundError: Index 'nonexistent-index' not found on table

    """

    def __init__(
        self,
        *,
        index_name: str,
    ) -> None:
        self.index_name = index_name
        super().__init__(
            f"Index '{index_name}' not found on table",
        )


class InsufficientConditionsError(PydamoError):
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


class UnknownConditionTypeError(PydamoError):
    """Raised when an unknown condition type is encountered.

    This occurs when building a condition expression with an unsupported
    condition class.

    Attributes:
        condition_type: The type of the unknown condition.

    """

    def __init__(self, condition_type: type) -> None:
        self.condition_type = condition_type
        super().__init__(f"Unknown condition type: {condition_type}")


class EmptyUpdateError(PydamoError):
    """Raised when an update operation has no fields to update.

    At least one field must be specified for an update operation.

    Example:
        model.update({})

    """

    def __init__(self) -> None:
        super().__init__("No updates provided")


__all__ = [
    "EmptyUpdateError",
    "IndexNotFoundError",
    "InsufficientConditionsError",
    "InvalidKeySchemaError",
    "MissingSortKeyValueError",
    "PydamoError",
    "UnknownConditionTypeError",
]
