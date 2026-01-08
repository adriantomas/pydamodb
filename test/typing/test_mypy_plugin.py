"""Test file for mypy plugin type checking.

Run with: mypy test/typing/test_mypy_plugin.py

Expected behavior:
- reveal_type calls should show proper types
- Invalid field access should error
- Type mismatches in comparisons should error
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import reveal_type

from pydamodb.models import PrimaryKeyAndSortKeyModel, PrimaryKeyModel, PydamoConfig

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table

# Mock table for testing - typed for mypy
table: Table = None  # type: ignore[assignment]


class User(PrimaryKeyModel):
    """Test model with partition key only."""

    pydamo_config = PydamoConfig(table=table)

    id: str
    name: str
    age: int
    email: str | None = None


class Order(PrimaryKeyAndSortKeyModel):
    """Test model with partition key and sort key."""

    pydamo_config = PydamoConfig(table=table)

    user_id: str
    order_id: str
    total: float
    items: list[str]


# ============================================================================
# Test: reveal_type for attr accessor
# ============================================================================

# These should show AttributePath[User] and AttributePath[Order]
reveal_type(User.attr)  # Expected: AttributePath[User]
reveal_type(Order.attr)  # Expected: AttributePath[Order]


# ============================================================================
# Test: reveal_type for field access
# ============================================================================

# These should show ExpressionField[<field_type>]
reveal_type(User.attr.id)  # Expected: ExpressionField[str]
reveal_type(User.attr.name)  # Expected: ExpressionField[str]
reveal_type(User.attr.age)  # Expected: ExpressionField[int]
reveal_type(User.attr.email)  # Expected: ExpressionField[str | None]

reveal_type(Order.attr.user_id)  # Expected: ExpressionField[str]
reveal_type(Order.attr.total)  # Expected: ExpressionField[float]
reveal_type(Order.attr.items)  # Expected: ExpressionField[list[str]]


# ============================================================================
# Test: Comparison operations should type-check
# ============================================================================

# Valid comparisons - these should pass
_valid1 = User.attr.name == "Alice"
_valid2 = User.attr.age > 18
_valid3 = User.attr.age >= 21
_valid4 = User.attr.age < 100
_valid5 = Order.attr.total <= 99.99

# Using ExpressionField methods
_valid6 = User.attr.name.begins_with("A")
_valid7 = User.attr.age.between(18, 65)

# ============================================================================
# Test: Type mismatches in comparisons
# ============================================================================

# __eq__ accepts object, so no error (standard Python behavior)
_compare1 = User.attr.age == "18"
