"""Tests for DynamoDB condition expressions."""

import pytest

from pydamodb.conditions import (
    And,
    AttributeExists,
    Between,
    Contains,
    Eq,
    Gt,
    In,
    Lt,
    Ne,
    Not,
    Or,
    Size,
    SizeEq,
    SizeGt,
    SizeLte,
    SizeNe,
)
from pydamodb.exceptions import InsufficientConditionsError


class TestAndOrRequireMinimumConditions:
    """Test that And/Or raise when given fewer than 2 conditions."""

    def test_and_with_zero_conditions_raises(self) -> None:
        with pytest.raises(InsufficientConditionsError) as exc_info:
            And()

        assert exc_info.value.operator == "And"
        assert exc_info.value.count == 0

    def test_and_with_one_condition_raises(self) -> None:
        condition = Eq("age", 30)

        with pytest.raises(InsufficientConditionsError) as exc_info:
            And(condition)

        assert exc_info.value.operator == "And"
        assert exc_info.value.count == 1

    def test_and_with_two_conditions_succeeds(self) -> None:
        cond1 = Eq("age", 30)
        cond2 = Eq("status", "active")

        result = And(cond1, cond2)

        assert result.conditions == (cond1, cond2)

    def test_or_with_zero_conditions_raises(self) -> None:
        with pytest.raises(InsufficientConditionsError) as exc_info:
            Or()

        assert exc_info.value.operator == "Or"
        assert exc_info.value.count == 0

    def test_or_with_one_condition_raises(self) -> None:
        condition = Eq("role", "admin")

        with pytest.raises(InsufficientConditionsError) as exc_info:
            Or(condition)

        assert exc_info.value.operator == "Or"
        assert exc_info.value.count == 1

    def test_or_with_two_conditions_succeeds(self) -> None:
        cond1 = Eq("role", "admin")
        cond2 = Eq("role", "moderator")

        result = Or(cond1, cond2)

        assert result.conditions == (cond1, cond2)


class TestAndOrFlatteningBehavior:
    """Test that And/Or flatten nested conditions of the same type."""

    def test_and_flattens_nested_and(self) -> None:
        a = Eq("a", 1)
        b = Eq("b", 2)
        c = Eq("c", 3)
        d = Eq("d", 4)

        # And(a, b) & And(c, d) should flatten to And(a, b, c, d)
        left = And(a, b)
        right = And(c, d)
        result = left & right

        assert isinstance(result, And)
        assert result.conditions == (a, b, c, d)

    def test_and_appends_single_condition(self) -> None:
        a = Eq("a", 1)
        b = Eq("b", 2)
        c = Eq("c", 3)

        # And(a, b) & c should produce And(a, b, c)
        left = And(a, b)
        result = left & c

        assert isinstance(result, And)
        assert result.conditions == (a, b, c)

    def test_or_flattens_nested_or(self) -> None:
        a = Eq("status", "active")
        b = Eq("status", "pending")
        c = Eq("status", "trial")
        d = Eq("status", "guest")

        # Or(a, b) | Or(c, d) should flatten to Or(a, b, c, d)
        left = Or(a, b)
        right = Or(c, d)
        result = left | right

        assert isinstance(result, Or)
        assert result.conditions == (a, b, c, d)

    def test_or_appends_single_condition(self) -> None:
        a = Eq("status", "active")
        b = Eq("status", "pending")
        c = Eq("status", "trial")

        # Or(a, b) | c should produce Or(a, b, c)
        left = Or(a, b)
        result = left | c

        assert isinstance(result, Or)
        assert result.conditions == (a, b, c)

    def test_and_does_not_flatten_or(self) -> None:
        a = Eq("a", 1)
        b = Eq("b", 2)
        c = Eq("c", 3)
        d = Eq("d", 4)

        # And(a, b) & Or(c, d) should NOT flatten
        left = And(a, b)
        right = Or(c, d)
        result = left & right

        assert isinstance(result, And)
        # Or should remain as a single condition, not flattened
        assert len(result.conditions) == 3
        assert result.conditions == (a, b, right)

    def test_or_does_not_flatten_and(self) -> None:
        a = Eq("a", 1)
        b = Eq("b", 2)
        c = Eq("c", 3)
        d = Eq("d", 4)

        # Or(a, b) | And(c, d) should NOT flatten
        left = Or(a, b)
        right = And(c, d)
        result = left | right

        assert isinstance(result, Or)
        # And should remain as a single condition, not flattened
        assert len(result.conditions) == 3
        assert result.conditions == (a, b, right)


class TestSizeLessThanOrEqual:
    """Test the Size <= operator creates SizeLte condition."""

    def test_size_less_than_or_equal(self) -> None:
        size = Size("tags")
        result = size <= 5

        assert isinstance(result, SizeLte)
        assert result.field == "tags"
        assert result.value == 5
        assert result.operator == "<="

    def test_size_less_than_or_equal_boundary(self) -> None:
        size = Size("items")
        result = size <= 0

        assert isinstance(result, SizeLte)
        assert result.field == "items"
        assert result.value == 0


class TestConditionEquality:
    """Test __eq__ methods on condition classes."""

    def test_eq_conditions_equal(self) -> None:
        cond1 = Eq("age", 30)
        cond2 = Eq("age", 30)

        assert cond1 == cond2

    def test_eq_conditions_different_values(self) -> None:
        cond1 = Eq("age", 30)
        cond2 = Eq("age", 31)

        assert cond1 != cond2

    def test_eq_conditions_different_fields(self) -> None:
        cond1 = Eq("age", 30)
        cond2 = Eq("count", 30)

        assert cond1 != cond2

    def test_different_comparison_operators_not_equal(self) -> None:
        eq = Eq("age", 30)
        ne = Ne("age", 30)
        lt = Lt("age", 30)
        gt = Gt("age", 30)

        assert eq != ne
        assert eq != lt
        assert eq != gt

    def test_between_conditions_equal(self) -> None:
        cond1 = Between("age", 20, 30)
        cond2 = Between("age", 20, 30)

        assert cond1 == cond2

    def test_between_conditions_different_bounds(self) -> None:
        cond1 = Between("age", 20, 30)
        cond2 = Between("age", 20, 40)

        assert cond1 != cond2

    def test_contains_conditions_equal(self) -> None:
        cond1 = Contains("tags", "python")
        cond2 = Contains("tags", "python")

        assert cond1 == cond2

    def test_in_conditions_equal(self) -> None:
        cond1 = In("status", ["active", "pending"])
        cond2 = In("status", ["active", "pending"])

        assert cond1 == cond2

    def test_in_conditions_different_values(self) -> None:
        cond1 = In("status", ["active", "pending"])
        cond2 = In("status", ["active", "inactive"])

        assert cond1 != cond2

    def test_size_conditions_equal(self) -> None:
        cond1 = SizeEq("tags", 5)
        cond2 = SizeEq("tags", 5)

        assert cond1 == cond2

    def test_size_conditions_different_operators_not_equal(self) -> None:
        eq = SizeEq("tags", 5)
        ne = SizeNe("tags", 5)
        gt = SizeGt("tags", 5)

        assert eq != ne
        assert eq != gt

    def test_attribute_exists_equal(self) -> None:
        cond1 = AttributeExists("email")
        cond2 = AttributeExists("email")

        assert cond1 == cond2

    def test_and_conditions_equal(self) -> None:
        a = Eq("a", 1)
        b = Eq("b", 2)
        cond1 = And(a, b)
        cond2 = And(a, b)

        assert cond1 == cond2

    def test_and_conditions_different_order(self) -> None:
        a = Eq("a", 1)
        b = Eq("b", 2)
        cond1 = And(a, b)
        cond2 = And(b, a)

        # Order matters in And conditions
        assert cond1 != cond2

    def test_not_conditions_equal(self) -> None:
        inner = Eq("deleted", value=True)
        cond1 = Not(inner)
        cond2 = Not(inner)

        assert cond1 == cond2

    def test_condition_not_equal_to_non_condition(self) -> None:
        cond = Eq("age", 30)

        assert cond.__eq__("not a condition") is NotImplemented
        assert cond.__eq__(30) is NotImplemented
        assert cond.__eq__(None) is NotImplemented
