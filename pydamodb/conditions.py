"""DynamoDB condition expression primitives.

This module defines small, composable objects that represent DynamoDB
condition expressions. These are consumed by the expression builder to
produce a DynamoDB `ConditionExpression` string.
"""

from __future__ import annotations

from typing import ClassVar, Generic, Literal, TypeVar

from pydamodb.exceptions import InsufficientConditionsError

T = TypeVar("T")


class Condition:
    """Base class for all DynamoDB condition expressions."""

    def __and__(self, other: Condition) -> And:
        return And(self, other)

    def __or__(self, other: Condition) -> Or:
        return Or(self, other)

    def __invert__(self) -> Not:
        return Not(self)


class ComparisonCondition(Condition, Generic[T]):
    """Base class for comparison conditions with typed values.

    The type parameter T ensures that comparisons are made with
    values of the correct type for the field being compared.
    """

    operator: ClassVar[Literal["=", "<>", "<", "<=", ">", ">="]]

    def __init__(self, field: str, value: T) -> None:
        self.field = field
        self.value = value

    def __repr__(self) -> str:
        return f"{self.field} {self.operator} {self.value!r}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ComparisonCondition):
            return NotImplemented
        return (
            self.field == other.field
            and self.operator == other.operator
            and self.value == other.value
        )


class Eq(ComparisonCondition[T]):
    """Equality condition: field = value"""

    operator = "="


class Ne(ComparisonCondition[T]):
    """Not equal condition: field <> value"""

    operator = "<>"


class Lt(ComparisonCondition[T]):
    """Less than condition: field < value"""

    operator = "<"


class Lte(ComparisonCondition[T]):
    """Less than or equal condition: field <= value"""

    operator = "<="


class Gt(ComparisonCondition[T]):
    """Greater than condition: field > value"""

    operator = ">"


class Gte(ComparisonCondition[T]):
    """Greater than or equal condition: field >= value"""

    operator = ">="


class Between(Condition, Generic[T]):
    """Between condition: field BETWEEN low AND high.

    The type parameter T ensures that both bounds are of the same type.
    """

    def __init__(self, field: str, low: T, high: T) -> None:
        self.field = field
        self.low = low
        self.high = high

    def __repr__(self) -> str:
        return f"{self.field} BETWEEN {self.low!r} AND {self.high!r}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Between):
            return NotImplemented
        return self.field == other.field and self.low == other.low and self.high == other.high


class BeginsWith(Condition):
    """Begins with condition: begins_with(field, prefix).

    Only applicable to string fields.
    """

    def __init__(self, field: str, prefix: str) -> None:
        self.field = field
        self.prefix = prefix

    def __repr__(self) -> str:
        return f"begins_with({self.field}, {self.prefix!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BeginsWith):
            return NotImplemented
        return self.field == other.field and self.prefix == other.prefix


class Contains(Condition, Generic[T]):
    """Contains condition: contains(field, value).

    For strings: checks if the string contains the substring.
    For sets/lists: checks if the collection contains the element.
    """

    def __init__(self, field: str, value: T) -> None:
        self.field = field
        self.value = value

    def __repr__(self) -> str:
        return f"contains({self.field}, {self.value!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Contains):
            return NotImplemented
        return self.field == other.field and self.value == other.value


class In(Condition, Generic[T]):
    """IN condition: field IN (value1, value2, ...).

    Checks if the field value matches any value in the provided list.
    """

    def __init__(self, field: str, values: list[T]) -> None:
        self.field = field
        self.values = values

    def __repr__(self) -> str:
        values_repr = ", ".join(repr(v) for v in self.values)
        return f"{self.field} IN ({values_repr})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, In):
            return NotImplemented
        return self.field == other.field and self.values == other.values


class SizeCondition(Condition):
    """Base class for size() function conditions.

    Used internally to represent size(field) comparisons.
    """

    operator: ClassVar[Literal["=", "<>", "<", "<=", ">", ">="]]

    def __init__(self, field: str, value: int) -> None:
        self.field = field
        self.value = value

    def __repr__(self) -> str:
        return f"size({self.field}) {self.operator} {self.value}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SizeCondition):
            return NotImplemented
        return (
            self.field == other.field
            and self.operator == other.operator
            and self.value == other.value
        )


class SizeEq(SizeCondition):
    """Size equals condition: size(field) = value"""

    operator = "="


class SizeNe(SizeCondition):
    """Size not equals condition: size(field) <> value"""

    operator = "<>"


class SizeLt(SizeCondition):
    """Size less than condition: size(field) < value"""

    operator = "<"


class SizeLte(SizeCondition):
    """Size less than or equals condition: size(field) <= value"""

    operator = "<="


class SizeGt(SizeCondition):
    """Size greater than condition: size(field) > value"""

    operator = ">"


class SizeGte(SizeCondition):
    """Size greater than or equals condition: size(field) >= value"""

    operator = ">="


class Size:
    """Size function wrapper for building size(field) conditions.

    Use comparison operators on this class to create size conditions.

    Example:
        Size("tags") > 0
        This builds the expression: size(tags) > 0

        Size("name") >= 3
        This builds the expression: size(name) >= 3
    """

    def __init__(self, field: str) -> None:
        self.field = field

    def __eq__(self, other: int) -> SizeEq:  # type: ignore[override]
        return SizeEq(field=self.field, value=other)

    def __ne__(self, other: int) -> SizeNe:  # type: ignore[override]
        return SizeNe(field=self.field, value=other)

    def __lt__(self, other: int) -> SizeLt:
        return SizeLt(field=self.field, value=other)

    def __le__(self, other: int) -> SizeLte:
        return SizeLte(field=self.field, value=other)

    def __gt__(self, other: int) -> SizeGt:
        return SizeGt(field=self.field, value=other)

    def __ge__(self, other: int) -> SizeGte:
        return SizeGte(field=self.field, value=other)


class AttributeExists(Condition):
    """Attribute exists condition: attribute_exists(field)."""

    def __init__(self, field: str) -> None:
        self.field = field

    def __repr__(self) -> str:
        return f"attribute_exists({self.field})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AttributeExists):
            return NotImplemented
        return self.field == other.field


class AttributeNotExists(Condition):
    """Attribute not exists condition: attribute_not_exists(field)."""

    def __init__(self, field: str) -> None:
        self.field = field

    def __repr__(self) -> str:
        return f"attribute_not_exists({self.field})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AttributeNotExists):
            return NotImplemented
        return self.field == other.field


class And(Condition):
    """Logical AND of two or more conditions.

    Args:
        *conditions: Two or more condition objects.

    Raises:
        InsufficientConditionsError: If fewer than two conditions are provided.
    """

    def __init__(self, *conditions: Condition) -> None:
        if len(conditions) < 2:
            raise InsufficientConditionsError(operator="And", count=len(conditions))
        self.conditions = conditions

    def __repr__(self) -> str:
        return " AND ".join(f"({c!r})" for c in self.conditions)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, And):
            return NotImplemented
        return self.conditions == other.conditions

    def __and__(self, other: Condition) -> And:
        if isinstance(other, And):
            return And(*self.conditions, *other.conditions)
        return And(*self.conditions, other)


class Or(Condition):
    """Logical OR of two or more conditions.

    Args:
        *conditions: Two or more condition objects.

    Raises:
        InsufficientConditionsError: If fewer than two conditions are provided.
    """

    def __init__(self, *conditions: Condition) -> None:
        if len(conditions) < 2:
            raise InsufficientConditionsError(operator="Or", count=len(conditions))
        self.conditions = conditions

    def __repr__(self) -> str:
        return " OR ".join(f"({c!r})" for c in self.conditions)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Or):
            return NotImplemented
        return self.conditions == other.conditions

    def __or__(self, other: Condition) -> Or:
        if isinstance(other, Or):
            return Or(*self.conditions, *other.conditions)
        return Or(*self.conditions, other)


class Not(Condition):
    """Logical NOT of a single condition."""

    def __init__(self, condition: Condition) -> None:
        self.condition = condition

    def __repr__(self) -> str:
        return f"NOT ({self.condition!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Not):
            return NotImplemented
        return self.condition == other.condition


__all__ = [
    # Base classes
    "Condition",
    "ComparisonCondition",
    # Comparison operators
    "Eq",
    "Ne",
    "Lt",
    "Lte",
    "Gt",
    "Gte",
    # Range and string conditions
    "Between",
    "BeginsWith",
    "Contains",
    # IN condition
    "In",
    # Size conditions
    "Size",
    "SizeCondition",
    "SizeEq",
    "SizeNe",
    "SizeLt",
    "SizeLte",
    "SizeGt",
    "SizeGte",
    # Attribute existence
    "AttributeExists",
    "AttributeNotExists",
    # Logical operators
    "And",
    "Or",
    "Not",
    # Type aliases
]
