"""Expression helpers for DynamoDB condition and update statements.

The main entry points are:
- `ExpressionField`: a typed field reference used to build condition objects
- `ExpressionBuilder`: converts condition/update objects into DynamoDB expression strings
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Generic, TypeVar, overload

from pydantic_core import to_jsonable_python

from pydamodb.conditions import (
    And,
    AttributeExists,
    AttributeNotExists,
    BeginsWith,
    Between,
    ComparisonCondition,
    Condition,
    Contains,
    Eq,
    Gt,
    Gte,
    In,
    Lt,
    Lte,
    Ne,
    Not,
    Or,
    Size,
    SizeCondition,
)
from pydamodb.exceptions import EmptyUpdateError, UnknownConditionTypeError

T = TypeVar("T")


class ExpressionField(Generic[T]):
    """A typed field reference for building DynamoDB expressions.

    The type parameter T represents the type of the field's value,
    enabling type-safe comparisons in condition expressions.

    Example:
        class User(PrimaryKeyModel):
            id: str
            age: int
            name: str

        User.attr.age > 18

        This is valid because `age` is an int.

        User.attr.age > "18"
        This is a type error because it compares int to str.
    """

    __slots__ = ("_path",)

    def __init__(self, path: str) -> None:
        self._path = path

    def __str__(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"ExpressionField({self._path!r})"

    def __hash__(self) -> int:
        return hash(self._path)

    def __getattr__(self, item: str) -> ExpressionField[Any]:
        return ExpressionField(f"{self._path}.{item}")

    def __getitem__(self, item: str | int) -> ExpressionField[Any]:
        if isinstance(item, int):
            return ExpressionField(f"{self._path}[{item}]")
        return ExpressionField(f"{self._path}.{item}")

    @overload  # type: ignore[override]
    def __eq__(self, other: T) -> Eq[T]: ...

    @overload
    def __eq__(self, other: ExpressionField[T]) -> Eq[T]: ...

    def __eq__(self, other: T | ExpressionField[T]) -> Eq[T]:  # type: ignore[override, unused-ignore]
        return Eq(field=self._path, value=other)  # type: ignore[arg-type]

    @overload  # type: ignore[override]
    def __ne__(self, other: T) -> Ne[T]: ...

    @overload
    def __ne__(self, other: ExpressionField[T]) -> Ne[T]: ...

    def __ne__(self, other: T | ExpressionField[T]) -> Ne[T]:  # type: ignore[override, unused-ignore]
        return Ne(field=self._path, value=other)  # type: ignore[arg-type]

    @overload
    def __lt__(self, other: T) -> Lt[T]: ...

    @overload
    def __lt__(self, other: ExpressionField[T]) -> Lt[T]: ...

    def __lt__(self, other: T | ExpressionField[T]) -> Lt[T]:
        return Lt(field=self._path, value=other)  # type: ignore[arg-type]

    @overload
    def __le__(self, other: T) -> Lte[T]: ...

    @overload
    def __le__(self, other: ExpressionField[T]) -> Lte[T]: ...

    def __le__(self, other: T | ExpressionField[T]) -> Lte[T]:
        return Lte(field=self._path, value=other)  # type: ignore[arg-type]

    @overload
    def __gt__(self, other: T) -> Gt[T]: ...

    @overload
    def __gt__(self, other: ExpressionField[T]) -> Gt[T]: ...

    def __gt__(self, other: T | ExpressionField[T]) -> Gt[T]:
        return Gt(field=self._path, value=other)  # type: ignore[arg-type]

    @overload
    def __ge__(self, other: T) -> Gte[T]: ...

    @overload
    def __ge__(self, other: ExpressionField[T]) -> Gte[T]: ...

    def __ge__(self, other: T | ExpressionField[T]) -> Gte[T]:
        return Gte(field=self._path, value=other)  # type: ignore[arg-type]

    def between(self, low: T, high: T) -> Between[T]:
        """Create a BETWEEN condition: field BETWEEN low AND high.

        Args:
            low: The lower bound (inclusive).
            high: The upper bound (inclusive).

        Returns:
            A Between condition for use in queries.
        """
        return Between(field=self._path, low=low, high=high)

    @overload
    def begins_with(self: ExpressionField[str], prefix: str) -> BeginsWith: ...

    @overload
    def begins_with(self, prefix: str) -> BeginsWith: ...

    def begins_with(self, prefix: str) -> BeginsWith:
        """Create a begins_with condition for string fields.

        This method is only valid for string fields. Using it on non-string
        fields will result in a DynamoDB error at runtime.

        Args:
            prefix: The string prefix to match.

        Returns:
            A BeginsWith condition for use in queries.

        Example:
            Find users whose name starts with "John":
            User.attr.name.begins_with("John")
        """
        return BeginsWith(field=self._path, prefix=prefix)

    def contains(self, value: T) -> Contains[T]:
        """Create a contains(field, value) condition.

        For strings this checks substring membership. For sets/lists it checks
        element membership.

        Args:
            value: The value to check for.

        Returns:
            A Contains condition.
        """
        return Contains(field=self._path, value=value)

    def in_(self, *values: T) -> In[T]:
        """Create an IN condition: field IN (value1, value2, ...).

        Args:
            *values: The values to check against.

        Returns:
            An In condition for use in queries/conditions.

        Example:
            Find users in specific cities:
            User.attr.city.in_("NYC", "LA", "Chicago")
        """
        return In(field=self._path, values=list(values))

    def size(self) -> Size:
        """Get the size of this field for comparison.

        Returns a Size wrapper that can be compared with integers.
        Works with strings (length), lists (item count), maps (key count),
        sets (element count), and binary (byte length).

        Returns:
            A Size wrapper for building size conditions.

        Example:
            User.attr.tags.size() > 3

            User.attr.name.size() >= 2
        """
        return Size(field=self._path)

    def exists(self) -> AttributeExists:
        """Create an attribute_exists(field) condition."""
        return AttributeExists(field=self._path)

    def not_exists(self) -> AttributeNotExists:
        """Create an attribute_not_exists(field) condition."""
        return AttributeNotExists(field=self._path)


class ExpressionBuilder:
    """Build DynamoDB expression strings and placeholder maps.

    This class converts condition objects into `ConditionExpression` strings and
    update mappings into `UpdateExpression` strings. While building expressions
    it also accumulates the corresponding `ExpressionAttributeNames` and
    `ExpressionAttributeValues` maps.
    """

    def __init__(self) -> None:
        self._name_counter = 0
        self._value_counter = 0
        self._attribute_names: dict[str, str] = {}
        self._attribute_values: dict[str, Any] = {}

    def _get_name_placeholder(self, field: str) -> str:
        """Return a placeholder path for an attribute name.

        For nested paths like `"a.b[0].c"`, each path component is assigned a
        placeholder (e.g. `#n0.#n1.#n2.#n3`) and recorded in
        `attribute_names`.
        """
        # Handle nested fields (e.g., "address.city" -> "#n0.#n1")
        parts = field.replace("[", ".").replace("]", "").split(".")
        placeholders = []
        for part in parts:
            if part not in self._attribute_names:
                placeholder = f"#n{self._name_counter}"
                self._name_counter += 1
                self._attribute_names[placeholder] = part
            else:
                placeholder = next(k for k, v in self._attribute_names.items() if v == part)
            placeholders.append(placeholder)
        return ".".join(placeholders)

    def _get_value_placeholder(self, value: Any) -> str:
        """Return a placeholder for a literal value and record it.

        The value is normalized via `to_jsonable_python` and stored in
        `attribute_values`.
        """
        placeholder = f":v{self._value_counter}"
        self._value_counter += 1
        self._attribute_values[placeholder] = to_jsonable_python(value)
        return placeholder

    def build_condition_expression(self, condition: Condition) -> str:
        """Build a DynamoDB ConditionExpression string for a condition object."""
        if isinstance(condition, ComparisonCondition):
            name_ph = self._get_name_placeholder(condition.field)
            value_ph = self._get_value_placeholder(condition.value)
            return f"{name_ph} {condition.operator} {value_ph}"

        if isinstance(condition, Between):
            name_ph = self._get_name_placeholder(condition.field)
            low_ph = self._get_value_placeholder(condition.low)
            high_ph = self._get_value_placeholder(condition.high)
            return f"{name_ph} BETWEEN {low_ph} AND {high_ph}"

        if isinstance(condition, BeginsWith):
            name_ph = self._get_name_placeholder(condition.field)
            value_ph = self._get_value_placeholder(condition.prefix)
            return f"begins_with({name_ph}, {value_ph})"

        if isinstance(condition, Contains):
            name_ph = self._get_name_placeholder(condition.field)
            value_ph = self._get_value_placeholder(condition.value)
            return f"contains({name_ph}, {value_ph})"

        if isinstance(condition, In):
            name_ph = self._get_name_placeholder(condition.field)
            value_phs = [self._get_value_placeholder(v) for v in condition.values]
            return f"{name_ph} IN ({', '.join(value_phs)})"

        if isinstance(condition, SizeCondition):
            name_ph = self._get_name_placeholder(condition.field)
            value_ph = self._get_value_placeholder(condition.value)
            return f"size({name_ph}) {condition.operator} {value_ph}"

        if isinstance(condition, AttributeExists):
            name_ph = self._get_name_placeholder(condition.field)
            return f"attribute_exists({name_ph})"

        if isinstance(condition, AttributeNotExists):
            name_ph = self._get_name_placeholder(condition.field)
            return f"attribute_not_exists({name_ph})"

        if isinstance(condition, And):
            parts = [f"({self.build_condition_expression(c)})" for c in condition.conditions]
            return " AND ".join(parts)

        if isinstance(condition, Or):
            parts = [f"({self.build_condition_expression(c)})" for c in condition.conditions]
            return " OR ".join(parts)

        if isinstance(condition, Not):
            inner = self.build_condition_expression(condition.condition)
            return f"NOT ({inner})"

        raise UnknownConditionTypeError(type(condition))

    def build_update_expression(self, updates: Mapping[ExpressionField[Any], Any]) -> str:
        """Build a DynamoDB UpdateExpression from a mapping of fields to values.

        Args:
            updates: A mapping of ExpressionField to new values.
                     Each field will be set to its corresponding value.

        Returns:
            A DynamoDB UpdateExpression string (e.g., "SET #n0 = :v0, #n1 = :v1").

        Raises:
            EmptyUpdateError: If no updates are provided.
        """
        set_clauses: list[str] = []

        for field, value in updates.items():
            name_ph = self._get_name_placeholder(str(field))
            value_ph = self._get_value_placeholder(value)
            set_clauses.append(f"{name_ph} = {value_ph}")

        if not set_clauses:
            raise EmptyUpdateError()

        return "SET " + ", ".join(set_clauses)

    @property
    def attribute_names(self) -> dict[str, str]:
        """Mapping of name placeholders (e.g. `#n0`) to attribute names."""
        return self._attribute_names

    @property
    def attribute_values(self) -> dict[str, Any]:
        """Mapping of value placeholders (e.g. `:v0`) to literal values."""
        return self._attribute_values


UpdateMapping = Mapping[ExpressionField[Any], Any]

__all__ = [
    "ExpressionField",
    "ExpressionBuilder",
    "UpdateMapping",
]
