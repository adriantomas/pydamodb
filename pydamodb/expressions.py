"""Expression helpers for DynamoDB condition and update statements.

The main entry points are:
- `ExpressionField`: a field reference used to build condition objects
- `ExpressionBuilder`: converts condition/update objects into DynamoDB expression strings
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

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


class ExpressionField:
    """A field reference for building DynamoDB expressions.

    Example:
        class User(PrimaryKeyModel):
            id: str
            age: int
            name: str

        User.attr("age") > 18

    """

    __slots__ = ("_path",)

    def __init__(self, path: str) -> None:
        self._path = path

    @property
    def field(self) -> str:
        """The DynamoDB attribute path represented by this field."""
        return self._path

    def __str__(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"ExpressionField({self._path!r})"

    def __hash__(self) -> int:
        return hash(self._path)

    def __eq__(self, other: Any) -> Eq:  # ty: ignore[invalid-method-override]
        return Eq(field=self._path, value=other)

    def __ne__(self, other: Any) -> Ne:  # ty: ignore[invalid-method-override]
        return Ne(field=self._path, value=other)

    def __lt__(self, other: Any) -> Lt:
        return Lt(field=self._path, value=other)

    def __le__(self, other: Any) -> Lte:
        return Lte(field=self._path, value=other)

    def __gt__(self, other: Any) -> Gt:
        return Gt(field=self._path, value=other)

    def __ge__(self, other: Any) -> Gte:
        return Gte(field=self._path, value=other)

    def between(self, low: Any, high: Any) -> Between:
        """Create a BETWEEN condition: field BETWEEN low AND high.

        Args:
            low: The lower bound (inclusive).
            high: The upper bound (inclusive).

        Returns:
            A Between condition for use in queries.

        """
        return Between(field=self._path, low=low, high=high)

    def begins_with(self, prefix: str) -> BeginsWith:
        """Create a begins_with condition for string fields.

        Args:
            prefix: The string prefix to match.

        Returns:
            A BeginsWith condition for use in queries.

        Example:
            Find users whose name starts with "John":
            User.attr("name").begins_with("John")

        """
        return BeginsWith(field=self._path, prefix=prefix)

    def contains(self, value: Any) -> Contains:
        """Create a contains(field, value) condition.

        For strings this checks substring membership. For sets/lists it checks
        element membership.

        Args:
            value: The value to check for.

        Returns:
            A Contains condition.

        """
        return Contains(field=self._path, value=value)

    def in_(self, *values: Any) -> In:
        """Create an IN condition: field IN (value1, value2, ...).

        Args:
            *values: The values to check against.

        Returns:
            An In condition for use in queries/conditions.

        Example:
            Find users in specific cities:
            User.attr("city").in_("NYC", "LA", "Chicago")

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
            User.attr("tags").size() > 3

            User.attr("name").size() >= 2

        """
        return Size(field=self._path)

    def exists(self) -> AttributeExists:
        """Create an attribute_exists(field) condition.

        Returns a condition that checks if the attribute exists on the item,
        regardless of its value (including null).

        Returns:
            An AttributeExists condition.

        Example:
            User.attr("optional_field").exists()

        """
        return AttributeExists(field=self._path)

    def not_exists(self) -> AttributeNotExists:
        """Create an attribute_not_exists(field) condition.

        Returns a condition that checks if the attribute does not exist on the item.
        Useful for conditional writes that should only succeed if a field is not set.

        Returns:
            An AttributeNotExists condition.

        Example:
            User.attr("optional_field").not_exists()

        """
        return AttributeNotExists(field=self._path)


class ExpressionBuilder:
    """Build DynamoDB expression strings and placeholder maps.

    This class converts condition objects into ConditionExpression strings and
    update mappings into UpdateExpression strings. While building expressions,
    it accumulates the corresponding ExpressionAttributeNames and
    ExpressionAttributeValues maps required by DynamoDB's expression API.

    The builder handles:
    - Nested attribute paths (e.g., "address.city")
    - Array indexing (e.g., "tags[0]")
    - Comparison conditions (=, <>, <, <=, >, >=)
    - Function conditions (begins_with, contains, size, attribute_exists, attribute_not_exists)
    - Logical operators (AND, OR, NOT)
    - Between and IN conditions

    Example:
        builder = ExpressionBuilder()
        condition = And(User.attr("age") > 18, User.attr("status") == "active")
        expr = builder.build_condition_expression(condition)
        expr is "(#n0 > :v0) AND (#n1 = :v1)"
        builder.attribute_names is {"#n0": "age", "#n1": "status"}
        builder.attribute_values is {":v0": 18, ":v1": "active"}

    """

    def __init__(self) -> None:
        self._name_counter = 0
        self._value_counter = 0
        self._attribute_names: dict[str, str] = {}
        self._name_reverse: dict[str, str] = {}
        self._attribute_values: dict[str, Any] = {}

    def _get_name_placeholder(self, field: str) -> str:
        """Return a placeholder path for an attribute name.

        DynamoDB requires attribute names to be replaced with placeholders to handle
        reserved words and special characters. This method creates placeholders for
        each component of a field path.

        Nested attribute names get their own placeholder (e.g., "address.city"
        → "#n0.#n1"). Array indices are kept as literal numbers in brackets
        (e.g., "tags[0]" → "#n0[0]", "address.items[2].name" → "#n0.#n1[2].#n2").

        Args:
            field: The field path (e.g., "address.city" or "tags[0].name").

        Returns:
            The placeholder path (e.g., "#n0.#n1" or "#n0[0].#n1").

        """
        parts = field.split(".")
        result: list[str] = []
        for part in parts:
            if "[" in part:
                name, _, rest = part.partition("[")
                index = rest.rstrip("]")
                result.append(f"{self._name_to_placeholder(name)}[{index}]")
            else:
                result.append(self._name_to_placeholder(part))
        return ".".join(result)

    def _name_to_placeholder(self, name: str) -> str:
        """Return an existing placeholder for name, or create a new one."""
        existing = self._name_reverse.get(name)
        if existing is not None:
            return existing
        placeholder = f"#n{self._name_counter}"
        self._name_counter += 1
        self._attribute_names[placeholder] = name
        self._name_reverse[name] = placeholder
        return placeholder

    def _get_value_placeholder(self, value: Any) -> str:
        """Return a placeholder for a literal value and record it.

        DynamoDB requires all literal values in expressions to be replaced with
        placeholders. This method creates a unique placeholder for each value.

        The value is normalized via to_jsonable_python (from pydantic_core) to ensure
        proper serialization of Python types to DynamoDB-compatible JSON.

        Args:
            value: The value to create a placeholder for.

        Returns:
            A placeholder string like ":v0", ":v1", etc.

        """
        placeholder = f":v{self._value_counter}"
        self._value_counter += 1
        self._attribute_values[placeholder] = to_jsonable_python(value)
        return placeholder

    def build_key_equality(self, field: str, value: Any) -> str:
        """Build a ``field = value`` equality expression for key conditions.

        Registers the necessary placeholders and returns the expression string.
        Prefer this over calling ``_get_name_placeholder`` / ``_get_value_placeholder``
        directly from outside this class.

        Args:
            field: The attribute name.
            value: The value to compare against.

        Returns:
            A DynamoDB condition expression string (e.g. ``#n0 = :v0``).

        """
        name_ph = self._get_name_placeholder(field)
        value_ph = self._get_value_placeholder(value)
        return f"{name_ph} = {value_ph}"

    def build_condition_expression(self, condition: Condition) -> str:
        """Build a DynamoDB ConditionExpression string from a condition object.

        Recursively processes the condition tree and generates the appropriate
        DynamoDB expression syntax. Populates attribute_names and attribute_values
        as needed.

        Args:
            condition: The condition object to convert (e.g., Eq, And, BeginsWith).

        Returns:
            A DynamoDB ConditionExpression string.

        Raises:
            UnknownConditionTypeError: If an unsupported condition type is encountered.

        """
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

    def build_update_expression(self, updates: Mapping[ExpressionField, Any]) -> str:
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


UpdateMapping = Mapping[ExpressionField, Any]

__all__ = [
    "ExpressionBuilder",
    "ExpressionField",
    "UpdateMapping",
]
