"""Typed attribute access for building DynamoDB expressions.

This module provides the AttributePath and AttrDescriptor classes that enable
type-safe attribute access via Model.attr.field_name patterns. When combined with
the mypy plugin, this provides full type inference for DynamoDB expression building.
"""

from typing import Any, Generic, TypeVar, overload

from pydantic import BaseModel

from pydamodb.expressions import ExpressionField

ModelT = TypeVar("ModelT", bound=BaseModel)


class AttributePath(Generic[ModelT]):
    """Provides typed attribute access for building DynamoDB expressions.

    When accessed via Model.attr.field_name, returns an ExpressionField
    typed with the field's actual type, enabling type-safe comparisons.

    Example:
        class User(PrimaryKeyModel):
            id: str
            age: int
            name: str | None

        User.attr.age
        Returns an ExpressionField[int].

        User.attr.name
        Returns an ExpressionField[str | None].

        User.attr.id
        Returns an ExpressionField[str].

    """

    _model_cls: type[ModelT]

    def __init__(self, model_cls: type[ModelT]) -> None:
        # Use object.__setattr__ to avoid triggering __setattr__
        object.__setattr__(self, "_model_cls", model_cls)

    def __getattr__(self, name: str) -> ExpressionField[Any]:
        model_cls: type[BaseModel] = object.__getattribute__(self, "_model_cls")

        if name.startswith("_"):
            raise AttributeError(f"Cannot access private attribute '{name}'")

        if name not in model_cls.model_fields:
            raise AttributeError(f"'{model_cls.__name__}' has no field '{name}'")

        field_info = model_cls.model_fields[name]
        field_path: str = field_info.alias or name

        # The return type is ExpressionField[T] where T is the field's type.
        # While we return ExpressionField[Any] at runtime, type checkers using
        # plugins or stubs can infer the actual type from the model definition.
        return ExpressionField(field_path)

    def __repr__(self) -> str:
        model_cls = object.__getattribute__(self, "_model_cls")
        return f"AttributePath({model_cls.__name__})"


class AttrDescriptor:
    """Descriptor that provides AttributePath access on model classes.

    This descriptor is assigned to the 'attr' class variable on PydamoDB models,
    enabling the Model.attr.field_name access pattern for building expressions.

    When used with the pydamodb.mypy plugin, type checkers can infer the correct
    ExpressionField type for each field access, providing full type safety.

    Example:
        class User(PrimaryKeyModel):
            id: str
            age: int

        Access returns ExpressionField[int] with type inference:
        age_field = User.attr.age

        Type checker catches errors:
        User.attr.age > 18  (OK)
        User.attr.age > "18"  (Type error)

    """

    @overload
    def __get__(self, obj: None, objtype: type[ModelT]) -> AttributePath[ModelT]: ...

    @overload
    def __get__(
        self,
        obj: ModelT,
        objtype: type[ModelT] | None = None,
    ) -> AttributePath[ModelT]: ...

    def __get__(
        self,
        obj: ModelT | None,
        objtype: type[ModelT] | None = None,
    ) -> AttributePath[ModelT]:
        if objtype is None:
            raise AttributeError("Cannot access attr from instance without class")
        return AttributePath(objtype)


__all__ = [
    "AttrDescriptor",
    "AttributePath",
]
