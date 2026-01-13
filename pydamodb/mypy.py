"""Mypy plugin for PydamoDB.

This plugin enables mypy to understand that when you access Model.attr.field_name,
the return type is ExpressionField[T] where T is the type of the field in the model.

Installation:
    1. Install mypy as a dev dependency
    2. Add to pyproject.toml:
       [tool.mypy]
    plugins = ["pydamodb.mypy"]

Usage:
    The plugin automatically handles type inference for:
    - Model.attr -> AttributePath[Model]
    - Model.attr.field_name -> ExpressionField[FieldType]

Example:
        class User(PrimaryKeyModel):
            id: str
            name: str
            age: int

        reveal_type(User.attr.name)
        This reveals ExpressionField[str].

        reveal_type(User.attr.age)
        This reveals ExpressionField[int].

        Type errors are caught:
        User.attr.nonexistent
        User.attr.age == "18"

"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mypy.plugin import Plugin

if TYPE_CHECKING:
    from collections.abc import Callable

    from mypy.nodes import TypeInfo
    from mypy.plugin import AttributeContext
    from mypy.types import Type

# Fully qualified names for PydamoDB model classes
PYDAMO_MODELS = frozenset(
    {
        "pydamodb.models.PrimaryKeyModel",
        "pydamodb.models.PrimaryKeyAndSortKeyModel",
        "pydamodb.models._PydamoModelBase",
    },
)

EXPRESSION_FIELD = "pydamodb.expressions.ExpressionField"
ATTRIBUTE_PATH = "pydamodb.fields.AttributePath"


def _is_pydamo_model(type_info: TypeInfo) -> bool:
    """Check if a TypeInfo represents a PydamoDB model."""
    if type_info.fullname in PYDAMO_MODELS:
        return True
    # Check MRO for PydamoDB base classes
    return any(base.fullname in PYDAMO_MODELS for base in type_info.mro)


def _lookup_typeinfo_from_modules(
    modules: dict[str, object],
    fullname: str,
) -> TypeInfo | None:
    """Look up a TypeInfo by fully qualified name from modules dict."""
    from mypy.nodes import MypyFile, TypeInfo

    # Split the fullname to get module and class name
    parts = fullname.rsplit(".", 1)
    if len(parts) != 2:
        return None

    module_name, class_name = parts

    if module_name not in modules:
        return None

    module = modules[module_name]
    if not isinstance(module, MypyFile):
        return None

    if class_name not in module.names:
        return None

    sym = module.names[class_name]
    if sym.node is not None and isinstance(sym.node, TypeInfo):
        return sym.node

    return None


def _get_field_type_from_model(model_info: TypeInfo, field_name: str) -> Type | None:
    """Get the type of a field from a model's TypeInfo."""
    from mypy.nodes import Var

    if field_name not in model_info.names:
        return None

    sym = model_info.names[field_name]
    if sym.node is None:
        return None

    # The node should be a Var for class attributes/fields
    if isinstance(sym.node, Var) and sym.node.type is not None:
        return sym.node.type

    return None


def _attr_path_getattr_callback(ctx: AttributeContext) -> Type:
    """Handle AttributePath[Model].__getattr__(field_name) -> ExpressionField[FieldType].

    This is called when accessing any attribute on an AttributePath instance.
    We extract the model type from AttributePath[Model] and look up the field type.
    """
    from mypy.nodes import MemberExpr
    from mypy.types import AnyType, Instance, TypeOfAny, get_proper_type

    obj_type = get_proper_type(ctx.type)

    if not isinstance(obj_type, Instance):
        return ctx.default_attr_type

    # Verify it's an AttributePath
    if obj_type.type.fullname != ATTRIBUTE_PATH:
        return ctx.default_attr_type

    # Get the model type from AttributePath[Model]
    if not obj_type.args:
        return ctx.default_attr_type

    model_type = get_proper_type(obj_type.args[0])
    if not isinstance(model_type, Instance):
        return ctx.default_attr_type

    model_info = model_type.type

    # Get the field name being accessed
    if not isinstance(ctx.context, MemberExpr):
        return ctx.default_attr_type

    field_name = ctx.context.name

    # Private attributes are not allowed
    if field_name.startswith("_"):
        ctx.api.fail(
            f"Cannot access private attribute '{field_name}' via attr",
            ctx.context,
        )
        return AnyType(TypeOfAny.from_error)

    # Look up the field in the model
    field_type = _get_field_type_from_model(model_info, field_name)

    if field_type is None:
        ctx.api.fail(
            f"'{model_info.name}' has no field '{field_name}'",
            ctx.context,
        )
        return AnyType(TypeOfAny.from_error)

    # Look up ExpressionField type
    try:
        modules = ctx.api.modules  # type: ignore[attr-defined]
    except AttributeError:
        return ctx.default_attr_type

    expr_field_info = _lookup_typeinfo_from_modules(modules, EXPRESSION_FIELD)
    if expr_field_info is None:
        return ctx.default_attr_type

    # Return ExpressionField[FieldType]
    return Instance(expr_field_info, [field_type])


def _model_attr_callback(ctx: AttributeContext) -> Type:
    """Handle Model.attr -> AttributePath[Model].

    This is called when accessing the 'attr' attribute on a PydamoDB model class.
    We return AttributePath[Model] with the proper model type parameter.
    """
    from mypy.types import CallableType, Instance, TypeType, get_proper_type

    obj_type = get_proper_type(ctx.type)

    # When accessing Class.attr, ctx.type is the class type (callable/constructor)
    # We need to extract the model type from it
    model_type: Instance | None = None

    if isinstance(obj_type, CallableType):
        # This is a class constructor - extract the return type which is the model
        ret_type = get_proper_type(obj_type.ret_type)
        if isinstance(ret_type, Instance):
            model_type = ret_type
    elif isinstance(obj_type, TypeType):
        # Type[Model] - extract the inner type
        inner = get_proper_type(obj_type.item)
        if isinstance(inner, Instance):
            model_type = inner
    elif isinstance(obj_type, Instance):
        # Direct instance access (shouldn't happen for class attr but handle it)
        model_type = obj_type

    if model_type is None:
        return ctx.default_attr_type

    if not _is_pydamo_model(model_type.type):
        return ctx.default_attr_type

    # Look up AttributePath type
    try:
        modules = ctx.api.modules  # type: ignore[attr-defined]
    except AttributeError:
        return ctx.default_attr_type

    attr_path_info = _lookup_typeinfo_from_modules(modules, ATTRIBUTE_PATH)
    if attr_path_info is None:
        return ctx.default_attr_type

    # Return AttributePath[ThisModel]
    return Instance(attr_path_info, [model_type])


class PydamoPlugin(Plugin):
    """Mypy plugin for PydamoDB type inference.

    This plugin provides type inference for the Model.attr.field_name pattern,
    returning properly typed ExpressionField instances.
    """

    def get_attribute_hook(self, fullname: str) -> Callable[[AttributeContext], Type] | None:
        """Return a hook for attribute access.

        This is called for:
        - Instance attribute access (obj.attr)
        - __getattr__ fallback access
        """
        # Hook for any attribute access on AttributePath
        # The fullname will be like "pydamodb.fields.AttributePath.field_name"
        if fullname.startswith(f"{ATTRIBUTE_PATH}."):
            attr_name = fullname[len(ATTRIBUTE_PATH) + 1 :]
            # Skip dunder methods and known internal attributes
            if not attr_name.startswith("_"):
                return _attr_path_getattr_callback

        return None

    def get_class_attribute_hook(
        self,
        fullname: str,
    ) -> Callable[[AttributeContext], Type] | None:
        """Return a hook for class attribute access.

        This is called when accessing a class attribute like Model.attr.
        The fullname will be like "module.ClassName.attr_name".
        """
        # Check if this is accessing .attr on any class
        if fullname.endswith(".attr"):
            return _model_attr_callback

        return None


def plugin(version: str) -> type[PydamoPlugin]:
    """Entry point for mypy plugin system.

    Args:
        version: The mypy version string.

    Returns:
        The plugin class.

    """
    return PydamoPlugin
