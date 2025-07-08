"""DataFrameModel components"""
"""DataFrameModel components"""

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

from pandera.api.dataframe.model_components import FieldInfo as BaseFieldInfo
from pandera.api.checks import Check
from pandera.errors import SchemaInitError
from polars import col, Expr
from typing import Union
AnyCallable = Callable[..., Any]


class FieldInfo(BaseFieldInfo):
    """Captures extra information about a field.

    *new in 0.5.0*
    """
    def __init__(self, *args, return_expresion:bool=False, **kwargs) -> None:
        """Initialize field info."""
        super().__init__(*args, **kwargs)
        self.return_expresion: bool = return_expresion

    @property
    def name(self) -> Union[str, Expr]:
        """Return the name of the field used in the data container object."""
        return_name = self.original_name
        if self.alias is not None:
            return_name = self.alias
        
        if self.return_expresion:
            if self.regex:
                return_name = "^" + return_name + "$"
            return_name = col(return_name)

        return return_name

def Field(
    *,
    eq: Optional[Any] = None,
    ne: Optional[Any] = None,
    gt: Optional[Any] = None,
    ge: Optional[Any] = None,
    lt: Optional[Any] = None,
    le: Optional[Any] = None,
    in_range: Optional[Dict[str, Any]] = None,
    isin: Optional[Iterable[Any]] = None,
    notin: Optional[Iterable[Any]] = None,
    str_contains: Optional[str] = None,
    str_endswith: Optional[str] = None,
    str_length: Optional[Dict[str, Any]] = None,
    str_matches: Optional[str] = None,
    str_startswith: Optional[str] = None,
    nullable: bool = False,
    unique: bool = False,
    coerce: bool = False,
    regex: bool = False,
    ignore_na: bool = True,
    raise_warning: bool = False,
    n_failure_cases: Optional[int] = None,
    alias: Optional[Any] = None,
    check_name: Optional[bool] = None,
    dtype_kwargs: Optional[Dict[str, Any]] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    default: Optional[Any] = None,
    metadata: Optional[Dict[str, Any]] = None,
    return_expresion: bool = False,
    **kwargs: Any,
) -> Any:
    """Used to provide extra information about a field of a DataFrameModel.

    *new in 0.16.0*

    Some arguments apply only to numeric dtypes and some apply only to ``str``.
    See the :ref:`User Guide <dataframe-models>` for more information.

    The keyword-only arguments from ``eq`` to ``str_startswith`` are dispatched
    to the built-in :py:class:`~pandera.api.checks.Check` methods.

    :param nullable: Whether or not the column/index can contain null values.
    :param unique: Whether column values should be unique. Currently Not supported
    :param coerce: coerces the data type if ``True``.
    :param regex: whether or not the field name or alias is a regex pattern.
    :param ignore_na: whether or not to ignore null values in the checks.
    :param raise_warning: raise a warning instead of an Exception.
    :param n_failure_cases: report the first n unique failure cases. If None,
        report all failure cases.
    :param alias: The public name of the column/index.
    :param check_name: Whether to check the name of the column/index during
        validation. `None` is the default behavior, which translates to `True`
        for columns and multi-index, and to `False` for a single index.
    :param dtype_kwargs: The parameters to be forwarded to the type of the
        field.
    :param title: A human-readable label for the field.
    :param description: An arbitrary textual description of the field.
    :param metadata: An optional key-value data.
    :param kwargs: Specify custom checks that have been registered with the
        :class:`~pandera.extensions.register_check_method` decorator.
    """
    # pylint:disable=C0103,W0613,R0914
    check_kwargs = {
        "ignore_na": ignore_na,
        "raise_warning": raise_warning,
        "n_failure_cases": n_failure_cases,
    }
    args = locals()
    checks = []

    check_dispatch = _check_dispatch()
    for key in kwargs:
        if key not in check_dispatch:
            raise SchemaInitError(
                f"custom check '{key}' is not available. Make sure you use "
                "pandera.extensions.register_check_method decorator to "
                "register your custom check method."
            )

    for arg_name, check_constructor in check_dispatch.items():
        arg_value = args.get(arg_name, kwargs.get(arg_name))
        if arg_value is None:
            continue
        if isinstance(arg_value, dict):
            check_ = check_constructor(**arg_value, **check_kwargs)
        else:
            check_ = check_constructor(arg_value, **check_kwargs)
        checks.append(check_)

    return FieldInfo(
        checks=checks or None,
        nullable=nullable,
        unique=unique,
        coerce=coerce,
        regex=regex,
        check_name=check_name,
        alias=alias,
        title=title,
        description=description,
        default=default,
        dtype_kwargs=dtype_kwargs,
        metadata=metadata,
        return_expresion=return_expresion,
    )



def _check_dispatch():
    return {
        "eq": Check.equal_to,
        "ne": Check.not_equal_to,
        "gt": Check.greater_than,
        "ge": Check.greater_than_or_equal_to,
        "lt": Check.less_than,
        "le": Check.less_than_or_equal_to,
        "in_range": Check.in_range,
        "between": Check.between,
        "isin": Check.isin,
        "notin": Check.notin,
        "str_contains": Check.str_contains,
        "str_endswith": Check.str_endswith,
        "str_matches": Check.str_matches,
        "str_length": Check.str_length,
        "str_startswith": Check.str_startswith,
        "unique_values_eq": Check.unique_values_eq,
        **Check.REGISTERED_CUSTOM_CHECKS,
    }
