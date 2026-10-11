"""Exact monetary conversion shared by imports and schema migrations."""
from decimal import Decimal, InvalidOperation


def cents(amount):
    try:
        value = Decimal(str(amount)) * 100
        if not value.is_finite() or value != value.to_integral_value():
            raise ValueError("Amounts must be finite and expressed in whole cents")
        return int(value)
    except InvalidOperation as exc:
        raise ValueError("Invalid monetary amount") from exc


