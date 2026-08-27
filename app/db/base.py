"""Declarative base and shared metadata.

Importing this module must not import anything that requires a live connection,
because Alembic imports it to obtain the target metadata.
"""

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

# Explicit naming convention so Alembic autogenerates stable constraint names
# instead of relying on PostgreSQL defaults.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
