from adbc_drivers_validation.tests.connection import (
    TestConnection,  # noqa: F401
    generate_tests,
)

from . import druid


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(druid.QUIRKS, metafunc)
