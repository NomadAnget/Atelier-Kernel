"""setuptools 入口；版本号单一来源于 atelier_sdk.__version__。"""

import pathlib
import re

from setuptools import find_packages, setup

_INIT = pathlib.Path(__file__).resolve().parent / "atelier_sdk" / "__init__.py"
VERSION = re.search(
    r'^__version__\s*=\s*"([^"]+)"', _INIT.read_text(encoding="utf-8"), re.MULTILINE
).group(1)

setup(
    name="atelier-sdk",
    version=VERSION,
    description="Official Python Domain SDK for Atelier Kernel",
    packages=find_packages(include=["atelier_sdk", "atelier_sdk.*"]),
    python_requires=">=3.8",
)
