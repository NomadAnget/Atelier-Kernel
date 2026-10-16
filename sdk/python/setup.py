from setuptools import find_packages, setup

setup(
    name="atelier-sdk",
    version="0.1.3",
    description="Official Python Domain SDK for Atelier Kernel",
    packages=find_packages(include=["atelier_sdk", "atelier_sdk.*"]),
    python_requires=">=3.8",
)
