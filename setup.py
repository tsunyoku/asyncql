#!/usr/bin/env python

import os
import re

from setuptools import setup


def get_package_version() -> str:
    with open(os.path.join("asyncql", "__init__.py")) as f:
        match = re.search("__version__ = ['\"]([^'\"]+)['\"]", f.read())
        if not match:
            raise RuntimeError("Unable to find version string.")

        return match.group(1)


def get_long_description() -> str:
    with open("README.md", encoding="utf8") as f:
        return f.read()


def get_packages() -> list[str]:
    return [
        directory_path
        for directory_path, _, _ in os.walk("asyncql")
        if os.path.exists(os.path.join(directory_path, "__init__.py"))
    ]


setup(
    name="asyncql",
    version=get_package_version(),
    python_requires=">=3.7",
    url="https://github.com/tsunyoku/asyncql",
    license="MIT",
    description="An asynchronous python library for all your database needs.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="tsunyoku",
    author_email="tsunyoku@gmail.com",
    packages=get_packages(),
    package_data={"asyncql": ["py.typed"]},
    extras_require={
        "postgresql": ["asyncpg"],
        "mysql": ["aiomysql"],
        "sqlite": ["aiosqlite"],
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Internet :: WWW/HTTP",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    zip_safe=False,
)
