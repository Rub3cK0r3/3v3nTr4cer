import sys

import pybind11
from setuptools import Extension, find_packages, setup


setup(
    name="3v3ntr4cer",
    packages=find_packages(where="src", exclude=("tests", "tests.*")),
    package_dir={"": "src"},
    python_requires=">=3.11",
    ext_modules=[
        Extension(
            "core.async_lib._async_manager",
            ["src/core/async_lib/systems_impl/async_manager.cpp"],
            include_dirs=[pybind11.get_include()],
            language="c++",
            extra_compile_args=["/std:c++17"]
            if sys.platform == "win32"
            else ["-std=c++17"],
        )
    ],
    zip_safe=False,
)