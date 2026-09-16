"""Setuptools build configuration for tickpipe.

Builds two pybind11 extensions:
* ``tickpipe._ext`` — placeholder native helpers;
* ``tickpipe.backtest._replay`` — the C++17 event-driven replay engine.

Set ``TICKPIPE_NATIVE=1`` to add ``-march=native`` to the replay engine build.
It is off by default so builds stay reproducible across machines.
"""

import os

from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import find_packages, setup


def _native_flags() -> list[str]:
    if os.environ.get("TICKPIPE_NATIVE") == "1":
        return ["-march=native"]
    return []


ext_modules = [
    Pybind11Extension(
        "tickpipe._ext",
        ["cpp/tickpipe_ext.cpp"],
        cxx_std=17,
    ),
    Pybind11Extension(
        "tickpipe.backtest._replay",
        ["cpp/src/replay.cpp"],
        include_dirs=["cpp/src"],
        cxx_std=17,
        extra_compile_args=_native_flags(),
    ),
]


setup(
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
)
