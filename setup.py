from setuptools import find_packages, setup
from pybind11.setup_helpers import Pybind11Extension, build_ext

ext_modules = [
    Pybind11Extension(
        "tickpipe._ext",
        ["cpp/tickpipe_ext.cpp"],
        cxx_std=17,
    ),
]

setup(
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
)
