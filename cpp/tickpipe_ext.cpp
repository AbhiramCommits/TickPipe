#include <cstdint>

#include <pybind11/pybind11.h>

namespace py = pybind11;

// Native helpers for tickpipe. Placeholder until real tick-arithmetic lands;
// keeps the pybind11 build plumbing exercised end-to-end.

std::int64_t add_ns(std::int64_t a, std::int64_t b) { return a + b; }

PYBIND11_MODULE(_ext, m) {
    m.doc() = "Native helpers for tickpipe (int64 nanosecond timestamps and tick arithmetic).";
    m.def("add_ns", &add_ns, py::arg("a"), py::arg("b"),
          "Add two int64 nanosecond timestamps.");
    m.attr("__version__") = "0.1.0";
}
