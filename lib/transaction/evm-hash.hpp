#pragma once
#include <cstddef>
#include <evmc/evmc.hpp>
#include <tuple>

namespace spectrum {

#define K std::tuple<evmc::address, evmc::bytes32>

struct KeyHasher {
    size_t operator()(const K& k) const;
};

#undef K

} // namespace spectrum

