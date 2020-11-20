#ifndef __DALEA__STATS__STATS__
#define __DALEA__STATS__STATS__
#include "Common/Common.hpp"
namespace Dalea{
    struct Stats
    {
        uint64_t simple_splits;
        uint64_t traditional_splits;
        uint64_t complex_splits;

        Stats() : simple_splits(0), traditional_splits(0), complex_splits(0) {};
        Stats(const Stats &) = default;
        Stats(Stats &&) = default;
        void Show() const noexcept;
        void Clear() noexcept;
    };
}
#endif
