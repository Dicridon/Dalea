#ifndef __DALEA__STATS__STATS__
#define __DALEA__STATS__STATS__
#include "Common/Common.hpp"
namespace Dalea{
    struct Stats
    {
        uint64_t simple_splits;
        uint64_t simple_split_time;
        uint64_t traditional_splits;
        uint64_t traditional_split_time;
        uint64_t complex_splits;
        uint64_t complex_split_time;
        uint64_t make_buddy;
        uint64_t make_buddy_time;

        Stats() : simple_splits(0), traditional_splits(0), complex_splits(0), make_buddy(0),
                  simple_split_time(0),
                  traditional_split_time(0),
                  complex_split_time(0),
                  make_buddy_time(0) {};
        Stats(const Stats &) = default;
        Stats(Stats &&) = default;
        void Show() const noexcept;
        void Clear() noexcept;
    };
}
#endif
