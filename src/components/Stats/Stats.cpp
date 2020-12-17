#include "Stats.hpp"
namespace Dalea
{
    void Stats::Show() const noexcept
    {
        std::cout << "simple splits: " << simple_splits;
        std::cout << "traditional splits: " << traditional_splits;
        std::cout << "complex splits: " << complex_splits;
    }

    void Stats::Clear() noexcept
    {
        simple_splits = 0;
        traditional_splits = 0;
        complex_splits = 0;
        simple_split_time = 0;
        traditional_split_time = 0;
        complex_split_time = 0;
    }
}
