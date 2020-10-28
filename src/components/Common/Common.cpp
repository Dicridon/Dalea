#include "Common.hpp"
namespace Dalea
{
    uint64_t HashValue::SegmentBits(uint64_t depth) const noexcept
    {
        uint64_t mask = (1ULL << depth) - 1;
        return hash_value & mask;
    }
    uint64_t HashValue::BucketBits() const noexcept
    {
        return (hash_value << META_BITS) >> (64 - META_BITS - BUCKET_SIZE);
    }

    uint64_t HashValue::GetRaw() const noexcept
    {
        return hash_value;
    }

    bool HashValue::IsInvalid() const noexcept
    {
        return hash_value == 0;
    }

    void HashValue::Invalidate() noexcept
    {
        hash_value = 0;
    }

    bool HashValue::operator==(const HashValue &h) const noexcept
    {
        return hash_value == h.hash_value;
    }
} // namespace Dalea