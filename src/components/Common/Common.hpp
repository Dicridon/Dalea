#ifndef __DALEA__COMMON__COMMON__
#define __DALEA__COMMON__COMMON__
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include <sstream>

// #define LOGGING
// #define DEBUG

namespace Dalea
{
    using String = std::string;
    using PString = pmem::obj::string;
    using PoolBase = pmem::obj::pool_base;
    using TX = pmem::obj::transaction;
    using RelativePtr = uint64_t;
    namespace pobj = pmem::obj;

    enum class FunctionStatus
    {
        Ok,
        Failed,
        SplitRequired,
        FlattenRequired,
        Retry,
    };

#ifndef DEBUG
    constexpr int BUCKET_SIZE = 10;
    constexpr int META_BITS = 16;
    constexpr int BUCKET_BITS = 10;
    constexpr int SEG_SIZE = (1 << Dalea::BUCKET_BITS);
    constexpr int SUBDIR_SIZE = (1 << 16);
    constexpr int METADIR_SIZE = (1 << 4);
#else
    constexpr int BUCKET_SIZE = 2;
    constexpr int META_BITS = 16;
    constexpr int BUCKET_BITS = 1;
    constexpr int SEG_SIZE = (1 << Dalea::BUCKET_BITS);
    constexpr int SUBDIR_SIZE = (1 << 16);
    constexpr int METADIR_SIZE = (1 << 4);
#endif

    struct HashValue
    {
        uint64_t hash_value;
        // zero is an invalid hash value for it would be used as fingerprint
        HashValue() : hash_value(0){};
        HashValue(uint64_t hv) : hash_value(hv == 0 ? hv + 1 : hv){};
        HashValue(const HashValue &h) = default;
        HashValue(HashValue &&h) = default;
        HashValue &operator=(const HashValue &h) = default;
        ~HashValue() = default;

        uint64_t SegmentBits(uint64_t depth) const noexcept;
        uint64_t BucketBits() const noexcept;
        uint64_t GetRaw() const noexcept;
        bool IsInvalid() const noexcept;
        void Invalidate() noexcept;

        bool operator==(const HashValue &h) const noexcept;
    };
} // namespace Dalea
#endif
