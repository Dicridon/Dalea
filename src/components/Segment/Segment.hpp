#ifndef __DALEA__SEGMENT__SEGMENT__
#define __DALEA__SEGMENT__SEGMENT__
#include "Bucket/Bucket.hpp"
namespace Dalea
{
    class Directory;
    class Segment;
    using SegmentPtr = pmem::obj::persistent_ptr<Segment>;
    enum class SegStatus
    {
        Quiescent,
        Initializing,
    };

    struct Segment
    {
        // supposed to be called within a transaction
        static SegmentPtr New(PoolBase &pop, uint8_t depth, uint64_t seg_no);

        Segment(PoolBase &pop, uint8_t depth, uint64_t segment_no, bool init);
        Segment(const Segment &) = delete;
        Segment(Segment &&) = delete;

        /* reserved methods
        KVPairPtr Get(const String &key, const HashValue &hash_value, std::shared_mutex &mux) const noexcept;
        FunctionStatus Put(PoolBase &pop, const String &key, const String &value, const HashValue &hash_value, std::shared_mutex &mux) noexcept;
        FunctionStatus Remove(const String &key, const HashValue &hash_value, std::shared_mutex &mux) const noexcept;
        */
        bool Recover() noexcept;
        SegmentPtr Split(PoolBase &pop, Directory &dir, uint64_t bkt_bits) noexcept;

        // unable to use smart pointers
        std::shared_mutex *locks;
        uint64_t segment_no;
        SegStatus status;
        Bucket buckets[SEG_SIZE];
    };
} // namespace Dalea
#endif
