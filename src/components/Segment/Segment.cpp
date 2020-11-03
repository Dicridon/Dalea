#include "Segment.hpp"
namespace Dalea
{
    SegmentPtr Segment::New(PoolBase &pop, uint8_t depth, uint64_t seg_no)
    {
        SegmentPtr seg;
        seg = pobj::make_persistent<Segment>(pop, depth, seg_no, false);
        return seg;
    }

    Segment::Segment(PoolBase &pop, uint8_t depth, uint64_t seg_no, bool init) : segment_no(seg_no)
    {
        if (init)
        {
            status = SegStatus::Initializing;
        }
        else
        {
            status = SegStatus::Quiescent;
        }

        locks = new std::shared_mutex[SEG_SIZE];
        std::for_each(std::begin(buckets), std::end(buckets), [&](Bucket &bkt) {
            bkt.SetDepth(depth);
        });
    }

    /*
    KVPairPtr Segment::Get(const String &key, const HashValue &hash_value, std::shared_mutex &mux) const noexcept
    {
        auto select = hash_value.BucketBits();
        return buckets[select].Get(key, hash_value, locks[select]);
    }

    // TODO
    FunctionStatus Segment::Put(PoolBase &pop, const String &key, const String &value, const HashValue &hash_value, std::shared_mutex &mux) noexcept
    {
        auto select = hash_value.BucketBits();
        return buckets[select].Put(pop, key, value, hash_value, locks[select]);
    }

    FunctionStatus Segment::Remove(const String &key, const HashValue &hash_value, std::shared_mutex &mux) const noexcept
    {
    }
    */
    bool Segment::Recover() noexcept
    {
        locks = new std::shared_mutex[SEG_SIZE];
        return true;
    }

    SegmentPtr Segment::Split(PoolBase &pop, Directory &dir, uint64_t bucket_bits) noexcept
    {
        return nullptr;
    }

    void Segment::Debug() const noexcept
    {
        std::cout << "[[ Segment " << segment_no << " reporting: \n";
        uint64_t tag = 0;
        std::for_each(std::begin(buckets), std::end(buckets), [&](const Bucket &bkt) {
            bkt.Debug(tag++);
        });
    }

} // namespace Dalea