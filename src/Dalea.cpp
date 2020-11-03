#include "Dalea.hpp"
namespace Dalea
{
    FunctionStatus HashTable::Put(PoolBase &pop, std::string &key, std::string &value) noexcept
    {
        auto hv = HashValue(std::hash<std::string>{}(key));
    RETRY:
        auto seg = dir.GetSegment(hv, depth);
        auto bkt = &seg->buckets[hv.BucketBits()];

        if (bkt->HasAncestor())
        {
            seg = dir.GetSegment(bkt->GetAncestor());
            bkt = &seg->buckets[hv.BucketBits()];
        }
        
        std::cout << key << ": (" << seg->segment_no << ", " << hv.BucketBits() << ")\n";

        auto ret = bkt->Put(pop, key, value, hv, seg->locks[hv.BucketBits()], seg->segment_no);
        switch (ret)
        {
        case FunctionStatus::Retry:
            goto RETRY;
        case FunctionStatus::SplitRequired:
        {
            std::unique_lock lock(seg->locks[hv.BucketBits()]);
            split(pop, *bkt, key, value, hv, seg, seg->segment_no);
        }
            goto RETRY;
        case FunctionStatus::FlattenRequired:
        {
            flatten_bucket(pop, *bkt, key, value, hv, seg->segment_no);
        }
        default:
            return ret;
        }
    }

    KVPairPtr HashTable::Get(std::string &key) const noexcept
    {
        auto hv = HashValue(std::hash<std::string>{}(key));
        auto seg = dir.GetSegment(hv, depth);
        auto bkt = &seg->buckets[hv.BucketBits()];
        if (bkt->HasAncestor())
        {
            seg = dir.GetSegment(bkt->GetAncestor());
            bkt = &seg->buckets[hv.BucketBits()];
        }
        return bkt->Get(key, hv, seg->locks[hv.BucketBits()]);
    }

    FunctionStatus HashTable::Remove(PoolBase &pop, std::string &key, std::string &value) noexcept
    {
        return FunctionStatus::Ok;
    }

    void HashTable::split(PoolBase &pop, Bucket &bkt, std::string &key, std::string &value, const HashValue &hv, SegmentPtr &seg, uint64_t segno) noexcept
    {
        auto local_depth = bkt.GetDepth();
        if (local_depth < depth)
        {
            simple_split(pop, bkt, key, value, hv, segno, false);
        }
        else
        {
            complex_split(pop, bkt, key, value, hv, seg, segno);
        }
    }

    /*
     * Involves no directory doubling
     * 1. set bucket's split byte; for crush consistency
     * 2. find the buddy bucket: check bucket depth change; e.g. 1 -> 01 and 11
     * 3. migrates kvs to buddy bucket in buddy segment(DO NOT DELETE THEM YET)
     * 4. sweep all bucket with proper encoding and adjust their 'ancestor's to point to 
     *    buddy bucket:
     *    1. update splitting bucket's local depth
     *    2. compute 'differecce' between local depth and global depth
     *    3. iterate 'difference' times to update buckets in proper segments:
     *    for (int i = 0; i < 2 ^ difference; i++)
     *    {
     *        segments[buddy_segment_bits + (1 << local_depth)][bucket_bits].ancestor = buddy_segmnet_bits
     *    }
     *  5. unset bucket's split byte
     *  6. insert kv
     * 
     * 
     *  concurrent sweep seems to be viable, for each thread touches different kvs, ther is no conflicts
     *  but should ensure the correctness of global depth after a crash
     */
    void HashTable::simple_split(PoolBase &pop, Bucket &bkt, std::string &key, std::string &value, const HashValue &hv, uint64_t segno, bool helper) noexcept
    {
        auto prev_depth = bkt.GetDepth();
        if (helper) {
            --prev_depth;
        } 
        else 
        {
            bkt.UpdateSplitMetaPersist(pop);
        }

        // clear all high bits of segno
        auto root_segno = segno & ((1UL << prev_depth) - 1);
        auto buddy_segno = root_segno | (1UL << prev_depth);
        auto bktbits = hv.BucketBits();
        auto buddy_bkt = &dir.GetSegment(buddy_segno)->buckets[bktbits];
        bkt.Migrate(*buddy_bkt, root_segno);
        buddy_bkt->SetMetaPersist(pop, bkt.GetDepth(), 0, (1UL << 49));

        auto current_depth = bkt.GetDepth();
        auto difference = depth - current_depth;
        auto walk = buddy_segno;
        for (int i = 1; i < (1 << difference); i++)
        {
            dir.GetSegment(walk += (1UL << current_depth))->buckets[bktbits].SetAncestorPersist(pop, buddy_segno);
        }
    }

    /*
     * Directory doubling required
     * 1. get doubling lock
     * 2. add new subdirectory to Directory if necessary
     * 3. prepare a new segment, all buckets point to corresponding buckets in old segment
     * 4. do a simple split
     * 5. add this new segment to Directory
     */
    void HashTable::complex_split(PoolBase &pop, Bucket &bkt, std::string &key, std::string &value, const HashValue &hv, SegmentPtr &seg, uint64_t segno) noexcept
    {
        std::unique_lock lock(doubling_lock);
        if (depth > bkt.GetDepth())
        {
            simple_split(pop, bkt, key, value, hv, segno, false);
            return;
        }
        auto prev_depth = bkt.GetDepth();
        bkt.UpdateSplitMetaPersist(pop);

        auto root_segno = segno & ((1UL << prev_depth) - 1);
        auto buddy_segno = root_segno | (1UL << prev_depth);

        SegmentPtr buddy = nullptr;
        dir.DoublingLink(depth, depth + 1);

        if (dir.GetSegment(root_segno) == dir.GetSegment(buddy_segno))
        {
            TX::run(pop, [&]() {
                    buddy = pobj::make_persistent<Segment>(pop, bkt.GetDepth(), buddy_segno, true);
                    dir.AddSegment(pop, buddy, buddy_segno);
                    });

            for (int i = 0; i < SEG_SIZE; i++)
            {
                // do not persist here
                buddy->buckets[i].SetAncestor(segno);
            }
        }

        simple_split(pop, bkt, key, value, hv, segno, true);
        buddy->status = SegStatus::Quiescent;
        pmemobj_persist(pop.handle(), &buddy->status, sizeof(SegStatus));
        ++depth;
        pmemobj_persist(pop.handle(), &depth, sizeof(depth));
    }

    /*
     * progress is similar to the sweep method in simple split
     * NOT REQUIRED FOR DALEA
     */
    void HashTable::flatten_bucket(PoolBase &pop, Bucket &bkt, std::string &key, std::string &value, const HashValue &hv, uint64_t segno) noexcept
    {
    }
} // namespace Dalea
