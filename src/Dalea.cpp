#include "Dalea.hpp"
#include <iomanip>
namespace Dalea
{
    FunctionStatus HashTable::Put(PoolBase &pop, const std::string &key, const std::string &value) noexcept
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

        std::stringstream log_buf;
        log_buf << "Putting " << key << ": (" << seg->segment_no << ", " << hv.BucketBits() << ")\n";
        logger.Write(log_buf.str());

        auto ret = bkt->Put(pop, key, value, hv, seg->segment_no);
        switch (ret)
        {
        case FunctionStatus::Retry:
            goto RETRY;
        case FunctionStatus::SplitRequired:
        {
            split(pop, *bkt, hv, seg, seg->segment_no);
        }
            goto RETRY;
        case FunctionStatus::FlattenRequired:
        {
            flatten_bucket(pop, *bkt, hv, seg->segment_no);
        }
        default:
            return ret;
        }
    }

    KVPairPtr HashTable::Get(const std::string &key) const noexcept
    {
        auto hv = HashValue(std::hash<std::string>{}(key));
        auto seg = dir.GetSegment(hv, depth);
        auto bkt = &seg->buckets[hv.BucketBits()];
        if (bkt->HasAncestor())
        {
            seg = dir.GetSegment(bkt->GetAncestor());
            bkt = &seg->buckets[hv.BucketBits()];
        }

        std::stringstream log_buf;
        log_buf << "Searching " << key << ": (" << seg->segment_no << ", " << hv.BucketBits() << ")\n";
        logger.Write(log_buf.str());
        return bkt->Get(key, hv);
    }

    FunctionStatus HashTable::Remove(PoolBase &pop, const std::string &key) noexcept
    {
        return FunctionStatus::Ok;
    }

    void HashTable::Destory() noexcept
    {
    }

    void HashTable::Debug() const noexcept
    {
        for (uint64_t i = 0; i < (1UL << depth); i++)
        {
            std::cout << std::setw(4) << i << " ";
        }
        std::cout << std::endl;

        for (uint64_t i = 0; i < (1UL << depth); i++)
        {
            std::cout << std::setw(4) << dir.GetSegment(i)->segment_no << " ";
        }
        std::cout << std::endl;

        for (uint64_t j = 0; j < SEG_SIZE; j++)
        {
            for (uint64_t i = 0; i < (1UL << depth); i++)
            {
                if (dir.GetSegment(i)->buckets[j].HasAncestor())
                {
                    std::cout << std::setw(4) << dir.GetSegment(i)->buckets[j].GetAncestor() << " ";
                }
                else
                {
                    std::cout << "     ";
                }
            }
            std::cout << std::endl;
        }
    }

    void HashTable::DebugToLog() const
    {
        std::stringstream meta_buf;
        std::stringstream content_buf;
        for (uint64_t i = 0; i < (1UL << depth); i++)
        {
            meta_buf << std::setw(4) << i << " ";
        }
        meta_buf << std::endl;

        for (uint64_t i = 0; i < (1UL << depth); i++)
        {
            meta_buf << std::setw(4) << dir.GetSegment(i)->segment_no << " ";
        }
        meta_buf << std::endl;

        for (uint64_t j = 0; j < SEG_SIZE; j++)
        {
            for (uint64_t i = 0; i < (1UL << depth); i++)
            {
                if (dir.GetSegment(i)->buckets[j].HasAncestor())
                {
                    meta_buf << std::setw(4) << dir.GetSegment(i)->buckets[j].GetAncestor() << " ";
                }
                else
                {
                    meta_buf << "     ";
                }
            }
            meta_buf << std::endl;
        }

        for (uint64_t i = 0; i < (1UL << depth); i++)
        {
            dir.GetSegment(i)->DebugTo(content_buf);
        }

        Log(meta_buf);
        Log(content_buf);
    }

    void HashTable::Log(std::string &msg) const
    {
        logger.Write(msg);
    }
    void HashTable::Log(std::stringstream &msg_s) const
    {
        logger.Write(msg_s.str());
    }

    void HashTable::split(PoolBase &pop, Bucket &bkt, const HashValue &hv, SegmentPtr &seg, uint64_t segno) noexcept
    {
        // std::cout << "entering " << __FUNCTION__ << "\n";
        auto local_depth = bkt.GetDepth();
        if (local_depth < depth)
        {
            std::shared_lock l(doubling_lock);
            simple_split(pop, bkt, hv, segno, false);
        }
        else
        {
            if (doubling_lock.try_lock())
            {
                complex_split(pop, bkt, hv, seg, segno);
                doubling_lock.unlock();
            }
        }
        // std::cout << "leaving" << __FUNCTION__ << "\n";
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
    void HashTable::simple_split(PoolBase &pop, Bucket &bkt, const HashValue &hv, uint64_t segno, bool helper) noexcept
    {
        // std::cout << "entering " << __FUNCTION__ << "\n";
        /*
         * exclusive access to one bucket
         */
        if (!bkt.TryLock())
        {
            return;
        }

        auto prev_depth = bkt.GetDepth();
        if (helper)
        {
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
        auto root = dir.GetSegment(root_segno);
        dir.LockSegment(buddy_segno);
        if (root == dir.GetSegment(buddy_segno))
        {
            make_buddy_segment(pop, root, segno, buddy_segno, bkt);
        }
        dir.UnlockSegment(buddy_segno);

        migrate_and_pave(pop, root_segno, buddy_segno, bkt, bktbits);
        bkt.Unlock();
        // std::cout << "leaving " << __FUNCTION__ << "\n";
    }

    /*
     * Directory doubling required
     * 1. get doubling lock
     * 2. add new subdirectory to Directory if necessary
     * 3. prepare a new segment, all buckets point to corresponding buckets in old segment
     * 4. do a simple split
     * 5. add this new segment to Directory
     * 
     * 
     * only one thread woulding calling this method
     */
    void HashTable::complex_split(PoolBase &pop, Bucket &bkt, const HashValue &hv, SegmentPtr &seg, uint64_t segno) noexcept
    {
        // std::cout << "entering " << __FUNCTION__ << "\n";
        // std::unique_lock lock(doubling_lock);
        // if (depth > bkt.GetDepth())
        // {
        //     simple_split(pop, bkt, key, value, hv, segno, false);
        //     // std::cout << "leaving " << __FUNCTION__ << "\n";
        //     doubling_lock.unlock();
        //     return;
        // }
        /* 
         * bucket depth would not be changed during a doubling
         * becaused all puts to this bucket result in 
         * directory doubling and fall into this function, 
         * competing doubling_lock
         */
        auto prev_depth = bkt.GetDepth();
        bkt.UpdateSplitMetaPersist(pop);

        auto root_segno = segno & ((1UL << prev_depth) - 1);
        auto buddy_segno = root_segno | (1UL << prev_depth);

        SegmentPtr buddy = nullptr;
        dir.DoublingLink(pop, depth, depth + 1);

        auto root = dir.GetSegment(root_segno);

        // if (root == dir.GetSegment(buddy_segno))
        // {
        buddy = make_buddy_segment(pop, root, segno, buddy_segno, bkt);
        // }

        simple_split(pop, bkt, hv, segno, true);
        buddy->status = SegStatus::Quiescent;
        pmemobj_persist(pop.handle(), &buddy->status, sizeof(SegStatus));
        ++depth;
        pmemobj_persist(pop.handle(), &depth, sizeof(depth));
        bkt.ClearSplit();
        // std::cout << "leaving " << __FUNCTION__ << "\n";
    }

    /*
     * progress is similar to the sweep method in simple split
     * NOT REQUIRED FOR DALEA
     */
    void HashTable::flatten_bucket(PoolBase &pop, Bucket &bkt, const HashValue &hv, uint64_t segno) noexcept
    {
    }

    SegmentPtr HashTable::make_buddy_segment(PoolBase &pop, const SegmentPtr &root, uint64_t segno, uint64_t buddy_segno, const Bucket &bkt) noexcept
    {
        SegmentPtr buddy = nullptr;
        TX::run(pop, [&]() {
                buddy = pobj::make_persistent<Segment>(pop, bkt.GetDepth(), buddy_segno, true);
                dir.AddSegment(pop, buddy, buddy_segno);
                });

        for (int i = 0; i < SEG_SIZE; i++)
        {
            // do not persist here
            if (root->buckets[i].HasAncestor())
            {
                // avoid chaining
                buddy->buckets[i].SetAncestor(root->buckets[i].GetAncestor());
            }
            else
            {
                buddy->buckets[i].SetAncestor(segno);
            }
        }
        return buddy;
    }

    void HashTable::migrate_and_pave(PoolBase &pop, uint64_t root_segno, uint64_t buddy_segno, Bucket &bkt, uint64_t bktbits) noexcept
    {
        auto buddy_seg = dir.GetSegment(buddy_segno);
        auto buddy_bkt = &buddy_seg->buckets[bktbits];
        bkt.Migrate(pop, *buddy_bkt, root_segno);
        buddy_bkt->SetMetaPersist(pop, bkt.GetDepth(), 0, (1UL << 49));

        auto current_depth = bkt.GetDepth();
        auto difference = depth - current_depth;
        auto walk = buddy_segno;
        SegmentPtr walk_ptr;
        for (int i = 1; i < (1 << difference); i++)
        {
            walk += (1UL << current_depth);
            walk_ptr = dir.GetSegment(walk);
            dir.LockSegment(walk);
            if (walk_ptr->segment_no != walk)
            {
                // a reference pointer pointing to one ancestor
                // dir.SetSegment(pop, buddy_seg, walk);
                SegmentPtr pre_seg = nullptr;
                TX::run(pop, [&]() {
                        pre_seg = pobj::make_persistent<Segment>(pop, bkt.GetDepth(), walk, true);
                        dir.AddSegment(pop, pre_seg, walk);
                        });
                for (int i = 0; i < SEG_SIZE; i++)
                {
                    auto ans = walk_ptr->buckets[i].HasAncestor() ? walk_ptr->buckets[i].GetAncestor() : walk_ptr->segment_no.get_ro();
                    pre_seg->buckets[i].SetAncestor(ans);
                }
                pre_seg->buckets[bktbits].SetAncestor(buddy_bkt->HasAncestor() ? buddy_bkt->GetAncestor() : buddy_segno);
                dir.UnlockSegment(walk);
                continue;
            }
            dir.UnlockSegment(walk);

            if (dir.GetSegment(walk) == dir.GetSegment(buddy_segno))
            {
                /*
                 * buddy bkt has uninitialized descendents
                 * since this buddy bkt hasn't splitted yet, all the descendents would be pointing
                 * back to this buddy bkt, thus break here to save some execution time
                 */
                break;
            }
            walk_ptr->buckets[bktbits].SetAncestorPersist(pop, buddy_segno);
        }
    }
} // namespace Dalea
