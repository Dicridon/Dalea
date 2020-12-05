#include "Dalea.hpp"

#include <chrono>
#include <iomanip>
#include <thread>

#define LINE                                  \
    {                                         \
        Log(std::to_string(__LINE__) + "\n"); \
    }
// #define TIMING
#define PREALLOCATION
namespace Dalea
{
    SegmentPtrQueue::SegmentPtrQueue(PoolBase &pop, int init_cap) : capacity(init_cap), size(0)
    {
        TX::run(pop, [&]() {
            buffer = pobj::make_persistent<SegmentPtr[]>(init_cap + 1);
        });

        head = tail = 0;
    }

    bool SegmentPtrQueue::Push(const SegmentPtr &ptr) noexcept
    {
        std::unique_lock _(lock);
        if (size == capacity)
        {
            return false;
        }
        buffer[(tail++) % capacity] = ptr;
        size++;
        return true;
    }

    SegmentPtr SegmentPtrQueue::Top() noexcept
    {
        std::unique_lock _(lock);
        if (size == 0)
        {
            return nullptr;
        }
        return buffer[head % capacity];
    }

    SegmentPtr SegmentPtrQueue::Pop() noexcept
    {
        std::unique_lock _(lock);
        if (size == 0)
        {
            return nullptr;
        }
        size--;
        return buffer[(head++) % capacity];
    }

    bool SegmentPtrQueue::HasSpace() const noexcept
    {
        return size != capacity;
    }

    HashTable::HashTable(PoolBase &pop)
        : dir(pop),
          depth(INIT_DEPTH),
          to_double(false),
          readers(0),
          logger(std::string("./dalea.log")),
          capacity(2 * SEG_SIZE * BUCKET_SIZE),
          segment_pool(pop, 4096 * 15)
    {
#ifdef PREALLOCATION
        TX::run(pop, [&]() {
            for (int i = 0; i < 4096 * 15; i++)
            {
                auto ptr = pobj::make_persistent<Segment>(pop, 0, 0, false);
                segment_pool.Push(ptr.get());
            }
        });
        auto guardian = [&](PoolBase &pop, SegmentPtrQueue &queue) {
            while(1) {
                while (queue.HasSpace())
                {
                    TX::run(pop, [&]() {
                        auto ptr = pobj::make_persistent<Segment>(pop, 0, 0, false);
                        queue.Push(ptr.get());
                    });
                }
            }
        };
        // std::thread(guardian, std::ref(pop), std::ref(segment_pool)).detach();
        // std::thread(guardian, std::ref(pop), std::ref(segment_pool)).detach();
        // std::thread(guardian, std::ref(pop), std::ref(segment_pool)).detach();
        // std::thread(guardian, std::ref(pop), std::ref(segment_pool)).detach();
#endif
    }

    FunctionStatus HashTable::Put(PoolBase &pop, Stats &stats, KVPairPtr pair, const std::string &key, const std::string &value) noexcept
    {
        auto hv = HashValue(std::hash<std::string>{}(key));

        // make sure there is no doubling thread
    RETRY:

        // directory doubling concurrency control
        if (to_double)
        {
            goto RETRY;
        }

        //reader_lock.lock_shared();
        ++readers;

        // starts trying put
        auto pos = hv.SegmentBits(depth);
        auto seg = dir.GetSegment(pos);
        auto bkt = &seg->buckets[hv.BucketBits()];

#ifdef LOGGING
        std::stringstream log_buf;
        log_buf << "first putting " << key << "(" << std::hex << hv.GetRaw() << std::dec << ")"
                << " to (" << seg->segment_no << ", " << hv.BucketBits() << "), global depth: "
                << uint64_t(depth) << ", bucket depth: " << uint64_t(bkt->metainfo.local_depth) << "\n";
        Log(log_buf);
#endif
        auto ans = bkt->GetAncestor();
        if (ans.has_value())
        {
            seg = dir.GetSegment(ans.value());
            bkt = &seg->buckets[hv.BucketBits()];
#ifdef LOGGING
            log_buf << ">#< changing putting " << key << "(" << std::hex << hv.GetRaw() << std::dec << ")"
                    << " to (" << seg->segment_no << ", " << hv.BucketBits() << "), global depth: " << uint64_t(depth) << "\n";
            Log(log_buf);
#endif
        }
#ifdef LOGGING
        else
        {
            log_buf << ">#< no ancestor detected\n";
            Log(log_buf);
        }
#endif
        if (!bkt->TryLock())
        {
            // reader_lock.unlock_shared();
            --readers;
            goto RETRY;
        }
#ifdef LOGGING
        auto ret = bkt->Put(logger, pop, pair, key, value, hv, seg->segment_no);
#else
        auto ret = bkt->Put(pop, pair, key, value, hv, seg->segment_no);
#endif
        switch (ret)
        {
        case FunctionStatus::Retry:
        {
#ifdef LOGGING
            std::stringstream buf;
            buf << "Reader lock unlocked "
                << " (" << seg->segment_no << ", " << hv.BucketBits() << ") in Retry\n";
            Log(buf);
#endif
        }
            bkt->Unlock();
            //reader_lock.unlock_shared();
            --readers;
            goto RETRY;
        case FunctionStatus::SplitRequired:
        {
            // lock is acquired inside split depending on global depth
            split(pop, stats, *bkt, hv, seg, seg->segment_no);

#ifdef LOGGING
            std::stringstream buf;
            buf << "Reader lock unlocked "
                << " (" << seg->segment_no << ", " << hv.BucketBits() << ") in SplitRequired\n";
            Log(buf);
#endif
        }
            bkt->Unlock();
            // reader_lock.unlock_shared();
            --readers;
            goto RETRY;
        case FunctionStatus::FlattenRequired:
        {
            // fall through
        }
        default:
        {
#ifdef LOGGING
            std::stringstream buf;
            buf << "Reader lock unlocked "
                << " (" << seg->segment_no << ", " << hv.BucketBits() << ") in Default\n";
            Log(buf);
#endif
        }
            bkt->Unlock();
            // reader_lock.unlock_shared();
            --readers;
            ++loaded;
            return ret;
        }
    }

    KVPairPtr HashTable::Get(const std::string &key) const noexcept
    {
        auto hv = HashValue(std::hash<std::string>{}(key));
        auto seg = dir.GetSegment(hv, depth);
        auto bkt = &seg->buckets[hv.BucketBits()];
#ifdef LOGGING
        std::stringstream log_buf;
        log_buf << "Searching " << key << ": (" << seg->segment_no << ", " << hv.BucketBits() << ")\n";
        Log(log_buf);
#endif
        auto ans = bkt->GetAncestor();
        if (ans.has_value())
        {
            seg = dir.GetSegment(ans.value());
            bkt = &seg->buckets[hv.BucketBits()];
#ifdef LOGGING
            log_buf << "Searching " << key << ": (" << seg->segment_no << ", " << hv.BucketBits() << ")\n";
            Log(log_buf);
#endif
        }
#ifdef LOGGING
        logger.Write(log_buf.str());
#endif
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
                    std::cout << std::setw(4) << dir.GetSegment(i)->buckets[j].GetAncestor().value() << " ";
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
            meta_buf << std::setw(2) << i << " ";
        }
        meta_buf << std::endl;

        for (uint64_t i = 0; i < (1UL << depth); i++)
        {
            meta_buf << std::setw(2) << dir.GetSegment(i)->segment_no << " ";
        }
        meta_buf << std::endl;

        for (uint64_t j = 0; j < SEG_SIZE; j++)
        {
            for (uint64_t i = 0; i < (1UL << depth); i++)
            {
                if (dir.GetSegment(i)->buckets[j].HasAncestor())
                {
                    meta_buf << std::setw(2) << dir.GetSegment(i)->buckets[j].GetAncestor().value() << " ";
                }
                else
                {
                    meta_buf << "   ";
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

    void HashTable::Log(std::string msg) const
    {
        logger.Write(msg);
    }

    void HashTable::Log(std::string &msg) const
    {
        logger.Write(msg);
    }

    void HashTable::Log(std::stringstream &msg_s) const
    {
        logger.Write(msg_s.str());
        msg_s.str("");
    }

    /*
     * bucket bkt is already locked if this method is called
     */
    void HashTable::split(PoolBase &pop, Stats &stats, Bucket &bkt, const HashValue &hv, SegmentPtr &seg, uint64_t segno) noexcept
    {
#ifdef LOGGING
        Log(">>>> entering split\n");
#endif
        auto local_depth = bkt.GetDepth();
        if (local_depth < depth)
        {
            // this function would decides if a simple split or a traditional split is required
            traditional_split(pop, stats, bkt, hv, segno, false);
        }
        else
        {
#ifdef LOGGING
            Log(">>>> to complex split ");
#endif
            auto start = std::chrono::steady_clock::now();
            auto expected = false;
            to_double.compare_exchange_strong(expected, true);
            if (expected)
            {
                return;
            }

            // complex split thread is the only reader;
            while (readers != 1)
                ;

            if (!doubling_lock.try_lock())
            {
#ifdef LOGGING
                Log(">>>> leaving split\n");
#endif
                return;
            }
            auto wait_end = std::chrono::steady_clock::now();
            std::cout << "complex waited for(ns): " << (wait_end - start).count() << "\n";
            complex_split(pop, stats, bkt, hv, seg, segno);
            // these two lines are executed inside compelx_split for fine-grained concurrency
            // doubling_lock.unlock();
            // to_double = false;
            auto end = std::chrono::steady_clock::now();
            std::cout << "complex split consumed(ns) in total: " << (end - start).count() << "\n";
        }
#ifdef LOGGING
        Log(">>>> leaving split\n");
#endif
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
    void HashTable::simple_split(PoolBase &pop, Stats &stats, uint64_t root_segno, uint64_t buddy_segno, Bucket &bkt, uint64_t bktbits) noexcept
    {
        stats.simple_splits++;
        auto start = std::chrono::steady_clock::now();
#ifdef LOGGING
        Log(">>>> entering simple_split\n");
#endif

        auto buddy_seg = dir.GetSegment(buddy_segno);
        auto buddy_bkt = &buddy_seg->buckets[bktbits];

        // dead lock is impossible
        // first step: kv migration
        buddy_bkt->Lock();
        bkt.Migrate(pop, *buddy_bkt, root_segno);
        buddy_bkt->SetMetaPersist(pop, bkt.GetDepth(), 0, (1UL << 49));

        // second step: bucket linking
        auto current_depth = bkt.GetDepth();
        auto difference = depth - current_depth;
        auto walk = buddy_segno;
        SegmentPtr walk_ptr;
        for (int i = 1; i < (1 << difference); i++)
        {
            walk += (1UL << current_depth);
            // dir.LockSegment(walk);
            walk_ptr = dir.LockSegment(walk);
            if (walk_ptr->segment_no != walk)
            {
                // a reference pointer pointing to one ancestor
                // dir.SetSegment(pop, buddy_seg, walk);
                capacity += SEG_SIZE * BUCKET_SIZE;
                SegmentPtr pre_seg = nullptr;
#ifndef PREALLOCATION
                TX::run(pop, [&]() {
                    pre_seg = pobj::make_persistent<Segment>(pop, bkt.GetDepth(), walk, true);
                });
#else
                if ((pre_seg = segment_pool.Pop()) == nullptr)
                {
                    std::cout << "empty pool in simple\n";
                    TX::run(pop, [&]() {
                        pre_seg = pobj::make_persistent<Segment>(pop, bkt.GetDepth(), walk, true).get();
                    });
                }
                else
                {
                    pre_seg->segment_no = walk;
                }
#endif
#ifdef LOGGING
                std::stringstream log;
                log << ">>>> creating new segment " << walk << " which connects to "
                    << walk_ptr->segment_no.get_ro() << "\n";
                Log(log);
#endif
                for (int i = 0; i < SEG_SIZE; i++)
                {
                    auto ans = walk_ptr->buckets[i].HasAncestor() ? walk_ptr->buckets[i].GetAncestor().value() : walk_ptr->segment_no.get_ro();
                    pre_seg->buckets[i].SetAncestor(ans);
#ifdef LOGGING
                    if (walk_ptr->buckets[i].HasAncestor())
                    {
                        log << "(" << walk_ptr->segment_no.get_ro() << ", " << i << ")has ancestor "
                            << ans << "\n";
                        Log(log);
                    }
                    else
                    {
                        log << "(" << walk_ptr->segment_no.get_ro() << ", " << i
                            << ")has no ancestor, connecting to " << walk_ptr->segment_no.get_ro()
                            << "\n";
                        Log(log);
                    }
#endif
                }
                pre_seg->buckets[bktbits].SetAncestor(buddy_bkt->HasAncestor() ? buddy_bkt->GetAncestor().value() : buddy_segno);
                TX::run(pop, [&]() {
                    dir.AddSegment(pop, pre_seg, walk);
                });
                dir.UnlockSegment(walk);
                continue;
            }
            dir.UnlockSegment(walk);

            // buddy_segno will never be modified, no lock is needed
            if (walk_ptr == dir.GetSegment(buddy_segno))
            {
                /*
                 * buddy bkt has uninitialized descendents
                 * since this buddy bkt hasn't splitted yet, all the descendents would be pointing
                 * back to this buddy bkt, thus break here to save some execution time
                 */
                break;
            }
            walk_ptr->buckets[bktbits].Lock();
            walk_ptr->buckets[bktbits].SetAncestorPersist(pop, buddy_segno);
            walk_ptr->buckets[bktbits].Unlock();
            auto end = std::chrono::steady_clock::now();
            stats.simple_split_time += (end - start).count();
        }
        /*
         * unlock buddy bucket here to prevent chaos: buddy bucket is filled up when
         * splitting bucket is connecting descendants of buddy bucket to it. 
         */
        buddy_bkt->Unlock();
#ifdef LOGGING
        Log(">>>> leaving simple_split\n");
#endif
    }

    /*
     * This is actually a combination of simple split and traditional split in paper
     * it decides whether to allocate or not, if allocating, then a traditional split is done
     * if not, a simple split is enough.
     */
    void HashTable::traditional_split(PoolBase &pop, Stats &stats,  Bucket &bkt, const HashValue &hv, uint64_t segno, bool helper) noexcept
    {
#ifdef LOGGING
        Log("entering traditional_split\n");
#endif
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

        // traditional split is combined here
        auto buddy = dir.LockSegment(buddy_segno);
        if (root == buddy)
        {
            make_buddy_segment(pop, root, segno, buddy_segno, bkt);
            stats.traditional_splits++;
            stats.simple_splits--;
            capacity += SEG_SIZE * BUCKET_SIZE;
        }
        dir.UnlockSegment(buddy_segno);

        simple_split(pop, stats, root_segno, buddy_segno, bkt, bktbits);
#ifdef LOGGING
        Log("leaving traditional_split\n");
#endif
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
    void HashTable::complex_split(PoolBase &pop, Stats &stats, Bucket &bkt, const HashValue &hv, SegmentPtr &seg, uint64_t segno) noexcept
    {
        stats.complex_splits++;
#ifdef LOGGING
        Log("entering complex_split\n");
#endif
#ifdef TIMING
        std::cout << "Doing a complex split\n";
        auto start = std::chrono::steady_clock::now();
#endif
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
#ifdef TIMING
        auto d_start = std::chrono::steady_clock::now();
#endif
        dir.DoublingLink(pop, depth, depth + 1);
#ifdef TIMING
        auto d_end = std::chrono::steady_clock::now();
        std::cout << "DoublingLink: " << (d_end - d_start).count() << "\n";
#endif
        auto root = dir.GetSegment(root_segno);

        // if (root == dir.GetSegment(buddy_segno))
        // {
#ifdef TIMING
        d_start = std::chrono::steady_clock::now();
#endif
        ++depth;
        pmemobj_persist(pop.handle(), &depth, sizeof(depth));
        dir.LockSegment(buddy_segno);
        doubling_lock.unlock();
        to_double = false;
        buddy = make_buddy_segment(pop, root, segno, buddy_segno, bkt);
#ifdef TIMING
        d_end = std::chrono::steady_clock::now();
        std::cout << "MakeBuddySegemnt: " << (d_end - d_start).count() << "\n";
#endif
        // }

        // traditional_split(pop, bkt, hv, segno, true);
#ifdef TIMING
        d_start = std::chrono::steady_clock::now();
#endif
        simple_split(pop, stats, root_segno, buddy_segno, bkt, hv.BucketBits());
#ifdef TIMING
        d_end = std::chrono::steady_clock::now();
        std::cout << "SimpleSplit: " << (d_end - d_start).count() << "\n";
#endif
        buddy->status = SegStatus::Quiescent;
        pmemobj_persist(pop.handle(), &buddy->status, sizeof(SegStatus));
        bkt.ClearSplit();
        dir.UnlockSegment(buddy_segno);
#ifdef TIMING
        auto end = std::chrono::steady_clock::now();
        std::cout << "Finishing a complex split\n";
        std::cout << "Time consumed(ns): " << (end - start).count() << "\n";
#endif
#ifdef LOGGING
        Log("leaving complex_split\n");
#endif
    }

    /*
     * NOT REQUIRED FOR DALEA NOW
     */
    void HashTable::flatten_bucket(PoolBase &pop, Bucket &bkt, const HashValue &hv, uint64_t segno) noexcept
    {
    }

    SegmentPtr HashTable::make_buddy_segment(PoolBase &pop, const SegmentPtr &root, uint64_t segno, uint64_t buddy_segno, const Bucket &bkt) noexcept
    {
#ifdef LOGGING
        std::stringstream log;
        log << ">>>> creating new segment " << buddy_segno << "\n";
        Log(log);
#endif
        SegmentPtr buddy = nullptr;
#ifdef TIMING
        auto start = std::chrono::steady_clock::now();
#endif

#ifndef PREALLOCATION
        TX::run(pop, [&]() {
            buddy = pobj::make_persistent<Segment>(pop, bkt.GetDepth(), buddy_segno, true);
        });
#else
        if ((buddy = segment_pool.Pop()) == nullptr)
        {
            std::cout << "empty pool in traditional\n";
            TX::run(pop, [&]() {
                buddy = pobj::make_persistent<Segment>(pop, bkt.GetDepth(), buddy_segno, true).get();
            });
        }
        else
        {
            buddy->segment_no = buddy_segno;
        }
#endif
#ifdef TIMING
        auto end = std::chrono::steady_clock::now();
        std::cout << "Allocating and initiailize new segment: " << (end - start).count() << "\n";
        start = std::chrono::steady_clock::now();
#endif
        for (int i = 0; i < SEG_SIZE; i++)
        {
            // do not persist here
            buddy->buckets[i].SetDepth(bkt.GetDepth());
            if (root->buckets[i].HasAncestor())
            {
                // avoid chaining
                buddy->buckets[i].SetAncestor(root->buckets[i].GetAncestor().value());
#ifdef LOGGING
                log << "(" << root->segment_no.get_ro() << ", " << i << ") has ancestor "
                    << root->buckets[i].GetAncestor().value() << "\n";
                Log(log);
#endif
            }
            else
            {
                buddy->buckets[i].SetAncestor(segno);
#ifdef LOGGING
                log << "(" << root->segment_no.get_ro() << ", " << i << ") has no ancestor "
                    << "connecting to " << segno << "\n";
                Log(log);
#endif
            }
        }
#ifdef TIMING
        end = std::chrono::steady_clock::now();
        std::cout << "Initiailize buckets in new segment: " << (end - start).count() << "\n";

        start = std::chrono::steady_clock::now();
#endif
        TX::run(pop, [&]() {
            dir.AddSegment(pop, buddy, buddy_segno);
        });
#ifdef TIMING
        end = std::chrono::steady_clock::now();
        std::cout << "nAdding segment: " << (end - start).count() << "\n";
#endif
        return buddy;
    }
} // namespace Dalea
