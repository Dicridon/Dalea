#include "Bucket.hpp"
namespace Dalea
{
    Bucket::Bucket()
    {
        memset(&metainfo, 0, 8 * sizeof(BucketMeta));
        for (int i = 0; i < BUCKET_SIZE; i++)
        {
            HashValue hv;
            hv.Invalidate();
            fingerprints[i] = hv;
            pairs[i] = nullptr;
        }
#ifndef PLOCK
        mux = new std::shared_mutex;
#endif
    }

    FunctionStatus Bucket::Get(const String &key, const HashValue &hash_value, KVPairPtr &ret, uint64_t segno) const noexcept
    {
#ifdef PLOCK
        std::shared_lock s(mux);
#else
        std::shared_lock s(*mux);
#endif
        // auto encoding = hash_value.GetRaw() & (((1UL << GetDepth()) - 1));
        auto mask = ((1UL << GetDepth()) - 1);
        auto encoding = hash_value.GetRaw() & mask; // (((1UL << GetDepth()) - 1));
        auto tag = segno & mask;
        /* 
         * access to a splitting bucket and then obtained a lock, however the split has finished
         * tag may have changed
         */
        if (tag != encoding)
        {
            return FunctionStatus::Retry;
        }

        for (auto search = 0; search < BUCKET_SIZE; search++)
        {
            // auto f = fingerprints[search].GetRaw() & (((1UL << GetDepth()) - 1));
            // if (f != encoding)
            // {
            //     // no need to persist
            //     fingerprints[search].Invalidate();
            // }
#ifdef USE_FP
            if (fingerprints[search] == hash_value)
            {
                if (pairs[search]->key == key)
#else
            if (pairs[search] && pairs[search]->key == key)
#endif

                {
                    // return pairs[search];
                    ret = pairs[search];
                    return FunctionStatus::Ok;
                }
#ifdef USE_FP
            }
#endif
        }

        return FunctionStatus::Failed;
    }

    FunctionStatus Bucket::Put(PoolBase &pop, const String &key, const String &value, const HashValue &hash_value, uint64_t segno) noexcept
    {
        // if (HasAncestor())
        // {
        //     return FunctionStatus::FlattenRequired;
        // }

        int slot = -1;
        auto mask = ((1UL << GetDepth()) - 1);
        auto encoding = hash_value.GetRaw() & mask; // (((1UL << GetDepth()) - 1));
        auto tag = segno & mask;
        /* 
         * access to a splitting bucket and then obtained a lock, however the split has finished
         * tag may have changed
         */
        if (tag != encoding)
        {
            return FunctionStatus::Retry;
        }

        for (auto search = 0; search < BUCKET_SIZE; search++)
        {
            // the later condition is used for lazy deletion, also postpone splitting as much as possible
            auto f = fingerprints[search].GetRaw() & (((1UL << GetDepth()) - 1));
            if (fingerprints[search].IsInvalid() || f != encoding)
            {
                slot = search;
            }
#ifdef USE_FP
            if (fingerprints[search] == hash_value)
            {
                if (pairs[search]->key == key && pairs[search]->value != value)
#else
            if (pairs[search] && pairs[search]->key == key && pairs[search]->value != value)
#endif
                {
                    // std::cout << "dup key: " << key << "\n";
                    // Not a good update strategy
                    TX::run(pop, [&]() {
                        // replace_content would be called
                        pairs[search]->value = value;
                    });
                    return FunctionStatus::Ok;
                }
#ifdef USE_FP
                else
                {
                    return FunctionStatus::Failed;
                }
            }
#endif
        }
        if (slot == -1)
        {
            return FunctionStatus::SplitRequired;
        }
        KVPairPtr pair = nullptr;
        TX::run(pop, [&]() {
            pair = pmem::obj::make_persistent<KVPair>(
                key,
                value);
        });
        pairs[slot] = pair;
        fingerprints[slot] = hash_value;
        pmemobj_persist(pop.handle(), pairs + slot, sizeof(KVPair));
        pmemobj_persist(pop.handle(), fingerprints + slot, sizeof(uint64_t));
        return FunctionStatus::Ok;
    }

    FunctionStatus Bucket::Put(Logger &logger, PoolBase &pop, const String &key, const String &value, const HashValue &hash_value, uint64_t segno) noexcept
    {
        if (HasAncestor())
        {
            std::stringstream buf;
            buf << "Ancestor detected " << GetAncestor() << " in (" << segno << ", "
                << hash_value.BucketBits() << ")\n";
            logger.Write(buf.str());
            return FunctionStatus::FlattenRequired;
        }

        int slot = -1;
        auto mask = ((1UL << GetDepth()) - 1);
        auto encoding = hash_value.GetRaw() & mask; // (((1UL << GetDepth()) - 1));
        auto tag = segno & mask;
        /* 
         * access to a splitting bucket and then obtained a lock, however the split has finished
         * tag may have changed
         */
        if (tag != encoding)
        {
            return FunctionStatus::Retry;
        }

        for (auto search = 0; search < BUCKET_SIZE; search++)
        {
            auto iter = std::to_string(search);
            // the later condition is used for lazy deletion, also postpone splitting as much as possible
            auto f = fingerprints[search].GetRaw() & (((1UL << GetDepth()) - 1));
            if (fingerprints[search].IsInvalid() || f != encoding)
            {
                slot = search;
            }
            if (fingerprints[search] == hash_value)
            {
                if (pairs[search]->key == key)
                {
                    TX::manual tx(pop);
                    pairs[search]->value = value;
                    TX::commit();
                    return FunctionStatus::Ok;
                }
            }
        }
        if (slot == -1)
        {
            return FunctionStatus::SplitRequired;
        }
        TX::run(pop, [&]() {
            auto pair = pmem::obj::make_persistent<KVPair>(
                key,
                value);
            pairs[slot] = pair;
            fingerprints[slot] = hash_value;
        });
#ifdef LOGGING
        std::string msg{">>>> putting finished\n"};
        logger.Write(msg);
#endif
        return FunctionStatus::Ok;
    }

    FunctionStatus Bucket::Remove(const String &key, const HashValue &hash_value, std::shared_mutex &mux) const noexcept
    {
        return FunctionStatus::Ok;
    }

    void Bucket::Lock() noexcept
    {
#ifdef PLOCK
        mux.lock();
#else
        mux->lock();
#endif
    }

    bool Bucket::TryLock() noexcept
    {
#ifdef PLOCK
        return mux.try_lock();
#else
        return mux->try_lock();
#endif
    }

    void Bucket::Unlock() noexcept
    {
#ifdef PLOCK
        mux.unlock();
#else
        mux->unlock();
#endif
    }

    void Bucket::LockShared() noexcept
    {
#ifdef PLOCK
        mux.lock_shared();
#else
        mux->lock_shared();
#endif
    }

    bool Bucket::TryLockShared() noexcept
    {
#ifdef PLOCK
        return mux.try_lock_shared();
#else
        return mux->try_lock_shared();
#endif
    }

    void Bucket::UnlockShared() noexcept
    {
#ifdef PLOCK
        return mux.unlock_shared();
#else
        return mux->unlock_shared();
#endif
    }

    bool Bucket::HasAncestor() const noexcept
    {
        return metainfo.has_ancestor;
    }

    void Bucket::SetAncestor(int64_t an) noexcept
    {
        metainfo.has_ancestor = 1;
        metainfo.ancestor = an;
    }

    void Bucket::SetAncestorPersist(PoolBase &pop, int64_t an) noexcept
    {
        auto tmp = metainfo;
        tmp.has_ancestor = 1;
        tmp.ancestor = an;
        metainfo = tmp;
        pmemobj_persist(pop.handle(), &metainfo, sizeof(BucketMeta));
    }

    uint64_t Bucket::GetAncestor() const noexcept
    {
        auto tmp = metainfo;
        return tmp.ancestor;
    }

    void Bucket::ClearAncestor() noexcept
    {
        metainfo.has_ancestor = 1;
    }

    void Bucket::ClearAncestorPersist(PoolBase &pop) noexcept
    {
        metainfo.has_ancestor = 0;
        pmemobj_persist(pop.handle(), &metainfo, sizeof(BucketMeta));
    }

    void Bucket::SetDepth(uint8_t depth) noexcept
    {
        metainfo.local_depth = depth;
    }

    void Bucket::SetDepthPersist(PoolBase &pop, uint8_t depth) noexcept
    {
        metainfo.local_depth = depth;
        pmemobj_persist(pop.handle(), &metainfo, sizeof(BucketMeta));
    }

    void Bucket::IncDepth() noexcept
    {
        metainfo.local_depth += 1;
    }

    void Bucket::IncDepthPersist(PoolBase &pop) noexcept
    {
        metainfo.local_depth += 1;
        pmemobj_persist(pop.handle(), &metainfo, sizeof(BucketMeta));
    }

    uint8_t Bucket::GetDepth() const noexcept
    {
        return metainfo.local_depth;
    }

    void Bucket::SetSplit() noexcept
    {
        metainfo.split_flag = 1;
    }

    void Bucket::SetSplitPersist(PoolBase &pop) noexcept
    {
        metainfo.split_flag = 1;
        pmemobj_persist(pop.handle(), &metainfo, sizeof(BucketMeta));
    }

    bool Bucket::IsSplitting() const noexcept
    {
        return metainfo.split_flag;
    }

    void Bucket::ClearSplit() noexcept
    {
        metainfo.split_flag = 0;
    }

    void Bucket::ClearSplitPersist(PoolBase &pop) noexcept
    {
        metainfo.split_flag = 0;
        pmemobj_persist(pop.handle(), &metainfo, sizeof(BucketMeta));
    }

    void Bucket::SetMetaPersist(PoolBase &pop, uint8_t dep, uint8_t split, uint64_t an) noexcept
    {
        auto tmp = metainfo;
        tmp.local_depth = dep;
        tmp.split_flag = split;
        if (an >= ((1UL << 48) - 1))
        {
            tmp.has_ancestor = 0;
        }
        else
        {
            tmp.has_ancestor = 1;
            tmp.ancestor = an;
        }
        metainfo = tmp;
        pmemobj_persist(pop.handle(), &metainfo, sizeof(BucketMeta));
    }

    void Bucket::UpdateSplitMetaPersist(PoolBase &pop) noexcept
    {
        auto tmp = metainfo;
        ++tmp.local_depth;
        tmp.split_flag = 1;
        metainfo = tmp;
        pmemobj_persist(pop.handle(), &metainfo, sizeof(BucketMeta));
    }

    void Bucket::Migrate(PoolBase &pop, Bucket &buddy, uint64_t encoding) noexcept
    {
        uint64_t mask = (1UL << GetDepth()) - 1;
        for (int i = 0; i < BUCKET_SIZE; i++)
        {
#ifdef USE_FP
            if (!fingerprints[i].IsInvalid())
            {
                if ((fingerprints[i].GetRaw() & mask) != encoding)
                {
#else
            if (pairs[i])
            {
                auto hv = std::hash<std::string>{}(std::string(pairs[i]->key.c_str()));
                if ((hv & mask) != encoding)
                {
#endif
                    // buddy bucket is ensured to be empty
                    buddy.fingerprints[i] = fingerprints[i];
                    buddy.pairs[i] = pairs[i];
                    pmemobj_persist(pop.handle(), buddy.pairs + i, sizeof(KVPairPtr));
                    pmemobj_persist(pop.handle(), buddy.fingerprints + i, sizeof(HashValue));
                    fingerprints[i].Invalidate();
                    pairs[i] = nullptr;
                }
            }
        }
        buddy.ClearAncestorPersist(pop);
    }

    void Bucket::PersistMeta(PoolBase &pop) const noexcept
    {
        pmemobj_persist(pop.handle(), &metainfo, 8 * sizeof(uint8_t));
    }

    void Bucket::PersistFingerprints(PoolBase &pop, int index) const noexcept
    {
        pmemobj_persist(pop.handle(), &fingerprints[index], sizeof(HashValue));
    }

    void Bucket::PersistAncestor(PoolBase &pop) const noexcept
    {
        pmemobj_persist(pop.handle(), &metainfo, sizeof(uint64_t));
    }

    void Bucket::PersistAll(PoolBase &pop) const noexcept
    {
        pmemobj_persist(pop.handle(), this, sizeof(Bucket));
    }

    void Bucket::Debug(uint64_t tag) const noexcept
    {
        std::cout << "    [[ Bucket " << tag << " reporting\n";
        std::cout << "       depth: " << uint64_t(GetDepth()) << "\n";
        std::cout << "       keys: \n";
        std::for_each(std::begin(pairs), std::end(pairs), [&](const KVPairPtr &kvp) {
            if (kvp != nullptr)
            {
                std::cout << "       >> " << kvp->key.c_str() << "\n";
            }
        });

        if (HasAncestor())
        {
            std::cout << "          ancestor: " << GetAncestor() << "\n";
        }
    }

    void Bucket::DebugTo(std::stringstream &strm, uint64_t tag) const noexcept
    {
        strm << "    [[ Bucket " << tag << " reporting\n";
        strm << "       depth: " << uint64_t(GetDepth()) << "\n";
        strm << "       keys: \n";
        std::for_each(std::begin(pairs), std::end(pairs), [&](const KVPairPtr &kvp) {
            if (kvp != nullptr)
            {
                strm << "       >> " << kvp->key.c_str() << "\n";
            }
        });

        if (HasAncestor())
        {
            strm << "          ancestor: " << GetAncestor() << "\n";
        }
    }

} // namespace Dalea
