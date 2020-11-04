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
    }
    KVPairPtr Bucket::Get(const String &key, const HashValue &hash_value, std::shared_mutex &mux) const noexcept
    {
        std::shared_lock shr(mux);
        auto encoding = hash_value.GetRaw() & (((1UL << GetDepth()) - 1));
        for (auto search = 0; search < BUCKET_SIZE; search++)
        {
            auto f = fingerprints[search].GetRaw() & (((1UL << GetDepth()) - 1));
            if (f != encoding)
            {
                // no need to persist
                fingerprints[search].IsInvalid();
            }
            if (fingerprints[search] == hash_value)
            {
                if (pairs[search]->key == key)
                {
                    return pairs[search];
                }
            }
        }

        return nullptr;
    }

    FunctionStatus Bucket::Put(PoolBase &pop, const String &key, const String &value, const HashValue &hash_value, std::shared_mutex &mux, uint64_t segno) noexcept
    {
        if (HasAncestor())
        {
            return FunctionStatus::FlattenRequired;
        }

        std::unique_lock exc(mux);
        int slot = -1;
        auto encoding = hash_value.GetRaw() & (((1UL << GetDepth()) - 1));
        for (auto search = 0; search < BUCKET_SIZE; search++)
        {
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
        return FunctionStatus::Ok;
    }

    FunctionStatus Bucket::Remove(const String &key, const HashValue &hash_value, std::shared_mutex &mux) const noexcept
    {
        return FunctionStatus::Ok;
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

    int64_t Bucket::GetAncestor() const noexcept
    {
        if (!HasAncestor())
        {
            return -1;
        }
        return metainfo.ancestor;
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

    void Bucket::Migrate(Bucket &buddy, uint64_t encoding) noexcept
    {
        uint64_t mask = (1UL << GetDepth()) - 1;
        for (int i = 0; i < BUCKET_SIZE; i++)
        {
            if (!fingerprints[i].IsInvalid())
            {
                if ((fingerprints[i].GetRaw() & mask) != encoding)
                {
                    // buddy bucket is ensured to be empty
                    buddy.fingerprints[i] = fingerprints[i];
                    buddy.pairs[i] = pairs[i];
                }
            }
        }
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
            if (kvp != nullptr) {
                std::cout << "       >> " << kvp->key.c_str() << "\n";
            }
        });

        if (HasAncestor()) {
            std::cout << "          ancestor: " << GetAncestor() << "\n";
        }
    }

} // namespace Dalea
