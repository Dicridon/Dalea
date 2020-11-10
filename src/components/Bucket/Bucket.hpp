#ifndef __DALEA__BUCKET__BUCKET__
#define __DALEA__BUCKET__BUCKET__
#include "KVPair/KVPair.hpp"

#include <shared_mutex>
namespace Dalea
{
    struct Bucket
    {
        Bucket();
        Bucket(const Bucket &) = delete;
        Bucket(Bucket &&) = delete;

        /*
         * only when pairs are all invalid can a Bucket be freed
         */
        ~Bucket() = default;

        KVPairPtr Get(const String &key, const HashValue &hash_value) const noexcept;
        FunctionStatus Put(PoolBase &pop, const String &key, const String &value, const HashValue &hash_value, uint64_t segno) noexcept;
        FunctionStatus Remove(const String &key, const HashValue &hash_value, std::shared_mutex &mux) const noexcept;

        void Lock() noexcept;
        bool TryLock() noexcept;
        void Unlock() noexcept;
        void LockShared() noexcept;
        bool TryLockShared() noexcept;
        void UnlockShared() noexcept;

        bool HasAncestor() const noexcept;
        void SetAncestor(int64_t an) noexcept;
        void SetAncestorPersist(PoolBase &pop, int64_t an) noexcept;
        uint64_t GetAncestor() const noexcept;
        void ClearAncestor() noexcept;
        void ClearAncestorPersist(PoolBase &pop) noexcept;

        void SetDepth(uint8_t depth) noexcept;
        void SetDepthPersist(PoolBase &pop, uint8_t depth) noexcept;
        uint8_t GetDepth() const noexcept;
        void IncDepth() noexcept;
        void IncDepthPersist(PoolBase &pop) noexcept;

        void SetSplit() noexcept;
        void SetSplitPersist(PoolBase &pop) noexcept;
        bool IsSplitting() const noexcept;
        void ClearSplit() noexcept;
        void ClearSplitPersist(PoolBase &pop) noexcept;

        // set local depth, split byte and ancestor with one single store and persit
        void SetMetaPersist(PoolBase &pop, uint8_t dep, uint8_t split, uint64_t an) noexcept;

        // first: update metadata
        void UpdateSplitMetaPersist(PoolBase &pop) noexcept;
        // second: migrate pairs
        void Migrate(PoolBase &pop, Bucket &buddy, uint64_t encoding) noexcept;

        void PersistMeta(PoolBase &pop) const noexcept;
        void PersistFingerprints(PoolBase &pop, int index) const noexcept;
        void PersistPairs(PoolBase &pop) const noexcept;
        void PersistAncestor(PoolBase &pop) const noexcept;
        void PersistAll(PoolBase &pop) const noexcept;

        void Debug(uint64_t tag) const noexcept;
        void DebugTo(std::stringstream &strm, uint64_t tag) const noexcept;

    private:
        /*
         * metainfo:
         *     local depth: 1 byte
         *     split flag:  1 byte
         *     ancestor:    6 byte
         */
        struct BucketMeta
        {
            uint64_t local_depth : 8;
            uint64_t split_flag : 1;
            uint64_t has_ancestor : 1;
            uint64_t : 6;
            uint64_t ancestor : 48;
        };

    public:
        /* 
         * 256Byte in total
         */
        BucketMeta metainfo;
        HashValue fingerprints[BUCKET_SIZE];
        KVPairPtr pairs[BUCKET_SIZE];
        // the index of ancestor segment in directory
        // int64_t padding;
        std::shared_mutex *mux;
    };
} // namespace Dalea
#endif
