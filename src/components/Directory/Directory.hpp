#ifndef __DALEA__DIRECTORY__DIRECTORY__
#define __DALEA__DIRECTORY__DIRECTORY__
#include <libpmemobj++/container/array.hpp>

#include "Segment/Segment.hpp"

namespace Dalea
{
    struct Directory
    {
    private:
        struct SubDirectory
        {
            SubDirectory(PoolBase &pop);

            SubDirectory() = default;
            SubDirectory(const SubDirectory &) = delete;
            SubDirectory(SubDirectory &&) = delete;

            pobj::array<SegmentPtr, SUBDIR_SIZE> segments;
            std::shared_mutex *mutexes;
        };

        using SubDirectoryPtr = pobj::persistent_ptr<SubDirectory>;
        struct MetaDirectory
        {
            MetaDirectory(PoolBase &pop);

            MetaDirectory() = default;
            MetaDirectory(const SubDirectory &) = delete;
            MetaDirectory(SubDirectory &&) = delete;

            pobj::array<SubDirectoryPtr, METADIR_SIZE> subdirectories;
        };

    public:
        Directory(PoolBase &pop) : meta(pop){};

        Directory() = delete;
        Directory(const SubDirectory &) = delete;
        Directory(SubDirectory &&) = delete;

        const SegmentPtr &GetSegment(uint64_t pos) const noexcept;
        const SegmentPtr &GetSegment(const HashValue &hv, uint64_t depth) const noexcept;

        void LockSegment(uint64_t pos) noexcept;
        void LockSegmentShared(uint64_t pos) noexcept;
        void TryLockSegment(uint64_t pos) noexcept;
        void TryLockSegmentShared(uint64_t pos) noexcept;
        void UnlockSegment(uint64_t pos) noexcept;
        void UnlockSegmentShared(uint64_t pos) noexcept;
        
        bool AddSegment(PoolBase &pop, const SegmentPtr &ptr, uint64_t pos) noexcept;
        bool SetSegment(PoolBase &pop, const SegmentPtr &ptr, uint64_t pos) noexcept;
        bool Probe(uint64_t pos) const noexcept;
        bool Probe(const HashValue &hv) const noexcept;
        void DoublingLink(PoolBase &pop, uint64_t prev_depth, uint64_t new_depth) noexcept;

        MetaDirectory meta;
    };
} // namespace Dalea
#endif
