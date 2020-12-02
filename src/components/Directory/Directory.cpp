#include "Directory.hpp"
namespace Dalea
{
    Directory::MetaDirectory::MetaDirectory(PoolBase &pop)
    {
        TX::run(pop, [&]() {
            subdirectories[0] = pobj::make_persistent<Directory::SubDirectory>(pop).get();
            pop.persist(subdirectories[0], sizeof(Directory::SubDirectory));
        });
        for (int i = 1; i < METADIR_SIZE; i++)
        {
            subdirectories[i] = nullptr;
        }
    }

    Directory::SubDirectory::SubDirectory(PoolBase &pop)
    {
        TX::run(pop, [&]() {
            segments[0] = pobj::make_persistent<Segment>(pop, 1, 0, false).get();
            segments[1] = pobj::make_persistent<Segment>(pop, 1, 1, false).get();
            pop.persist(segments[0], sizeof(Segment));
            pop.persist(segments[1], sizeof(Segment));
        });

        for (int i = 2; i < SUBDIR_SIZE; i++)
        {
            segments[i] = nullptr;
        }
        mutexes = new std::shared_mutex[SUBDIR_SIZE];
    }

    const SegmentPtr &Directory::GetSegment(uint64_t pos) const noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        return meta.subdirectories[sub]->segments[seg];
    }

    const SegmentPtr &Directory::GetSegment(const HashValue &hv, uint64_t depth) const noexcept
    {
        return GetSegment(hv.SegmentBits(depth));
    }

    const SegmentPtr Directory::LockSegment(uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        meta.subdirectories[sub]->mutexes[seg].lock();
        return meta.subdirectories[sub]->segments[seg];
    }

    const SegmentPtr Directory::LockSegment(const HashValue &hv, uint64_t depth) noexcept
    {
        return LockSegment(hv.SegmentBits(depth));
    }

    const SegmentPtr Directory::LockSegmentShared(uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        meta.subdirectories[sub]->mutexes[seg].lock_shared();
        return meta.subdirectories[sub]->segments[seg];
    }

    const SegmentPtr Directory::LockSegmentShared(const HashValue &hv, uint64_t depth) noexcept
    {
        return LockSegmentShared(hv.SegmentBits(depth));
    }

    bool Directory::TryLockSegment(uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        return meta.subdirectories[sub]->mutexes[seg].try_lock();
    }

    bool Directory::TryLockSegmentShared(uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        return meta.subdirectories[sub]->mutexes[seg].try_lock_shared();
    }

    void Directory::UnlockSegment(uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        meta.subdirectories[sub]->mutexes[seg].unlock();
    }

    void Directory::UnlockSegmentShared(uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        meta.subdirectories[sub]->mutexes[seg].unlock_shared();
    }

    bool Directory::AddSegment(PoolBase &pop, const SegmentPtr &ptr, uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        if (meta.subdirectories[sub] == nullptr)
        {
            TX::run(pop, [&]() {
                meta.subdirectories[sub] = pobj::make_persistent<SubDirectory>(pop).get();
            });
        }
        meta.subdirectories[sub]->segments[seg] = ptr;
        return true;
    }

    bool Directory::SetSegment(PoolBase &pop, const SegmentPtr &ptr, uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        meta.subdirectories[sub]->segments[seg] = ptr;
        return true;
    }

    bool Directory::Probe(uint64_t pos) const noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        return (meta.subdirectories[sub] != nullptr) && (meta.subdirectories[sub]->segments[seg] != nullptr);
    }

    bool Directory::Probe(const HashValue &hv) const noexcept
    {
        return Probe(hv.GetRaw());
    }

    void Directory::DoublingLink(PoolBase &pop, uint64_t prev_depth, uint64_t new_depth) noexcept
    {
        auto start = (1UL << prev_depth);
        auto end = (1UL << new_depth);
        for (auto i = start; i < end; i += SUBDIR_SIZE)
        {
            auto sub = i / SUBDIR_SIZE;
            if (meta.subdirectories[sub] == nullptr)
            {
                TX::run(pop, [&]() {
                    meta.subdirectories[sub] = pobj::make_persistent<SubDirectory>(pop).get();
                });
            }
        }

        for (auto i = start; i < end; i++)
        {
            // this buddy is the older buddy
            auto buddy = i - start;
            auto buddy_sub = buddy / SUBDIR_SIZE;
            auto buddy_seg = buddy % SUBDIR_SIZE;
            auto sub = i / SUBDIR_SIZE;
            auto seg = i % SUBDIR_SIZE;
            meta.subdirectories[sub]->segments[seg] = meta.subdirectories[buddy_sub]->segments[buddy_seg];
        }
    }
} // namespace Dalea
