#include "Directory.hpp"
namespace Dalea
{
    Directory::MetaDirectory::MetaDirectory(PoolBase &pop)
    {
        TX::run(pop, [&]() {
            subdirectories[0] = pobj::make_persistent<Directory::SubDirectory>(pop);
            pop.persist(subdirectories[0]);
        });
        for (int i = 1; i < METADIR_SIZE; i++)
        {
            subdirectories[i] = nullptr;
        }
    }

    Directory::SubDirectory::SubDirectory(PoolBase &pop)
    {
        TX::run(pop, [&]() {
            segments[0] = pobj::make_persistent<Segment>(pop, 1, 0, false);
            segments[1] = pobj::make_persistent<Segment>(pop, 1, 1, false);
            pop.persist(segments[0]);
            pop.persist(segments[1]);
        });

        for (int i = 2; i < SUBDIR_SIZE; i++)
        {
            segments[i] = nullptr;
        }
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

    bool Directory::AddSegment(PoolBase &pop, const SegmentPtr &ptr, uint64_t pos) noexcept
    {
        auto sub = pos / SUBDIR_SIZE;
        auto seg = pos % SUBDIR_SIZE;
        if (meta.subdirectories[sub] == nullptr)
        {
            TX::run(pop, [&]() {
                meta.subdirectories[sub] = pobj::make_persistent<SubDirectory>(pop);
            });
        }
        if (meta.subdirectories[sub]->segments[seg] == nullptr)
        {
            meta.subdirectories[sub]->segments[sub] = ptr;
        }
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
} // namespace Dalea