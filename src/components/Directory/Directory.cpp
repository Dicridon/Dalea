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
                TX::run(pop, [&](){
                    meta.subdirectories[sub] = pobj::make_persistent<SubDirectory>(pop);
                });
            }
        }
        for(auto i = start; i < end; i++)
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
