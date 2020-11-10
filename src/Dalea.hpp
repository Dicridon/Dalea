#ifndef __DALEA__
#define __DALEA__
#include "Directory/Directory.hpp"
#include "Logger/Logger.hpp"

#include <sstream>
namespace Dalea
{
    class HashTable
    {
    public:
        HashTable(PoolBase &pop) : dir(pop), depth(1), logger(std::string("./dalea.log")) {};
        HashTable() = delete;
        HashTable(const HashTable &) = delete;
        HashTable(HashTable &&) = delete;

        FunctionStatus Put(PoolBase &pop, const std::string &key, const std::string &value) noexcept;
        KVPairPtr Get(const std::string &key) const noexcept;
        FunctionStatus Remove(PoolBase &pop, const std::string &key) noexcept;
        void Destory() noexcept;
        void Debug() const noexcept;
        void DebugToLog() const;
        void Log(std::string &msg) const;
        void Log(std::stringstream &msg_s) const;

    private:
        uint8_t depth;
        Directory dir;
        std::shared_mutex doubling_lock;
        mutable Logger logger;

        void split(PoolBase &pop, Bucket &bkt, const HashValue &hv, SegmentPtr &seg, uint64_t segno) noexcept;
        void simple_split(PoolBase &pop, Bucket &bkt, const HashValue &hv, uint64_t segno, bool helper) noexcept;
        void complex_split(PoolBase &pop, Bucket &bkt, const HashValue &hv, SegmentPtr &ptr, uint64_t segno) noexcept;

        void flatten_bucket(PoolBase &pop, Bucket &bkt, const HashValue &hv, uint64_t segno) noexcept;
        SegmentPtr make_buddy_segment(PoolBase &pop, const SegmentPtr &root, uint64_t segno, uint64_t buddy_segno, const Bucket &bkt) noexcept;
        void migrate_and_pave(PoolBase &pop, uint64_t root_segno, uint64_t buddy_segno, Bucket &bkt, uint64_t bktbits) noexcept;
    };
} // namespace Dalea
#endif
