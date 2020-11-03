#ifndef __DALEA__
#define __DALEA__
#include "Directory/Directory.hpp"
namespace Dalea
{
    class HashTable
    {
    public:
        HashTable(PoolBase &pop) : dir(pop), depth(1) {};
        HashTable() = delete;
        HashTable(const HashTable &) = delete;
        HashTable(HashTable &&) = delete;

        FunctionStatus Put(PoolBase &pop, std::string &key, std::string &value) noexcept;
        KVPairPtr Get(std::string &key) const noexcept;
        FunctionStatus Remove(PoolBase &pop, std::string &key, std::string &value) noexcept;
        void Destory() noexcept;


    private:
        uint8_t depth;
        Directory dir;
        std::shared_mutex doubling_lock;

        void split(PoolBase &pop, Bucket &bkt, std::string &key, std::string &value, const HashValue & hv, SegmentPtr &seg, uint64_t segno) noexcept;
        void simple_split(PoolBase &pop, Bucket &bkt, std::string &key, std::string &value, const HashValue & hv, uint64_t segno, bool helper) noexcept;
        void complex_split(PoolBase &pop, Bucket &bkt, std::string &key, std::string &value, const HashValue & hv, SegmentPtr &ptr, uint64_t segno) noexcept;

        void flatten_bucket(PoolBase &pop, Bucket &bkt, std::string &key, std::string &value, const HashValue & hv, uint64_t segno) noexcept;

    };
} // namespace Dalea
#endif
