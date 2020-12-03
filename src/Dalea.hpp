#ifndef __DALEA__
#define __DALEA__
#include "Directory/Directory.hpp"
#include "Logger/Logger.hpp"
#include "Stats/Stats.hpp"

#include <atomic>
#include <chrono>
#include <sstream>
#include <thread>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/vector.hpp>
namespace Dalea
{
    using namespace std::chrono_literals;
    struct SegmentPtrQueue
    {
        SegmentPtrQueue(PoolBase &pop, int init_cap = 512);
        bool Push(const SegmentPtr &ptr) noexcept;
        SegmentPtr Top() noexcept;
        SegmentPtr Pop() noexcept;
        bool HasSpace() const noexcept;

        std::mutex lock;
        pobj::persistent_ptr<SegmentPtr[]> buffer;
        uint64_t head;
        uint64_t tail;
        int capacity;
        int size;
    };

    struct HashPair
    {
        KVPairPtr kv;
        HashValue hv;
        HashPair(const KVPairPtr &_kv, const HashValue &_hv) : kv(_kv), hv(_hv) {};
        HashPair(const HashPair &) = default;
        HashPair(HashPair &&) = default;
        HashPair &operator=(const HashPair &rhs) = default;
    };

    class HashTable
    {
    public:
        HashTable(PoolBase &pop, int thread_num);
        HashTable() = delete;
        HashTable(const HashTable &) = delete;
        HashTable(HashTable &&) = delete;

        FunctionStatus Put(PoolBase &pop, Stats &stats, int thread_id, const std::string &key, const std::string &value) noexcept;
        KVPairPtr Get(const std::string &key) const noexcept;
        FunctionStatus Remove(PoolBase &pop, const std::string &key) noexcept;
        uint64_t Capacity() const noexcept;
        void Destory() noexcept;
        void Debug() const noexcept;
        void DebugToLog() const;
        void Log(std::string msg) const;
        void Log(std::string &msg) const;
        void Log(std::stringstream &msg_s) const;

        mutable SegmentPtrQueue segment_pool;
        mutable pobj::concurrent_hash_map<std::string, HashPair> stash;

    private:
        uint8_t depth;
        bool doubling;
        Directory dir;

        uint64_t capacity;
        uint64_t loaded;

        std::atomic_bool to_double;
        std::atomic_int readers;
        std::shared_mutex doubling_lock;
        mutable Logger logger;
        pobj::vector<int> stash_limits;


        void split(PoolBase &pop, Stats &stats, Bucket &bkt, const HashValue &hv, SegmentPtr &seg, uint64_t segno) noexcept;
        void simple_split(PoolBase &pop, Stats &stats, uint64_t root_segno, uint64_t buddy_segno, Bucket &bkt, uint64_t bktbits) noexcept;
        void traditional_split(PoolBase &pop, Stats &stats, Bucket &bkt, const HashValue &hv, uint64_t segno, bool helper) noexcept;
        void complex_split(PoolBase &pop, Stats &stats, Bucket &bkt, const HashValue &hv, SegmentPtr &ptr, uint64_t segno) noexcept;

        void flatten_bucket(PoolBase &pop, Bucket &bkt, const HashValue &hv, uint64_t segno) noexcept;
        SegmentPtr make_buddy_segment(PoolBase &pop, const SegmentPtr &root, uint64_t segno, uint64_t buddy_segno, const Bucket &bkt) noexcept;
    };
} // namespace Dalea
#endif
