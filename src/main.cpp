#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <numeric>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "CmdParser.hpp"
#include "Dalea.hpp"

#define VALIATION

using namespace Dalea;

const std::string PUT = "INSERT";
const std::string GET = "READ";
const std::string UPDATE = "UPDATE";
const std::string DELETE = "DELETE";

struct DaleaRoot
{
    pobj::persistent_ptr<HashTable> map;
};

enum class Ops
{
    Insert,
    Read,
    Update,
    Delete,
};

struct WorkloadItem
{
    Ops type;
    std::string key;

    WorkloadItem(const Ops &t, const std::string &k) : type(t), key(k){};
};

static std::string new_string(uint64_t i)
{
    static std::string prefix = "xxxxxxxxx";
    return prefix + std::to_string(i);
}

auto prepare_pool(std::string &file, size_t size)
{
    remove(file.c_str());
    auto pop = pobj::pool<DaleaRoot>::create(file, "Dalea", PMEMOBJ_MIN_POOL * 10240, S_IWUSR | S_IRUSR);
    return pop;
}

auto prepare_root(pobj::pool<DaleaRoot> &pop, int thread_num)
{
    auto r = pop.root();
    TX::run(pop, [&]() {
        r->map = pobj::make_persistent<HashTable>(pop, thread_num);
    });
    return r;
}

void bench_thread(std::function<void(const WorkloadItem &, Stats &, int)> func,
                  int tid,
                  std::vector<Stats> &stats,
                  std::vector<WorkloadItem> &workload,
                  std::vector<double> &throughput,
                  std::vector<double> &latency,
                  std::vector<double> &p90,
                  std::vector<double> &p99,
                  std::vector<double> &p999)
{
    auto sampling_batch = 20000;
    auto counter = 0;
    std::priority_queue<double> tail;
    std::vector<double> latencies;

    std::chrono::time_point<std::chrono::steady_clock> lat_start;
    std::chrono::time_point<std::chrono::steady_clock> lat_end;
    double time_elapsed = 0;
    Stats st;
    for (const auto &i : workload)
    {
        lat_start = std::chrono::steady_clock::now();

        func(i, st, tid);

        lat_end = std::chrono::steady_clock::now();
        time_elapsed += (lat_end - lat_start).count();
        tail.push((lat_end - lat_start).count());
        latencies.push_back((lat_end - lat_start).count()); // in nanoseconds
        if (++counter == sampling_batch)
        {
            throughput.push_back(sampling_batch / time_elapsed * 1000000000.0);
            latency.push_back(std::accumulate(latencies.cbegin(), latencies.cend(), 0.0) / sampling_batch);

            double tmp_p90 = 0, tmp_p99 = 0, tmp_p999 = 0;
            for (int i = 0; i < sampling_batch * 0.001; i++)
            {
                tmp_p90 += tail.top();
                tmp_p99 += tail.top();
                tmp_p999 += tail.top();
                tail.pop();
            }

            for (int i = 0; i < sampling_batch * 0.009; i++)
            {
                tmp_p90 += tail.top();
                tmp_p99 += tail.top();
                tail.pop();
            }

            for (int i = 0; i < sampling_batch * 0.09; i++)
            {
                tmp_p90 += tail.top();
                tail.pop();
            }
            p90.push_back(tmp_p90 / (sampling_batch * 0.1));
            p99.push_back(tmp_p99 / (sampling_batch * 0.01));
            p999.push_back(tmp_p999 / (sampling_batch * 0.001));
            stats.push_back(st);

            time_elapsed = 0;
            latencies.clear();
            tail = std::priority_queue<double>();
            counter = 0;
            st.Clear();
        }
    }
}

int main(int argc, char *argv[])
{

    Dalea::CmdParser parser;
    if (argc < 4 || !parser.buildCmdParser(argc, argv))
    {
        std::cout << "usage: ./target/Dalea -p pool_file -w warmup_file -r run_file -t threads -b batch\n";
        return -1;
    }

    auto pool_file = parser.getOption("pool_file");
    auto warm_file = parser.getOption("warm_file");
    auto run_file = parser.getOption("run_file");
    auto threads = std::stoi(parser.getOption("threads"));
    auto batch = std::stol(parser.getOption("batch"));

    std::cout << "[[ bench info: \n";
    std::cout << "   pool file is " << pool_file << "\n";
    std::cout << "   warm file is " << warm_file << "\n";
    std::cout << "   run file is " << run_file << "\n";
    std::cout << "   threads is " << threads << "\n";
    std::cout << "   batch size is " << batch << "\n";

    auto pop = prepare_pool(pool_file, 10240);
    auto root = prepare_root(pop, threads);

    using namespace std::chrono_literals;
    bool to_stop = false;
    auto guardian = [&](PoolBase &pop, SegmentPtrQueue &queue) {
        while (!to_stop)
        {
            while (queue.HasSpace())
            {
                // std::cout << "[[[[[[[[[[[[[[[[[[[[ refilling\n";
                TX::run(pop, [&]() {
                    auto ptr = pobj::make_persistent<Segment>(pop, 0, 0, false);
                    queue.Push(ptr);
                });
            }
        }
    };

    std::thread(guardian, std::ref(pop), std::ref(root->map->segment_pool)).detach();
    std::thread(guardian, std::ref(pop), std::ref(root->map->segment_pool)).detach();

    {
        std::thread workers[threads];
        std::vector<WorkloadItem> workloads[threads];
        std::vector<Stats> statses[threads];
        std::vector<double> throughputs[threads];
        std::vector<double> latencies[threads];
        std::vector<double> p90s[threads];
        std::vector<double> p99s[threads];
        std::vector<double> p999s[threads];

        std::ifstream warmup;
        std::ifstream run;
        warmup.open(warm_file);
        run.open(run_file);
        std::string buffer;

        std::cout << "warming up\n";
        Stats _unused;
        auto load_count = 0;
        while (getline(warmup, buffer))
        {
            std::string key = buffer.c_str() + PUT.length();
            root->map->Put(pop, _unused, 0, key, key);
            ++load_count;
            // if (load_count % 160000 == 0)
            // {
            //     std::cout << "loadfactor: " << double(load_count) / root->map->Capacity() << '\n';
            // }
        }

        auto count = 0;
        auto load = 0;
        std::cout << "starts running\n";
        while (getline(run, buffer))
        {
            ++load;
            std::string key;
            Ops type;
            if (buffer.rfind(PUT, 0) == 0)
            {
                key = buffer.c_str() + PUT.length();
                type = Ops::Insert;
            }
            else if (buffer.rfind(GET, 0) == 0)
            {
                key = buffer.c_str() + GET.length();
                type = Ops::Read;
            }
            else if (buffer.rfind(UPDATE, 0) == 0)
            {
                key = buffer.c_str() + UPDATE.length();
                type = Ops::Update;
            }
            else if (buffer.rfind(DELETE, 0) == 0)
            {
                key = buffer.c_str() + DELETE.length();
                type = Ops::Delete;
            }
            else
            {
                std::cout << "unknown operation: " << buffer << "\n";
                return -1;
            }
            workloads[(count++) % threads].push_back(WorkloadItem(type, key));
        }

        auto consume = [&](const WorkloadItem &item, Stats &stats, int tid) {
            switch (item.type)
            {
            case Ops::Insert:
                root->map->Put(pop, stats, tid, item.key, item.key);
                break;
            case Ops::Read:
                root->map->Get(item.key);
                break;
            case Ops::Update:
                root->map->Put(pop, stats, tid, item.key, item.key);
                break;
            case Ops::Delete:
                root->map->Remove(pop, item.key);
                break;
            default:
                break;
            }
        };

        auto start = std::chrono::steady_clock::now();
        for (auto i = 0; i < threads; i++)
        {
            workers[i] = std::thread(bench_thread,
                                     consume,
                                     i,
                                     std::ref(statses[i]),
                                     std::ref(workloads[i]),
                                     std::ref(throughputs[i]),
                                     std::ref(latencies[i]),
                                     std::ref(p90s[i]),
                                     std::ref(p99s[i]),
                                     std::ref(p999s[i]));
        }
        for (auto &t : workers)
        {
            t.join();
        }
        auto end = std::chrono::steady_clock::now();
        auto duration = (end - start).count();
        std::cout << "time elapsed is " << duration << "\n";
        std::cout << "throughput is " << double(load) / duration * 1000000000.0 << "\n";

        std::cout << "\nreporting throughput by thread:\n";
        for (auto i = 0; i < threads; i++)
        {
            std::cout << "thread " << i << ": ";
            for (auto t : throughputs[i])
            {
                std::cout << t << " ";
            }
            std::cout << "\n";
        }

        std::cout << "\nreporting latency by thread:\n";
        for (auto i = 0; i < threads; i++)
        {
            std::cout << "thread " << i << ": ";
            for (auto l : latencies[i])
            {
                std::cout << l << " ";
            }
            std::cout << "\n";
        }

        std::cout << "\nreporting p90 by thread:\n";
        for (auto i = 0; i < threads; i++)
        {
            std::cout << "thread " << i << ": ";
            for (auto p : p90s[i])
            {
                std::cout << p << " ";
            }
            std::cout << "\n";
        }

        std::cout << "\nreporting p99 by thread:\n";
        for (auto i = 0; i < threads; i++)
        {
            std::cout << "thread " << i << ": ";
            for (auto p : p99s[i])
            {
                std::cout << p << " ";
            }
            std::cout << "\n";
        }

        std::cout << "\nreporting p999 by thread:\n";
        for (auto i = 0; i < threads; i++)
        {
            std::cout << "thread " << i << ": ";
            for (auto p : p999s[i])
            {
                std::cout << p << " ";
            }
            std::cout << "\n";
        }

        std::cout << "\nreporting simple split by thread:\n";
        for (auto i = 0; i < threads; i++)
        {
            std::cout << "thread " << i << ": ";
            for (auto p : statses[i])
            {
                std::cout << p.simple_splits << " ";
            }
            std::cout << "\n";
        }

        std::cout << "\nreporting traditional split by thread:\n";
        for (auto i = 0; i < threads; i++)
        {
            std::cout << "thread " << i << ": ";
            for (auto p : statses[i])
            {
                std::cout << p.traditional_splits << " ";
            }
            std::cout << "\n";
        }

        std::cout << "\nreporting complex split by thread:\n";
        for (auto i = 0; i < threads; i++)
        {
            std::cout << "thread " << i << ": ";
            for (auto p : statses[i])
            {
                std::cout << p.complex_splits << " ";
            }
            std::cout << "\n";
        }
        to_stop = true;
#ifdef VALIATION
        bool pass = true;
        long i = 0;
        for (; i < batch; i++)
        {
            auto key = new_string(i);
            auto value = new_string(i);
            auto ptr = root->map->Get(key);
            std::stringstream buf;
            if (ptr == nullptr)
            {
                buf << "missing value for key " << key << "\n";
                std::cout << buf.str();
                root->map->Log(buf);
                pass = false;
            }
            else if (ptr->value != value)
            {
                buf << "wrong value for key " << key << "\n";
                buf << "expecting " << value << "\n";
                buf << "got " << ptr->value.c_str() << "\n";
                std::cout << buf.str();
                root->map->Log(buf);
                pass = false;
            }
        }
        if (pass)
        {
            std::cout << "check passed\n";
        }
        else
        {
            std::cout << "check failed\n";
            root->map->DebugToLog();
        }
#endif
    }
}
