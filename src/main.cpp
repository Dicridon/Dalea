#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <algorithm>
#include <numeric>

#include "CmdParser.hpp"
#include "Dalea.hpp"

using namespace Dalea;

struct DaleaRoot
{
    pobj::persistent_ptr<HashTable> map;
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

auto prepare_root(pobj::pool<DaleaRoot> &pop)
{
    auto r = pop.root();
    TX::run(pop, [&]() {
        r->map = pobj::make_persistent<HashTable>(pop);
    });
    return r;
}

void put(PoolBase &pop, std::vector<std::string> &workload, pobj::persistent_ptr<HashTable> &map)
{
    // for (const auto &i : workload)
    // {
    //     map->Put(pop, i, i);
    // }
    for (auto i = 0; i < workload.size(); i++)
     {
        if (i % 100000 == 0)
        {
            std::cout << "Progress: " << double(i) / workload.size() * 100 << "%\n";
        }
        // std::cout << workload[i] << "\n";
        map->Put(pop, workload[i], workload[i]);
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
    auto root = prepare_root(pop);

    std::thread workers[threads];
    std::vector<std::string> workload[threads];

    for (auto i = 0; i < batch; i++)
    {
        workload[i % threads].push_back(new_string(i));
    }

    for (auto i = 0; i < threads; i++)
    {
        workers[i] = std::thread(put, std::ref(pop), std::ref(workload[i]), std::ref(root->map));
    }

    for (auto &t : workers)
    {
        t.join();
    }

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
}
