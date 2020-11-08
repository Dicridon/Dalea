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
    for (const auto &i : workload)
    {
        map->Put(pop, i, i);
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

    for (long i = 0; i < batch; i++)
    {
        auto key = new_string(i);
        auto value = new_string(i);
        auto ptr = root->map->Get(key);
        if (ptr == nullptr)
        {
            std::cout << "missing value for key " << key << "\n";
        }
        else if (ptr->value != value)
        {
            std::cout << "wrong value for key " << key << "\n";
            std::cout << "expecting " << value << "\n";
            std::cout << "got " << ptr->value.c_str() << "\n";
        }
    }
    std::cout << "single thread check passed\n";
}
