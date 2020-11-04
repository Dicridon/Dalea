#include <iostream>

#include "CmdParser.hpp"
#include "Dalea.hpp"

using namespace Dalea;

struct DaleaRoot
{
    pobj::persistent_ptr<HashTable> map;
};

int main(int argc, char *argv[])
{
    Dalea::CmdParser parser;
    if (argc < 4 || !parser.buildCmdParser(argc, argv))
    {
        std::cout << "usage: ./target/Dalea -p pool_file -w warmup_file -r run_file [-t [threads]]\n";
        return -1;
    }

    auto pool_file = parser.getOption("pool_file");
    auto warm_file = parser.getOption("warm_file");
    auto run_file = parser.getOption("run_file");
    auto threads = std::stoi(parser.getOption("threads"));

    std::cout << "[[ bench info: \n";
    std::cout << "   pool file is " << pool_file << "\n";
    std::cout << "   warm file is " << warm_file << "\n";
    std::cout << "   run file is " << run_file << "\n";
    std::cout << "   threads is " << threads << "\n";

    remove(pool_file.c_str());
    auto pop = pobj::pool<DaleaRoot>::create(pool_file, "Dalea", PMEMOBJ_MIN_POOL * 1024, S_IWUSR | S_IRUSR); 
    auto r = pop.root();
    TX::run(pop, [&]() {
            r->map = pobj::make_persistent<HashTable>(pop);
            });
    for (long i = 0; i < 10000000; i++)
    {
        auto key = "xxxxxxxxx" + std::to_string(i);
        auto value = "xxxxxxxxx" + std::to_string(i);
        r->map->Put(pop, key, value);
        std::cout << r->map->Get(key)->value.c_str() << "\n";
        // r->map->Debug();
    }
}
