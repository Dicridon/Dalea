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

    remove(pool_file.c_str());
    auto pop = pobj::pool<DaleaRoot>::create(pool_file, "Dalea", PMEMOBJ_MIN_POOL * 10240, S_IWUSR | S_IRUSR); 
    auto r = pop.root();
    TX::run(pop, [&]() {
            r->map = pobj::make_persistent<HashTable>(pop);
            });

    for (long i = 0; i < batch; i++)
    {
        auto key = "xxxxxxxxx" + std::to_string(i);
        auto value = "xxxxxxxxx" + std::to_string(i);
        r->map->Put(pop, key, value);
        // r->map->Debug();
        // std::cout << r->map->Get(key)->value.c_str() << "\n";
        // if (i % (batch / 100) == 0)
        // {
        //     std::cout << "progress: " << double(i) / batch * 100 << "%\n";
        // }
    }

    for (long i = 0; i < batch; i++)
    {
        auto key = "xxxxxxxxx" + std::to_string(i);
        auto value = "xxxxxxxxx" + std::to_string(i);
        auto ptr = r->map->Get(key);
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
