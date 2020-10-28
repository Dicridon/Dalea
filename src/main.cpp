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

    std::cout << "pool file is " << pool_file << "\n";
    std::cout << "warm file is " << warm_file << "\n";
    std::cout << "run file is " << run_file << "\n";
    std::cout << "threads is " << threads << "\n";
}
