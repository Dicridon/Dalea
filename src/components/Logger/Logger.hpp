#ifndef __DALEA__LOGGER__LOGGER__
#define __DALEA__LOGGER__LOGGER__
#include <fstream>
#include <iostream>
#include <mutex>
namespace Dalea
{
    /*
     * a logger for serialized output for my programs to simplify debugging, not for recovery
     */
    class Logger
    {
    public:
        Logger(std::string &log_file) : out_file(log_file) {}
        ~Logger() = default;

        void Write(std::string &msg);

    private:
        std::ofstream out_file;
        std::mutex lock;
    };
} // namespace Dalea
#endif
