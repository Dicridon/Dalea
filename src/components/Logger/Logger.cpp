#include "Logger.hpp"
namespace Dalea
{
    void Logger::Write(const std::string &msg)
    {
        std::unique_lock l(lock);
        out_file << msg;
    }

    void Logger::Write(std::string &&msg)
    {
        std::unique_lock l(lock);
        out_file << msg;
    }
}
