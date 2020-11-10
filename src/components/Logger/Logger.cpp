#include "Logger.hpp"
namespace Dalea
{
    void Logger::Write(std::string &msg)
    {
        std::unique_lock l(lock);
        out_file << msg;
    }
}
