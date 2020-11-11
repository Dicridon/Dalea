#include "Logger.hpp"
#include <chrono>
#include <thread>

namespace Dalea
{
    void Logger::Write(const std::string &msg)
    {
        std::unique_lock l(lock);
        auto now = std::chrono::high_resolution_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
        out_file << ns.count() << " by " << std::this_thread::get_id() << ": ";
        out_file << msg;
        out_file.flush();
    }

    void Logger::Write(std::string &&msg)
    {
        std::unique_lock l(lock);
        auto now = std::chrono::high_resolution_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
        out_file << ns.count() << " by " << std::this_thread::get_id() << ": ";
        out_file << msg;
        out_file.flush();
    }
}
