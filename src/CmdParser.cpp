#include "CmdParser.hpp"
#include <iostream>
namespace Dalea
{
    bool CmdParser::buildCmdParser(int argc, char *argv[]) noexcept
    {
        for (int i = 1; i < argc; i++)
        {
            if (parsePoolFile(argv[i], i < argc - 1 ? argv[i + 1] : nullptr) == ParserStatus::Rejected)
            {
                return false;
            }
            if (parseWarmFile(argv[i], i < argc - 1 ? argv[i + 1] : nullptr) == ParserStatus::Rejected)
            {
                return false;
            }
            if (parseRunFile(argv[i], i < argc - 1 ? argv[i + 1] : nullptr) == ParserStatus::Rejected)
            {
                return false;
            }
            if (parseThreads(argv[i], i < argc - 1 ? argv[i + 1] : nullptr) == ParserStatus::Rejected)
            {
                return false;
            }
            if (parseBatch(argv[i], i < argc - 1 ? argv[i + 1] : nullptr) == ParserStatus::Rejected)
            {
                return false;
            }
        }
        return true;
    }

    std::string CmdParser::getOption(std::string &opt_name) noexcept
    {
        auto opt = opts.find(opt_name);
        if (opt != opts.end())
        {
            return opt->second;
        }
        return "";
    }

    std::string CmdParser::getOption(std::string opt_name) noexcept
    {
        auto opt = opts.find(opt_name);
        if (opt != opts.end())
        {
            return opt->second;
        }
        return "";
    }

    bool CmdParser::optionOffered(std::string &opt_name) const noexcept
    {
        return opts.find(opt_name) == opts.end();
    }

    bool CmdParser::optionOffered(std::string opt_name) const noexcept
    {
        return opts.find(opt_name) == opts.end();
    }

    void CmdParser::putOption(std::string opt_name, std::string value)
    {
        opts.insert({opt_name, value});
    }

    ParserStatus CmdParser::parsePoolFile(char *argv, char *next)
    {
        if (strncmp("--pool_file", argv, 11) == 0)
        {
            if (strncmp("--pool_file=", argv, 12) == 0)
            {
                std::string value(argv + 12);
                if (value.length() == 0)
                {
                    std::cerr << "please offer a value to pool_file\n";
                    return ParserStatus::Rejected;
                }
                putOption("pool_file", value);
            }

            return ParserStatus::Accepted;
        }

        if (strncmp("-p", argv, 2) == 0)
        {
            if (next)
            {
                putOption("pool_file", next);
                return ParserStatus::Accepted;
            }
            else
            {
                std::cerr << "not enough arguments to -p\n";
                return ParserStatus::Rejected;
            }
        }

        return ParserStatus::Missing;
    }

    ParserStatus CmdParser::parseWarmFile(char *argv, char *next)
    {
        if (strncmp("--warm_file", argv, 11) == 0)
        {
            if (strncmp("--warm_file=", argv, 12) == 0)
            {
                std::string value(argv + 12);
                if (value.length() == 0)
                {
                    std::cerr << "please offer a value to warm_file\n";
                    return ParserStatus::Rejected;
                }
                putOption("warm_file", value);
            }

            return ParserStatus::Accepted;
        }

        if (strncmp("-w", argv, 2) == 0)
        {
            if (next)
            {
                putOption("warm_file", next);
                return ParserStatus::Accepted;
            }
            else
            {
                std::cerr << "not enough arguments to -w\n";
                return ParserStatus::Rejected;
            }
        }

        return ParserStatus::Missing;
    }

    ParserStatus CmdParser::parseRunFile(char *argv, char *next)
    {
        if (strncmp("--run_file", argv, 10) == 0)
        {
            if (strncmp("--run_file=", argv, 11) == 0)
            {
                std::string value(argv + 11);
                if (value.length() == 0)
                {
                    std::cerr << "please offer a value to run_file\n";
                    return ParserStatus::Rejected;
                }
                putOption("run_file", value);
            }

            return ParserStatus::Accepted;
        }

        if (strncmp("-r", argv, 2) == 0)
        {
            if (next)
            {
                putOption("run_file", next);
                return ParserStatus::Accepted;
            }
            else
            {
                std::cerr << "not enough arguments to -r\n";
                return ParserStatus::Rejected;
            }
        }

        return ParserStatus::Missing;
    }

    ParserStatus CmdParser::parseThreads(char *argv, char *next)
    {
        if (strncmp("--threads", argv, 9) == 0)
        {
            if (strncmp("--threads=", argv, 10) == 0)
            {
                std::string value(argv + 10);
                if (value.length() == 0)
                {
                    std::cerr << "please offer a value to threads\n";
                    return ParserStatus::Rejected;
                }
                putOption("threads", value);
            }

            return ParserStatus::Accepted;
        }

        if (strncmp("-t", argv, 2) == 0)
        {
            if (next)
            {
                putOption("threads", next);
                return ParserStatus::Accepted;
            }
            else
            {
                std::cerr << "not enough arguments to -t\n";
                return ParserStatus::Rejected;
            }
        }

        return ParserStatus::Missing;
    }

    ParserStatus CmdParser::parseBatch(char *argv, char *next)
    {
        if (strncmp("--batch", argv, 7) == 0)
        {
            if (strncmp("--batch=", argv, 8) == 0)
            {
                std::string value(argv + 8);
                if (value.length() == 0)
                {
                    std::cerr << "please offer a value to batch\n";
                    return ParserStatus::Rejected;
                }
                putOption("batch", value);
            }

            return ParserStatus::Accepted;
        }

        if (strncmp("-b", argv, 2) == 0)
        {
            if (next)
            {
                putOption("batch", next);
                return ParserStatus::Accepted;
            }
            else
            {
                std::cerr << "not enough arguments to -b\n";
                return ParserStatus::Rejected;
            }
        }

        return ParserStatus::Missing;
    }

} // namespace Dalea
