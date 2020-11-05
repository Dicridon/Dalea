#include <string>
#include <unordered_map>
#include <cstring>
namespace Dalea {
    enum class ParserStatus {
        Accepted,
        Missing,
        Rejected,
    };
    
    class CmdParser {
        public:
            CmdParser() {};
            CmdParser(CmdParser& ) = delete;
            CmdParser(CmdParser&& ) = delete;
                  
            bool buildCmdParser(int argc, char* argv[]) noexcept; 
            std::string getOption(std::string& opt_name) noexcept;
            std::string getOption(std::string opt_name) noexcept;
            bool optionOffered(std::string& opt_name) const noexcept;
            bool optionOffered(std::string opt_name) const noexcept;

        private:
            std::unordered_map<std::string, std::string> opts;
            
            void putOption(std::string optname, std::string value) ;
            
            ParserStatus parsePoolFile(char *argv, char *next) ;
            
            ParserStatus parseWarmFile(char *argv, char *next) ;
            
            ParserStatus parseRunFile(char *argv, char *next) ;
            
            ParserStatus parseThreads(char *argv, char *next) ;
            
            ParserStatus parseBatch(char *argv, char *next) ;

    };
}
