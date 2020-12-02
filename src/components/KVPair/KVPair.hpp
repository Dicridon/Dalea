#ifndef __DALEA__KVPAIR__KVPAIR__
#define __DALEA__KVPAIR__KVPAIR__
#include <utility>

#include "Common/Common.hpp"
namespace Dalea
{
    // using KVPairPtr = pmem::obj::persistent_ptr<KVPair>;
    using KVPair = std::pair<std::string, std::string>;
    using KVPairPtr = KVPair *;
} // namespace Dalea
#endif
