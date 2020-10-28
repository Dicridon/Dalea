#ifndef __DALEA__KVPAIR__KVPAIR__
#define __DALEA__KVPAIR__KVPAIR__

#include "Common/Common.hpp"
namespace Dalea
{
    struct KVPair
    {
        PString key;
        PString value;

        KVPair(const String &k, const String &v) : key(k), value(v){};
        KVPair(const PString &k, const PString &v) : key(k), value(v){};
        KVPair(PString &&k, PString &&v) : key(std::move(k)), value(std::move(v)){};
    };

    using KVPairPtr = pmem::obj::persistent_ptr<KVPair>;
} // namespace Dalea
#endif
