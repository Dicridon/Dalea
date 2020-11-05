# Dalea: **D**irectory-sh**a**ring Multi**l**evel **E**xtendible **H**ash
A persistent extendible hash which has extremely low split cost

## Main indexes
1. Throughput (varying segment and bucket size)
2. load factor
3. average latency
4. P90, P99, P999
5. Positive/Negative search latency

## Known bugs
1. Meta directory expansion not implemented yet, thus if the hash table size exceeds the capacity of meta directory, program may crash or hang up.
