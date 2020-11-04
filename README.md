# Dalea: **D**irectory-sh**a**ring Multi**l**evel **E**xtendible **H**ash
A persistent extendible hash which has extremely low split cost

## Known bugs
1. Meta directory expansion not implemented yet, thus if the hash table size exceeds the capacity of meta directory, program may crash or hang up.
