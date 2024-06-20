# Dalea: A Persistent Multi-level Extendible Hashing with Improved Tail Performance
This is the source code of paper **Dalea: A Persistent Multi-level Extendible Hashing with Improved Tail Performance** in JCST, 2023, 38(5).

# Building
Currently building tool `canoe` is used to manage this project, which is a ruby gem requiring Ruby >= 2.7.1, please using `rvm` and `gem` to install Ruby 2.7.1 and `canoe`.

A `Makefile` would be added later.

# Test
please offer a bench file containing lines in format of `INSERT/READ/UPDATE/DELETE data`, which is also use by CLevel.

## Main indexes
1. Throughput (varying segment and bucket size)
2. load factor
3. average latency
4. P90, P99, P999
5. Positive/Negative search latency

## Known bugs
1. Meta directory expansion not implemented yet, thus if the hash table size exceeds the capacity of meta directory, program may crash or hang up.
