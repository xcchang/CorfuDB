# Reactive LogUnit server

1. indexing: immutable skip list
  - immutable skip list: https://www.sciencedirect.com/science/article/pii/S0743731504002333
  - nitro: http://www.vldb.org/pvldb/vol9/p1413-lakshman.pdf
  advantages:
   - completely immutable data structure
   - lock free, non-blocking reads - high performance, scalability
   - atomic updates
   - delete head - O(1)
   - seek - log(n)
   - random insert - log(n)
   - can be persistent index, don't need to keep in memory
   - can read from disk in one operation
   - very high performance
   - flexible - can load pieces of the index from disk, can save pieces of index in disk
   - can be lazy index or persistent or both.
  performance:
   - scalability on reads and writes, concurrent reads, many threads
   - insert: much faster/scalable (10x - one thread) than ConcurrentMap
   - smaller memory footprint - 2 times. 
  
2. performance metrics:
  - random reads
  - sequential writes - gathering
  - write: dynamic buffering/batching according to message size
  - read: batch read/scattering. Since we have skipList index we can figure out
    if the range of global addresses is sequential or not, 
    if yes then `scattering` can be used.
  
3. cache:
  - get rid of cache
  - having cache only for the stream tail 
  
4. stream log
  - multi writer - many threads writing data
  - multi reader - many threads reading data