## How to Run
To run the benchmarks for existing caches, run the following command -
```
go test -bench=. -cpu=N
```
where N is the number of CPUs (routines) that will access the map concurrently.
