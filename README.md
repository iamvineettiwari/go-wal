# go-wal
Implementation of Write Ahead Log in golang to with high write throughput and support for log rotation

### Operations supported
1. `Write` - writes data to wal
2. `CreateCheckpoint` - creates new checkpoint
3. `Read` - read data from current segment
4. `ReadFromSegment` - read data from passed segment number till current segment
5. `ReadFromLastCheckpoint` - read data from last created checkpoint
6. `Repair` - repair the corrupted segments
7. `Sync` - flush the buffer write to disk

### Benchmarks
![benchmark-wal](https://github.com/user-attachments/assets/2450b50d-c12c-4b0c-ab11-73804eee5d72)

### Further Optimizations
1. Usage of `protobuf` in place of `json` for data serialization / de-serialization
