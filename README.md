# go-wal
Implementation of Write Ahead Log in golang to with high write throughput

### Operations supported
1. `Write` - writes data to wal
2. `Read` - read data from current segment
3. `ReadFromSegment` - read data from passed segment number till current segment
4. `Repair` - repair the corrupted segments
5. `Sync` - flush the buffer write to disk

### Benchmarks
![benchmark-wal](https://github.com/user-attachments/assets/2450b50d-c12c-4b0c-ab11-73804eee5d72)

### Further Optimizations
1. Usage of `protobuf` in place of `json` for data serialization / de-serialization
