# Benchmark Environment

- **OS**: Windows 11 Home 10.0.26200
- **CPU**: Intel Core (13th/14th Gen, Family 6 Model 183), 10 physical cores, 16 logical cores
- **RAM**: 31.8 GB
- **Python**: 3.11.15 (Anaconda, MSC v.1942 64-bit)

## Package Versions

| Package      | Version   |
|--------------|-----------|
| polars       | 1.40.1    |
| pandas       | 2.2.2     |
| pyarrow      | 24.0.0    |
| duckdb       | 1.5.2     |
| numpy        | 2.4.6     |
| psutil       | 7.2.2     |
| matplotlib   | 3.10.9    |
| fastparquet  | not installed |
| pytables     | not installed |

## Notes

- HDF5 benchmarks skipped (pytables not installed).
- Memory measured via `psutil` process peak working set (`peak_wset` on Windows).
- Polars default thread count: 16 (all logical cores).
