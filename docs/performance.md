# Performance: deliberately-bad vs optimized layout

Same ~312k-row dataset, two Iceberg layouts. `events_bad` is unpartitioned with
many tiny files, so every query scans the whole pile. `events_optimized` is
hidden-partitioned by `days(event_ts)` and `bucket(16, user_id)`, so user- and
date-filtered queries prune to only the matching files. Laptop-scale data, so the
absolute times are small; the ratios and the data-scanned reduction are the point.

## Table layout

| table | files | avg file size | total |
|---|--:|--:|--:|
| events_bad | 500 | 21.97 kB | 10.99 MB |
| events_optimized | 176 | 9.76 kB | 1.72 MB |

At this data scale the optimized table's files are actually *smaller* on
average (it spreads the same rows across day + user-bucket partitions). The
win is not bigger/compacted files — it is fewer files overall and, decisively,
a layout that lets queries skip the irrelevant ones (see scanned bytes below).

## Query before/after

| query | bad (ms) | optimized (ms) | speedup | bad scanned | optimized scanned |
|---|--:|--:|--:|--:|--:|
| user-filtered scan | 4818.2 | 2981.2 | 1.6x | 8.45 MB | 52.77 kB |
| date-range scan | 4904.4 | 3485.9 | 1.4x | 10.48 MB | 288 B |
| campaign rollup | 4864.3 | 2261.4 | 2.2x | 10.48 MB | 2.55 MB |

Wall-clock is the median of 5 runs through the Trino CLI (the fixed
docker-exec overhead is the same for both tables, so the comparison holds).
Bytes scanned is the physical input reported by `EXPLAIN ANALYZE`; the
optimized table reads far less on the user- and date-filtered queries because
partition pruning skips the non-matching files. The campaign rollup is a full
aggregate over all rows, so it reads everything either way — its only edge is
the optimized table's smaller file count, not pruning.

