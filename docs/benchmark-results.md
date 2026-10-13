# Recorded benchmark run

One real execution of `python scripts/benchmark.py` (default options) during development.
These are measurements from that machine only — not a performance claim.

* Machine: Linux 6.18 x86_64 virtual machine, 2 vCPUs, 7 GiB RAM, Python 3.13.16
* Topology: coordinator + 3 storage nodes (4 for the recovery test), all on one machine over loopback TCP
* Chunk size 1 MiB; each file size uploaded/downloaded 3 times, medians shown
* Timestamp: 2026-10-12T18:07 (UTC)

```

== file operations, RF=3 ==
    64.0KiB: upload      8.5 ms (    7.3 MiB/s)   download      4.6 ms (   13.7 MiB/s)
     1.0MiB: upload     24.0 ms (   41.6 MiB/s)   download     12.3 ms (   81.4 MiB/s)
     8.0MiB: upload    155.6 ms (   51.4 MiB/s)   download     82.7 ms (   96.7 MiB/s)
    32.0MiB: upload    573.9 ms (   55.8 MiB/s)   download    287.8 ms (  111.2 MiB/s)

== raw TCP chunk ops (1.0MiB) ==
  store median 4.95 ms, retrieve median 1.55 ms

== concurrency (8 clients x 3 upload+download of 1.0MiB) ==
  21.53 round-trips/s, median 215.9 ms, aggregate 43.1 MiB/s

== file operations, RF=1 ==
    64.0KiB: upload      9.4 ms (    6.6 MiB/s)   download      4.6 ms (   13.7 MiB/s)
     1.0MiB: upload     15.6 ms (   64.2 MiB/s)   download     12.3 ms (   81.2 MiB/s)
     8.0MiB: upload    112.3 ms (   71.2 MiB/s)   download     68.7 ms (  116.5 MiB/s)
    32.0MiB: upload    411.5 ms (   77.8 MiB/s)   download    259.5 ms (  123.3 MiB/s)

== replication overhead (median upload time RF=3 / RF=1) ==
    64.0KiB: x0.91
     1.0MiB: x1.54
     8.0MiB: x1.39
    32.0MiB: x1.39

== recovery after SIGKILL of one of 4 nodes (32.0MiB file) ==
  detection 2.79 s, re-replication 0.23 s (28 replicas, 119.8 MiB/s), crash -> full RF 3.02 s

```

Notes on reading the numbers:

* Upload time includes multipart parsing, staging the file to a coordinator temp file, hashing,
  and three verified replica writes with fsync per chunk.
* Download time includes fetching every chunk over TCP, verifying each chunk and the whole file,
  staging to a temp file, and streaming it back.
* "Replication overhead" is the ratio of median upload times with RF=3 vs RF=1 on the same
  3-node cluster; replica writes for one chunk run in parallel, so it is well below 3x.
* Recovery: detection time is bounded by the configured `failure_timeout` (3 s in this run);
  re-replication is the time from DOWN detection until every chunk is back at RF=3.
* Small-file numbers (64 KiB) are dominated by per-request overhead, so run-to-run variation is large.
