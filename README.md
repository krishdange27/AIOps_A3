# DA3408 A3 — Spark vs. Ray: The Data Engineering Duel

## Overview
This project implements the requested NYC Yellow Taxi preprocessing workflow in both PySpark and Ray Data and benchmarks the two frameworks on the real multi-month TLC dataset.

### Pipeline
1. Ingest monthly Parquet files.
2. Remove required null records and duplicates.
3. Derive trip duration and average speed.
4. Enrich pickup/drop-off Location IDs with zone and borough information.
5. Select the structured output schema.
6. Export the result as Parquet.

## Dataset
- NYC Yellow Taxi data, January 2023–December 2024.
- 24 monthly Parquet files.
- 79,479,946 input rows.
- Taxi zone lookup CSV with 265 locations.
- The 100,000-row development subset was used only for development/UDF investigation.

## Infrastructure
### Spark
- Docker-based Spark cluster.
- 1 master + 2 worker containers.
- 2 active workers verified through Spark Master UI.
- Spark application evidence captured through the Spark UI.

### Ray
- 1 Ray head + 2 Ray worker nodes.
- 3 active Ray nodes in total.
- 4 aggregate Ray CPUs on the configured cluster.
- Cluster evidence captured through the Ray Dashboard.

## Final benchmark
| Metric | Spark | Ray |
|---|---:|---:|
| Full execution time | 252.970 s | 1260.419 s |
| Relative time | 1.00× | 4.98× |
| Peak process CPU | 323.5% | 101.3% |
| Peak process memory | 16.4% | 11.6% |
| Peak RES | ~1.9 GB | ~1.4 GB |

The benchmark therefore gives Spark the measured performance win on this machine, dataset and configuration.

## UDF Deep-Dive
An isolated speed-UDF experiment was performed on the 100,000-row real development subset:
- Spark: 0.316 s
- Ray: 0.440 s

This experiment is explicitly a development-scale UDF measurement and is not presented as a full 79.5M-row UDF timing.

Ray's Python-native execution model avoids Spark's JVM-to-Python UDF boundary in principle, but Python-native execution does not automatically guarantee lower wall-clock time. In this controlled experiment, Spark was slightly faster.

## Important implementation notes
- Spark performs `dropDuplicates()` per monthly input file.
- Ray uses batch-level duplicate removal rather than a full global shuffle.
- A distributed Ray join was tested but caused severe memory pressure on the constrained development VM. The final Ray implementation uses a small local lookup dictionary for location enrichment instead. This is documented as a performance-tuning choice and means the two implementations are not perfectly identical internally.
- The benchmark values are from actual runs on the real dataset; no synthetic benchmark values were used.

## Evidence
Relevant screenshots include:
- Spark Master UI showing two active workers.
- Spark application Jobs/Stages UI.
- Ray Dashboard Cluster view showing three active nodes.
- Ray runtime cluster view.

See the `evidence/` directory in the submission repository for the captured screenshots.

## Framework conclusion
For this BI/data-engineering benchmark, **Spark is the winner**, with a measured runtime of 252.970 s versus 1260.419 s for Ray.

For AI-first workloads, Ray remains attractive because of its Python-native execution model and broader fit with Python-based distributed computation. For the workload measured here, however, Spark provided substantially better end-to-end performance.
