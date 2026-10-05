# AI Disclosure — DA3408 Assignment 3

## Tool used
OpenAI ChatGPT was used during development.

## How AI was used
AI assistance was used for:
- translating preprocessing concepts between PySpark and Ray Data;
- explaining Spark/Ray APIs and distributed execution concepts;
- debugging implementation issues;
- suggesting performance-tuning approaches;
- helping structure the benchmark report and documentation.

## What was done manually
The following work was performed and verified by the student:
- Spark Docker cluster setup with two workers;
- Ray head/worker cluster setup and networking/authentication;
- dataset acquisition and local organization;
- execution of the final Spark and Ray benchmarks;
- monitoring with `top`;
- verification using Spark Master/Spark UI and Ray Dashboard;
- inspection of benchmark outputs and runtime results;
- interpretation of the performance results.

## Performance tuning note
AI assistance contributed to a tuning decision in the Ray implementation. A distributed location join introduced severe memory pressure on the constrained development machine. The final Ray pipeline therefore uses the small 265-row taxi-zone lookup as an in-memory Python dictionary and performs location enrichment in batch operations. Batch size and Ray block configuration were also tuned.

This optimization changes the internal execution strategy relative to Spark's relational join and is therefore disclosed rather than presented as exact implementation parity.

## Benchmark integrity
No synthetic benchmark results were generated or substituted for measurements. The final execution-time and resource values in the report came from actual runs on the real NYC Taxi dataset.

## Student understanding
The student is responsible for the final code, infrastructure configuration, execution, verification and interpretation and can explain the pipeline and deployment decisions used in the submission.
