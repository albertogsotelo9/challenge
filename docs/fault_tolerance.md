# Fault Tolerance Strategy in Spark


---

##  Node Failure Recovery

- Spark automatically handles executor failures by rescheduling tasks on other available nodes.
- Cluster managers like YARN, Kubernetes, or Databricks help maintain cluster resilience and handle node replacement.

---

## Task Retry Mechanism

- Controlled via `spark.task.maxFailures` (default is 4).
- When a task fails (e.g., due to network or disk I/O), Spark does not fail the entire job immediately. It retries the task on another executor.
- Ensures robustness against transient infrastructure issues.

---

##  Checkpointing

- `.checkpoint()` to persist long lineage DataFrames to reliable storage (e.g., DBFS, HDFS).
- Breaks the lineage chain and improves fault recovery.
- Benefits:
  - Avoids recomputation of all previous transformations.
  - Makes recovery faster and more reliable.

---

##  Speculative Execution

- Via `spark.speculation=true`.
- Spark runs a duplicate (speculative) task for slow-running tasks (due to skew or shared resource).
- When some tasks are slower than others, spark can throw a copy (speculative task) within another executor. The one which finish first, win.
- Helps address straggler tasks that delay overall stage completion.

---

##  Job Restart Strategy

- Orchestration tools like Airflow, Databricks Workflows to:
  - Detect failures
  - Automatically retry jobs or steps

---

##  Idempotent Writes

- Ensures data consistency even under retries or failure.
- Implemented by:
  - Writing data in overwrite mode or to a temporary staging location, then moving to final destination.
  - Avoiding `.mode("append")` unless we're enforcing deduplication.

---

##  Failure Isolation

- Each processing stage (e.g., cleaning, filtering, aggregation) is modularized.
- This allows:
  - Easier debugging and reruns
  - Targeted retries using orchestrators

---
