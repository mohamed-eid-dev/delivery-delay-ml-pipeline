# MLOps Workshop — Python ETL, Medallion Architecture & Apache Airflow

**Level:** Beginner / early MLOps | **Session:** 3 hours | **Language:** Arabic instructor materials + readable Python code

A complete, **small** classroom project with two independent Airflow DAGs:

```text
Daily source CSV (Retail Delivery Orders)
        |
        v
DAG 1: retail_data_pipeline (@daily)
        |
        +--> extract_to_bronze  (copy raw data)
        +--> transform_to_silver (clean rows and remove duplicates)
        +--> prepare_gold      (select ML-ready columns)
        +--> validate_gold     (quality checks)
        |
        v
Gold CSV files: data/gold/orders_YYYY-MM-DD.csv
        |
        v
DAG 2: retail_ml_training_pipeline (manually triggered)
        |
        +--> prepare_features  (combine Gold partitions)
        +--> train_model       (Logistic Regression)
        +--> evaluate_model    (Accuracy / Precision / Recall / F1)
        +--> save_model        (Joblib artifact)
```

**Problem:** Predict `late_delivery` (0 = on time, 1 = late) from order features.

**Important architecture choice:** This is not one giant DAG. DAG 1 runs daily. DAG 2 runs on demand; weekly scheduling can be added after the demo. DAG 2 deliberately does **not** auto-trigger when DAG 1 succeeds, to keep the first Airflow lesson understandable. The Gold files form the simple interface between them.

## 1. Project files

```text
MLOps_Airflow_Medallion_Workshop/
├── generate_data.py                 # Rebuild daily input CSVs
├── data/
│   ├── raw/                        # 46 packaged source daily CSVs
│   ├── bronze/                     # Exact copies of source CSVs
│   ├── silver/                     # Clean data
│   ├── gold/                       # ML-ready data
│   └── ml/                         # Combined train/test CSVs
├── src/
│   ├── etl.py                      # Pure Python ETL stages
│   ├── train.py                    # Pure Python ML stages
│   └── ml_pipeline.py              # Airflow-compatible ML import
├── dags/
│   ├── 01_data_pipeline.py         # DAG 1 (4 tasks)
│   └── 02_ml_training_pipeline.py  # DAG 2 (4 tasks)
├── scripts/
│   ├── run_without_airflow.py      # Understand the logic first
│   └── show_results.py             # View rows and metrics
├── tests/test_pipeline.py          # Example tests
├── materials/
│   ├── instructor_guide_ar.md
│   ├── student_handout_ar.md
│   └── slide_content_ar.md
├── DockerFile.airFlow
├── docker-compose.yml
└── requirements-local.txt
```

## 2. Quick start with Docker (Airflow)

Prerequisite: Docker Desktop (Mac/Windows) or Docker Engine + Compose (Linux). Docker should be running.

Unzip the project and open a terminal **inside the project folder**.

**Mac / Linux:** (make writable file ownership match your host user)

```bash
printf 'AIRFLOW_UID=%s\n' "$(id -u)" > .env
docker compose up --build -d
```

**Windows PowerShell:**

```powershell
docker compose up --build -d
```

The first build must download the Airflow image and Python libraries. After it starts:

```bash
docker compose logs -f airflow
```

In another terminal, retrieve the auto-generated Airflow password:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Open **http://localhost:8080** and log in with username **`admin`** and the password in that file. Airflow standalone does not use a fixed `admin/admin` password.

> **Note:** `airflow standalone` is for local teaching only (SQLite + an all-in-one service). It is not a production architecture. A real deployment would normally use separate Airflow components, a proper metadata database, access control and persistent storage.

## 3. Classroom demo: run DAG 1 for 2026-10-20

1. In Airflow UI, locate **`retail_data_pipeline`** and enable/unpause it.
2. Select **Trigger** (manual Dag Run).
3. Enter these run parameters (the UI may show editable fields or a JSON configuration):

```json
{"run_date": "2026-10-20", "demo_retry": false}
```

4. Open **Graph**. Follow the 4 tasks as they complete.
5. Open **Logs** for `transform_to_silver`. For 2026-10-20 it reports `196 raw rows -> 176 clean rows`.
6. Look at `data/bronze/orders_2026-10-20.csv`, `data/silver/...` and `data/gold/...` in your local editor. Mounted folders are shared with Docker.

**If no parameters screen appears:** Manually trigger the DAG without a date override: it uses the Airflow run's data interval start date. Make sure there is a matching file under `data/raw/` (the pipeline also accepts `data/source/`), or use the UI's config JSON option. The packaged dataset spans **2026-10-01 through 2026-11-15**.

## 4. Run three dates and then DAG 2

Trigger DAG 1 with the following `run_date` values, one at a time (you can reuse 2026-10-20):

```text
2026-10-20
2026-10-21
2026-10-22
```

Now manually trigger **`retail_ml_training_pipeline`**. The ML DAG:

- Finds the available Gold files.
- Combines Gold data from available dates, removes duplicates.
- Splits data into train/test (25% test).
- Fits `StandardScaler + LogisticRegression` as one scikit-learn pipeline.
- Writes `metrics/metrics.json` and `models/late_delivery_model.joblib`.

**Why scale in the ML step?** Scaling is a *fitted* preprocessing step. It should be fitted on the training data only, avoiding leakage from the test set.

**Note:** If Airflow has already run other daily intervals, the ML DAG includes their Gold files too, so you may see more than the 577 rows in the packaged 3-day demo.

## 5. Retry demo (intentional failure)

Trigger `retail_data_pipeline` with:

```json
{"run_date": "2026-10-23", "demo_retry": true}
```

The `extract_to_bronze` task **intentionally fails once per Dag Run** and creates a tiny marker in `data/demo/`. The task is configured with `retries=2` and `retry_delay=10 seconds`, so it retries and then succeeds. Show the students the task attempts and logs.

**Do not use fail-on-purpose code in production.** It is a temporary classroom demo.

## 6. Idempotency demo

Trigger the same `run_date` again, for example `2026-10-20`.

- `bronze` copies the raw file again, without changing its contents.
- `silver` removes duplicate `order_id`s and **overwrites the dated output**.
- `gold` overwrites the same dated output.
- As a result, 2026-10-20's Gold file still contains **176 clean orders**, not 352.

You can also show the unit test:

```bash
# On the host (if packages installed)
python -m pytest tests -q
```

## 7. Backfill demo (not the same as manual triggering)

Since DAG 1 has the time-based `@daily` schedule, it supports true Airflow **backfills**.

In Airflow 3.1, use the DAG details page → **Trigger** → **Backfill**; choose:

```text
From: 2026-10-24
To:   2026-10-26
Reprocessing: Missing runs
Max active runs: 1
```

The task uses `data_interval_start` to select each historical date, unless you explicitly set a `run_date` parameter. **For a real backfill, leave `run_date` empty.** Dates are interpreted according to the DAG's UTC schedule.

Or use the CLI:

```bash
docker compose exec airflow airflow backfill create \
  --dag-id retail_data_pipeline \
  --from-date 2026-10-24 \
  --to-date 2026-10-26 \
  --reprocess-behavior none \
  --max-active-runs 1
```

Backfill creates DAG runs **for historical schedule intervals**. Manually triggering a DAG three times with different `run_date` values is a useful demo, but it is **not the same mechanism** as an Airflow backfill.

## 8. No Docker? Run the Python logic alone

This is useful for the **first 30–35 minutes** of the hands-on, before introducing Airflow.

```bash
python -m pip install -r requirements-local.txt
python -m scripts.run_without_airflow
python -m scripts.show_results
python -m pytest tests -q
```

The script uses 2026-10-20, 21, 22. The packaged source files contain 196, 209, and 222 rows respectively; after cleaning they produce 176, 193, and 208 Gold rows (577 rows together). Metrics vary from the actual generated distribution and train/test split; orchestration and reproducibility are the workshop goals.

The host and Docker share the same `data/`, `models/` and `metrics/` directories. If you want students to see all outputs appear only from Airflow, delete generated contents from Bronze/Silver/Gold/ML, models and metrics **before** starting the Airflow demo. The distributed package starts with empty output directories.

## 9. Generate more daily source data

```bash
python generate_data.py --start 2026-11-16 --days 10
```

This creates another 10 daily CSV files. The generator is deterministic for each date. **Never overwrite source files with genuine data**; this command is for the classroom's synthetic dataset only.

## 10. Optional: weekly ML training

Change the ML DAG line from `schedule=None` to:

```python
schedule="0 9 * * 1"  # Monday 09:00 UTC
```

For production, don't assume a weekly clock event guarantees validated new Gold data. Use appropriate data readiness checks, or add Airflow Assets in a later lesson.

## 11. Common issues

- **UI not ready:** inspect `docker compose logs -f airflow`.
- **Port 8080 occupied:** change `127.0.0.1:8080:8080` to `127.0.0.1:8081:8080` in Compose and browse localhost:8081.
- **DAG absent:** confirm `dags/` files are mounted and `PYTHONPATH=/opt/airflow`, then check logs.
- **Permission denied (Linux):** make sure `.env` has `AIRFLOW_UID=$(id -u)` and folders are writable by that UID.
- **Model DAG fails / no Gold data:** run the Data DAG for at least 2 dates first.
- **Missing source date:** generate it with `generate_data.py` or use a date from the provided range.
- **Need password again:** run the password command in section 2.
- **Stop containers:** `docker compose down`. Removing/recreating the standalone container loses Airflow's SQLite metadata/history; CSV and model outputs remain on the host.

## 12. Concept summary

| Topic | Here, in the workshop |
|---|---|
| ETL | Python extracts source CSV, transforms rows, and writes cleaned results |
| ELT-style layering | Raw source is loaded unchanged into Bronze before later transforms |
| Medallion | Bronze (raw), Silver (clean), Gold (fit for ML consumption) |
| DAG | The graph describing what runs and in what order |
| Task | One Python function managed by Airflow |
| Schedule | `@daily` for data; manual/on-demand for ML |
| Retry | Repeat a failed task after a delay |
| Idempotency | Reprocessing the same date does not create duplicate rows |
| Backfill | Create Dag Runs for past intervals missed by the schedule |
| ML Pipeline | Prepare features, fit model, measure quality, save model |

**Learning scope:** The lesson saves a trained model. Automatic API deployment, Model Registry, batch inference, temporal model validation, and continuous training are later MLOps topics.

Official references:
- Airflow Docker: https://airflow.apache.org/docs/apache-airflow/3.1.8/howto/docker-compose/
- Airflow Task SDK: https://airflow.apache.org/docs/task-sdk/stable/
- Airflow Dag Runs / Backfill: https://airflow.apache.org/docs/apache-airflow/3.1.8/core-concepts/dag-run.html
