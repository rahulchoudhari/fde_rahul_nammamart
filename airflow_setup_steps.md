# Airflow Setup — Meridian Pipeline Live Demo (Astro CLI)

Do all of this **before class** (Phase 0 prep). Never run these commands live in front of the cohort.

## Prerequisites
1. Install Docker Desktop and make sure it's **running** (Astro runs Airflow in Docker containers locally).
2. Make sure Docker has the **buildx** plugin (BuildKit). The Astro Runtime image uses `RUN --mount` build steps, which fail without it. Docker Desktop includes buildx already; if you use Homebrew's `docker` CLI with Colima instead, install it yourself:
   ```
   brew install docker-buildx
   mkdir -p ~/.docker/cli-plugins
   ln -sfn "$(brew --prefix)/opt/docker-buildx/bin/docker-buildx" ~/.docker/cli-plugins/docker-buildx
   ```
   Check: `docker buildx version` should print a version.
3. Install the Astro CLI:
   - macOS: `brew install astro`
   - Windows/Linux: `curl -sSL install.astronomer.io | sudo bash`
4. Confirm install: `astro version`

## Step-by-step

**1. Create the project folder**
```
mkdir meridian-airflow
cd meridian-airflow
```

**2. Initialize the Astro project**
```
astro dev init
```
This creates: `dags/`, `include/`, `plugins/`, `Dockerfile`, `requirements.txt`, `packages.txt`, `airflow_settings.yaml`.

**3. Add the Python dependencies**
Open `requirements.txt` and add:
```
pandas
duckdb
```

**4. Add the data folder**
```
mkdir -p include/data
```
Copy `sales.csv`, `customers.csv`, `inventory.csv` into `include/data/`.

**5. Add the DAG**
Copy `meridian_pipeline.py` into the `dags/` folder.

**6. Start Airflow**
```
astro dev start
```
This spins up the webserver, scheduler, triggerer, and metadata Postgres in Docker. First start takes a few minutes — this is exactly why you do it before class, not during.

**7. Open the UI**
Go to the **Airflow UI** URL that `astro dev start` prints (newer Astro CLI versions use a `<project>.localhost:<port>` address rather than `http://localhost:8080`). Default login is `admin` / `admin`.

**8. Confirm the DAG is registered**
You should see `meridian_pipeline` in the DAGs list, unpaused, with no import errors. If there's an import error, check the requirements.txt packages installed correctly (`astro dev restart` after editing requirements.txt).

**9. Do one full dry run before class**
Trigger it manually from the UI (the ▶ play button) or:
```
astro dev bash
airflow dags trigger meridian_pipeline
```
Watch it complete green end-to-end once, so you know it works before the cohort ever sees it.

**10. Reset for the live session**
You don't need to reset anything — Airflow keeps run history, which is actually useful (you can show a prior successful run if you want a fallback view). Just leave the environment running and switch to the browser tab when you reach Phase 3 (DAGs, 0:30–0:50) in the session.

## During the live demo
- Switch to the `http://localhost:8080` tab (already logged in).
- Open `meridian_pipeline` → **Graph** view.
- Click **Trigger DAG**.
- Narrate as tasks light up: the three `extract_*` tasks running in parallel (yellow → green together), then `validate` waiting on all three, then `transform`, then `load_to_warehouse`.
- Click into the `validate` task's **Logs** to show the printed data quality issue counts live — this doubles as a preview hook for Session 2.

## After class
```
astro dev stop
```
Stops the containers without deleting them, so you can `astro dev start` again for Session 2 without redoing setup.

## Troubleshooting
- **`the --mount option requires BuildKit`** (often after a `DEPRECATED: The legacy builder is deprecated` warning): the buildx plugin is missing. Follow Prerequisite 2 above, then run `astro dev start` again.

## Fallback
If Docker/Airflow has any issue on the day, fall back to the screenshots taken during your Step 9 dry run — same DAG, same green graph, no live dependency.
