# airflow_env
airflow_operators_dependencies_branching_taskgroup_labels



# WHY WSL V2

Airflow doesn’t run natively on Windows. We use **WSL2 + Ubuntu** so Airflow can run on Linux while you keep your Windows workflow.



# 1) Install WSL + Ubuntu (Windows side)

1. Open PowerShell (Administrator) → install Ubuntu:

   ```powershell
   wsl --install -d Ubuntu
   ```
2. Launch **Ubuntu** from Start Menu and create your Linux username/password.



# 2) Install Miniconda inside Ubuntu (WSL)

```bash
# clean any partial installs (safe)
rm -rf "$HOME/miniconda3" "$HOME/.conda" "$HOME/.condarc"

# download & silent install Miniconda
wget -qO "$HOME/miniconda.sh" https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash "$HOME/miniconda.sh" -b -p "$HOME/miniconda3"

# initialize conda for bash (current & future shells)
eval "$("$HOME/miniconda3/bin/conda" shell.bash hook)"
"$HOME/miniconda3/bin/conda" init bash
exec bash   # reload shell so (base) appears
```



# 3) Create a Conda env for Airflow

Conda may prompt for ToS; accept once per channel.

```bash
# if prompted previously:
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r

# create & activate env
conda create -n airflow python=3.9 -y
conda activate airflow
```



# 4) Install Airflow (+ pinned deps) and providers

```bash
AIRFLOW_VERSION=2.9.1
PYTHON_VERSION=3.9
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install apache-airflow-providers-amazon apache-airflow-providers-snowflake
```



# 5) Initialize Airflow & create a user

```bash
airflow db init

airflow users create \
  --username jjrex8988 \
  --firstname jj \
  --lastname rex8988 \
  --role Admin \
  --email your_email@example.com \
  --password <your_password>
```



# 6) Run Airflow (two terminals)

* **Terminal 1 (UI):**

  ```bash
  conda activate airflow
  airflow webserver -p 8080
  ```
* **Terminal 2 (scheduler):**

  ```bash
  conda activate airflow
  airflow scheduler
  ```

Open the UI at **[http://localhost:8080](http://localhost:8080)** and log in.



# 7) Create the DAGs folder & demo DAG

```bash
conda activate airflow
mkdir -p ~/airflow/dags
nano ~/airflow/dags/demo_dag.py
```



Paste:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="demo_dag",
    start_date=datetime(2025, 8, 21),
    schedule="@daily",
    catchup=False,
) as dag:
    hello = BashOperator(task_id="say_hello", bash_command="echo 'Hello from Airflow!'")
    bye = BashOperator(task_id="say_goodbye", bash_command="echo 'Goodbye from Airflow!'")
    hello >> bye
```


# 8) Run and verify

1. Refresh the UI; toggle `demo_dag` **ON**.
2. Click **Run** ▶ to trigger once.
3. In **Graph View**, open each task → **View Log** to see `echo` output.




# 9) Quick diagnostics we used

* List DAGs (confirms Airflow sees your file):

  ```bash
  airflow dags list | grep demo
  ```
* Check the configured DAGs folder:

  ```bash
  airflow config get-value core dags_folder
  ```
* If UI didn’t show the DAG: restart webserver, clear UI filters, ensure same `AIRFLOW_HOME`/env.


✅ Key difference

* `~/airflow` = Airflow’s own project folder (DAGs + logs + metadata).
* `~/miniconda3/envs/airflow` = Conda environment called “airflow” (Python + libraries).

They share the same name (**airflow**) because:

* Conda env: you named it `airflow`.
* Airflow home: by default, Airflow uses `$HOME/airflow`.




### 🔹 Conda environment (`~/miniconda3/envs/airflow`)

* Holds your **Python interpreter** + installed packages (Airflow, providers, boto3, etc.).
* Airflow runs inside whatever env you activate.
* If you rename this env → Airflow still works, as long as you activate the renamed one.


### 🔹 Airflow home (`~/airflow`)

* Holds your **DAG files, logs, SQLite DB** (not Python packages).
* Airflow doesn’t care what your Conda env is called.
* Your DAGs live in `~/airflow/dags` — they won’t move or disappear.


### ✅ Summary

* Conda env name = just how you call the Python environment.
* Airflow DAGs/logs = live in `~/airflow` folder, independent of the Conda env.
* Renaming the env won’t affect DAGs at all.



#### ** Use `conda rename` (if your conda supports it)**

1. Deactivate:

   ```bash
   conda deactivate
   ```
2. Run rename:

   ```bash
   conda rename -n airflow airflow_env(or anyname you like)
