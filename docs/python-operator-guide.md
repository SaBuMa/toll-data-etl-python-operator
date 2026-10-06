# 👨‍🍳 PythonOperator Explained: The Kitchen Analogy

A mental model for how Airflow runs Python functions, and the ideas this project is built on.

## The kitchen

Airflow is the **head chef** holding the order ticket (the DAG). Each operator is a **cook**:

| Airflow | Kitchen | In this project |
|---|---|---|
| DAG | Order ticket: what to cook, in what order | `ETL_toll_data` |
| `BashOperator` | Cook who receives a note with a terminal command | used in the [bash version](https://github.com/SaBuMa/toll-data-etl-airflow) |
| `PythonOperator` | Cook who receives a **recipe card** (a Python function) | every task here |
| Task log | The cook's notes | every function `print()`s what it did |

## Recipe card vs cooked plate

```
 python_callable=download_dataset     → 📝 hand over the recipe card
                                        Airflow cooks it when the task runs ✅

 python_callable=download_dataset()   → 🍽️ you cooked it yourself, right now,
                                        while the scheduler was just READING the file
                                        and handed Airflow the plate (None) ❌
```

The scheduler re-reads DAG files every few seconds. With `()`, the download would run on
every read, and the task itself would have nothing to call.

## The smiling cook: how Airflow knows a task failed

The head chef doesn't read the cook's notes. It only checks **how the cook came back**:

```
 function returns normally  →  🟢 success   (even if it printed "Failed!")
 function raises an error   →  🔴 failed    → retries kick in
```

So every failure must **raise**:

```
 response.raise_for_status()   → HTTP 4xx/5xx    → HTTPError 🚨
 requests.get(..., timeout=30) → server silent   → Timeout 🚨
 open(missing_file)            → no input        → FileNotFoundError 🚨
 validate_output()             → bad final data  → ValueError 🚨
```

`raise_for_status()` is a **smoke detector**, not a question: silent when things are fine,
alarm when they aren't. That's why `download_dataset` has no `if`.

## The hotel room: always use absolute paths

Each task runs in a temporary working directory that is cleaned afterwards:

```
 open("tolldata.tgz", "wb")                 → lands in /tmp/airflowtmpXYZ/ → deleted 🧹
 open(os.path.join(STAGING, "tolldata.tgz")) → lands in staging, next task finds it ✅
```

That's why every path is built once from a single `STAGING` constant.

## Two rulers: 1-based vs 0-based

```
 bash  cut -f1-4 / cut -c 59-61   → counts from 1, end INCLUDED   (piso 1)
 pandas usecols=[0,1,2,3]         → counts from 0                  (floor 0)
 pandas colspecs=(58, 61)         → counts from 0, end EXCLUDED

 Same characters: cut -c 59-61  ==  colspecs (58, 61)
 Rule: start moves down by one, end stays the same.
```

## Sequential vs fan-out

```
 Submission:  download → untar → csv → tsv → fixed → consolidate → transform

 Enhanced:                      ┌→ csv   ─┐
              download → untar ─┼→ tsv   ─┼→ consolidate → transform → validate
                                └→ fixed ─┘
```

The three extracts read and write **different files**, so running them in parallel is safe.
