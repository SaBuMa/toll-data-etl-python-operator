# 🛠️ Troubleshooting & Lessons Learned

Real issues hit while building this pipeline, and how each one was diagnosed.

### Function runs in a notebook but prints nothing
Defining a function (`def ...`) only writes the recipe card. Nothing runs until it is **called**:
`download_dataset()`. In Airflow, the operator does the calling.

### Task is green but the next task fails
The download printed "Failed" but returned normally, so Airflow marked it successful.
**Fix:** raise an exception (`response.raise_for_status()`), so the failure is reported
where it happens.

### `TypeError` when concatenating strings with numbers
`"Code: " + 404` fails. Use an f-string: `f"Code: {404}"`.

### Output shows `2, 4` instead of Rowid
`usecols=[1,2,3,4]` used bash's 1-based numbering. pandas columns start at **0**.

### Extra column `0,1,2...` and a header line `,0,1,2,3` in the CSV
`to_csv` writes the index and header by default. **Fix:** `header=False, index=False`.

### Output didn't match the code
A notebook cell was edited but not re-run (old function still in memory), or the output came from
an old file. **Always confirm the file is fresh** (`ls -l` timestamps) before debugging code.

### Windows line endings in the TSV
`cat -A` showed `^M$` at the end of every source line. pandas `read_csv` treats `\r\n` as a
line break, so the output ends in a clean `$`, verified on **both** input and output.

### Fixed-width: rows shifted by one column
Splitting on a single space (`delimiter=' '`) turns every run of padding into empty fields, and
the padding length changes with the data (a 7-digit vs 6-digit vehicle number).
**Fix:** read by position with `pd.read_fwf(..., colspecs=[(58, 61), (62, 67)])`.

### `read_fwf` used the first row as column names
Without `header=None`, the first data line became the header (`KeyError` when selecting by number).

### `DeprecationWarning` from `tarfile.extractall`
Python 3.12+ warns that 3.14 will filter archives by default. **Fix:** `filter="data"`,
the setting recommended for archives you did not create.

### macOS `._` files in the archive
AppleDouble metadata created by macOS archiving. Skipped with a `members=` list that filters
names starting with `._`.

### `pd.concat` aligns by index, not by line
Side-by-side concat matches rows by **index label**. If one extract had fewer rows, cells would
silently become empty. The enhanced DAG raises an error when row counts differ.
