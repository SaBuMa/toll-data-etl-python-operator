# 📂 Data Sources

All three files come from `tolldata.tgz`, published by IBM Skills Network.

| File | Format | Fields used | How it's read |
|---|---|---|---|
| `vehicle-data.csv` | Comma-separated, no header | Rowid, Timestamp, Anonymized Vehicle number, Vehicle type | `read_csv(usecols=[0,1,2,3])` |
| `tollplaza-data.tsv` | Tab-separated, **Windows line endings** | Number of axles, Tollplaza id, Tollplaza code | `read_csv(sep="\t", usecols=[4,5,6])` |
| `payment-data.txt` | **Fixed width**, 67 characters per line | Type of Payment code, Vehicle Code | `read_fwf(colspecs=[(58,61),(62,67)])` |

## Output files (staging folder)

| File | Columns | Produced by |
|---|---|---|
| `csv_data.csv` | 4 | `extract_data_from_csv` |
| `tsv_data.csv` | 3 | `extract_data_from_tsv` |
| `fixed_width_data.csv` | 2 | `extract_data_from_fixed_width` |
| `extracted_data.csv` | 9 | `consolidate_data` |
| `transformed_data.csv` | 9 (vehicle type uppercase) | `transform_data` |

## Final schema

```
 Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles,
 Tollplaza id, Tollplaza code, Type of Payment code, Vehicle Code

 1,Thu Aug 19 21:54:38 2021,125094,CAR,2,4856,PC7C042B7,PTE,VC965
```
