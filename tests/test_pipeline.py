"""
End-to-end tests for the enhanced DAG's Python functions.

No Airflow installation and no internet needed:
  * Airflow is replaced by a tiny stub (only DAG and PythonOperator are used)
  * requests.get is mocked to return a sample archive built in the same
    formats as the real data (CSV, TSV with \\r\\n endings, 67-char fixed width,
    plus macOS '._' metadata files)

Run from the repository root:
    python3 -m unittest discover -s tests -v
"""
import importlib.util
import io
import os
import sys
import tarfile
import tempfile
import types
import unittest
from unittest import mock

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAG_FILE = os.path.join(ROOT, "dags", "enhanced", "ETL_toll_data_enhanced.py")


# ---------------------------------------------------------------------------
# Minimal Airflow stub
# ---------------------------------------------------------------------------
class _StubOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.downstream = set()

    def __rshift__(self, other):
        targets = other if isinstance(other, list) else [other]
        for t in targets:
            self.downstream.add(t.task_id)
        return other

    def __rrshift__(self, other):  # [a, b, c] >> self
        for t in other:
            t.downstream.add(self.task_id)
        return self


class _StubDAG:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _install_airflow_stub():
    airflow = types.ModuleType("airflow")
    airflow.DAG = _StubDAG
    operators = types.ModuleType("airflow.operators")
    python = types.ModuleType("airflow.operators.python")
    python.PythonOperator = _StubOperator
    sys.modules.update({
        "airflow": airflow,
        "airflow.operators": operators,
        "airflow.operators.python": python,
    })


# ---------------------------------------------------------------------------
# Sample data in the real formats
# ---------------------------------------------------------------------------
VEHICLE_CSV = (
    "1,Thu Aug 19 21:54:38 2021,125094,car,2,VC965\n"
    "2,Sat Jul 31 04:09:44 2021,174434,truck,4,VC965\n"
    "3,Sat Aug 14 17:19:04 2021,8538286,van,2,VC965"          # no trailing newline
)
TOLLPLAZA_TSV = (
    "1\tThu Aug 19 21:54:38 2021\t125094\tcar\t2\t4856\tPC7C042B7\r\n"
    "2\tSat Jul 31 04:09:44 2021\t174434\ttruck\t4\t4154\tPC2C2EF9E\r\n"
    "3\tSat Aug 14 17:19:04 2021\t8538286\tvan\t2\t4070\tPCEECA8B2\r\n"
)
PAYMENT_TXT = (
    "     1 Thu Aug 19 21:54:38 2021 125094     4856 PC7C042B7 PTE VC965\n"
    "     2 Sat Jul 31 04:09:44 2021 174434     4154 PC2C2EF9E PTP VC965\n"
    "     3 Sat Aug 14 17:19:04 2021 8538286    4070 PCEECA8B2 PTE VC965\n"
)


def _build_archive():
    buffer = io.BytesIO()
    files = {
        "fileformats.txt": "sample",
        "vehicle-data.csv": VEHICLE_CSV,
        "._vehicle-data.csv": "mac metadata",
        "tollplaza-data.tsv": TOLLPLAZA_TSV,
        "._tollplaza-data.tsv": "mac metadata",
        "payment-data.txt": PAYMENT_TXT,
    }
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, text in files.items():
            data = text.encode("utf-8")
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class _FakeResponse:
    def __init__(self, content, status=200):
        self.content = content
        self.status_code = status
        self.elapsed = "0:00:00.01"

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code} error")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class PipelineTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        _install_airflow_stub()
        cls.tmp = tempfile.TemporaryDirectory()
        os.environ["TOLL_STAGING"] = os.path.join(cls.tmp.name, "staging")
        spec = importlib.util.spec_from_file_location("etl_enhanced", DAG_FILE)
        cls.etl = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.etl)

        archive = _build_archive()
        with mock.patch.object(cls.etl.requests, "get", return_value=_FakeResponse(archive)):
            cls.etl.download_dataset()
        cls.etl.untar_dataset()
        cls.etl.extract_data_from_csv()
        cls.etl.extract_data_from_tsv()
        cls.etl.extract_data_from_fixed_width()
        cls.etl.consolidate_data()
        cls.etl.transform_data()

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def _read(self, path):
        return pd.read_csv(path, header=None)

    def test_untar_skips_macos_metadata(self):
        names = os.listdir(self.etl.STAGING)
        self.assertIn("vehicle-data.csv", names)
        self.assertFalse([n for n in names if n.startswith("._")])

    def test_csv_extract(self):
        df = self._read(self.etl.OUTPUT_CSV)
        self.assertEqual(df.shape, (3, 4))
        self.assertEqual(df.iloc[0].tolist(), [1, "Thu Aug 19 21:54:38 2021", 125094, "car"])

    def test_tsv_extract_has_no_carriage_return(self):
        with open(self.etl.OUTPUT_TSV, "rb") as f:
            self.assertNotIn(b"\r", f.read())
        self.assertEqual(self._read(self.etl.OUTPUT_TSV).shape, (3, 3))

    def test_fixed_width_reads_by_position(self):
        df = self._read(self.etl.OUTPUT_FIXED_WIDTH)
        # line 3 has a longer vehicle number: positions must still be right
        self.assertEqual(df.values.tolist(),
                         [["PTE", "VC965"], ["PTP", "VC965"], ["PTE", "VC965"]])

    def test_transformed_file(self):
        df = self._read(self.etl.TRANSFORMED)
        self.assertEqual(df.shape, (3, 9))
        self.assertEqual(df[3].tolist(), ["CAR", "TRUCK", "VAN"])
        self.assertEqual(df.iloc[0].tolist(),
                         [1, "Thu Aug 19 21:54:38 2021", 125094, "CAR",
                          2, 4856, "PC7C042B7", "PTE", "VC965"])

    def test_no_index_or_header_written(self):
        with open(self.etl.TRANSFORMED) as f:
            self.assertTrue(f.readline().startswith("1,Thu Aug 19"))

    def test_validate_output_passes(self):
        self.etl.validate_output()

    def test_validate_output_catches_lowercase(self):
        df = self._read(self.etl.TRANSFORMED)
        df.loc[0, 3] = "car"
        bad = os.path.join(self.tmp.name, "bad.csv")
        df.to_csv(bad, header=False, index=False)
        with mock.patch.object(self.etl, "TRANSFORMED", bad):
            with self.assertRaises(ValueError):
                self.etl.validate_output()

    def test_consolidate_refuses_mismatched_row_counts(self):
        short = os.path.join(self.tmp.name, "short.csv")
        self._read(self.etl.OUTPUT_TSV).head(2).to_csv(short, header=False, index=False)
        with mock.patch.object(self.etl, "OUTPUT_TSV", short):
            with self.assertRaises(ValueError):
                self.etl.consolidate_data()

    def test_download_failure_raises(self):
        import requests
        with mock.patch.object(self.etl.requests, "get", return_value=_FakeResponse(b"", 404)):
            with self.assertRaises(requests.HTTPError):
                self.etl.download_dataset()

    def test_dag_structure_fan_out_fan_in(self):
        self.assertEqual(self.etl.task_untar_dataset.downstream,
                         {"extract_data_from_csv", "extract_data_from_tsv",
                          "extract_data_from_fixed_width"})
        for task in (self.etl.task_extract_csv, self.etl.task_extract_tsv,
                     self.etl.task_extract_fixed):
            self.assertEqual(task.downstream, {"consolidate_data"})
        self.assertEqual(self.etl.task_transform_data.downstream, {"validate_output"})


if __name__ == "__main__":
    unittest.main()
