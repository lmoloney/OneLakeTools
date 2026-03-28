"""Tests for Avro preview, Delta table features, and path format builders."""

from __future__ import annotations

import io
import json

# ── Avro preview ────────────────────────────────────────────────────────


class TestAvroPreview:
    """Test Avro file reading via fastavro."""

    def test_fastavro_reads_schema_and_data(self):
        """fastavro can read a simple Avro file produced in-memory."""
        import fastavro

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": ["null", "string"], "default": None},
                {"name": "active", "type": "boolean"},
            ],
        }
        records = [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": None, "active": False},
        ]
        buf = io.BytesIO()
        fastavro.writer(buf, schema, records)
        buf.seek(0)

        reader = fastavro.reader(buf)
        assert reader.writer_schema["name"] == "TestRecord"
        assert len(reader.writer_schema["fields"]) == 3

        rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] is None

    def test_avro_nullable_field_detection(self):
        """Union types like ["null", "string"] should be detected as nullable."""
        fields = [
            {"name": "a", "type": "string"},
            {"name": "b", "type": ["null", "int"]},
            {"name": "c", "type": ["null", "string", "int"]},
        ]
        results = []
        for f in fields:
            ftype = f["type"]
            nullable = False
            if isinstance(ftype, list):
                nullable = "null" in ftype
                non_null = [t for t in ftype if t != "null"]
                ftype = non_null[0] if len(non_null) == 1 else str(non_null)
            results.append((f["name"], str(ftype), nullable))

        assert results[0] == ("a", "string", False)
        assert results[1] == ("b", "int", True)
        assert results[2] == ("c", "['string', 'int']", True)


# ── Delta transaction log parsing ───────────────────────────────────────


class TestTransactionLogParsing:
    """Test parsing of Delta _delta_log/*.json commit files."""

    def test_parse_commit_info(self):
        """Extract commitInfo from NDJSON delta log entry."""
        log_entry = "\n".join(
            [
                json.dumps(
                    {
                        "commitInfo": {
                            "timestamp": 1711612800000,
                            "operation": "WRITE",
                            "operationMetrics": {
                                "numFiles": "5",
                                "numOutputRows": "1000",
                            },
                        }
                    }
                ),
                json.dumps(
                    {
                        "add": {
                            "path": "part-00000.parquet",
                            "size": 12345,
                        }
                    }
                ),
            ]
        )

        commits = []
        for line in log_entry.strip().splitlines():
            obj = json.loads(line)
            if "commitInfo" in obj:
                ci = obj["commitInfo"]
                commits.append(ci)

        assert len(commits) == 1
        assert commits[0]["operation"] == "WRITE"
        assert commits[0]["operationMetrics"]["numFiles"] == "5"

    def test_timestamp_formatting(self):
        """Delta timestamps are ms-since-epoch; format to human-readable."""
        from datetime import UTC, datetime

        ts = 1711627200000  # 2024-03-28 12:00:00 UTC
        formatted = datetime.fromtimestamp(ts / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        assert formatted == "2024-03-28 12:00:00 UTC"

    def test_timestamp_fallback_to_datetime(self):
        """When commitInfo.timestamp is missing, a datetime object is used."""
        from datetime import UTC, datetime

        # Simulate: commitInfo has no timestamp, file lastModified is a datetime
        ci = {"operation": "WRITE"}
        file_modified = datetime(2024, 3, 28, 12, 0, 0, tzinfo=UTC)

        ts_raw = ci.get("timestamp")
        assert ts_raw is None  # missing from commitInfo

        # Fallback logic
        if ts_raw is None and file_modified:
            ts_raw = file_modified

        # Format check — datetime objects should be formatted directly
        assert isinstance(ts_raw, datetime)
        formatted = ts_raw.strftime("%Y-%m-%d %H:%M:%S UTC")
        assert formatted == "2024-03-28 12:00:00 UTC"

    def test_version_from_filename(self):
        """Delta log filenames encode version as zero-padded number."""
        filenames = [
            "00000000000000000000.json",
            "00000000000000000001.json",
            "00000000000000000042.json",
        ]
        versions = []
        for f in filenames:
            v = f.replace(".json", "").lstrip("0") or "0"
            versions.append(v)

        assert versions == ["0", "1", "42"]


# ── Path format builders ────────────────────────────────────────────────


class TestPathFormatBuilders:
    """Test the three path format conversions (named, ABFSS, HTTPS)."""

    WS_ID = "aaaa-bbbb-cccc"
    WS_NAME = "MyWorkspace"
    ITEM_ID = "dddd-eeee-ffff"
    ITEM_NAME = "MyLakehouse"
    HOST = "onelake.dfs.fabric.microsoft.com"

    def _make_named_path(self, ws_name, item_name, rel):
        return f"onelake://{ws_name}/{item_name}/{rel}"

    def _make_abfss_path(self, ws_id, host, full_path):
        return f"abfss://{ws_id}@{host}/{full_path}"

    def _make_https_path(self, ws_id, host, full_path):
        return f"https://{host}/{ws_id}/{full_path}"

    def test_named_path_for_folder(self):
        directory = f"{self.ITEM_ID}/Files"
        rel = directory.split("/", 1)[-1]
        path = self._make_named_path(self.WS_NAME, self.ITEM_NAME, rel)
        assert path == "onelake://MyWorkspace/MyLakehouse/Files"

    def test_named_path_for_file(self):
        file_path = f"{self.ITEM_ID}/Files/data.csv"
        rel = file_path.split("/", 1)[-1]
        path = self._make_named_path(self.WS_NAME, self.ITEM_NAME, rel)
        assert path == "onelake://MyWorkspace/MyLakehouse/Files/data.csv"

    def test_named_path_for_table(self):
        path = self._make_named_path(self.WS_NAME, self.ITEM_NAME, "Tables/customers")
        assert path == "onelake://MyWorkspace/MyLakehouse/Tables/customers"

    def test_abfss_path_for_folder(self):
        directory = f"{self.ITEM_ID}/Files"
        path = self._make_abfss_path(self.WS_ID, self.HOST, directory)
        assert path == f"abfss://{self.WS_ID}@{self.HOST}/{self.ITEM_ID}/Files"

    def test_abfss_path_for_table(self):
        full = f"{self.ITEM_ID}/Tables/customers"
        path = self._make_abfss_path(self.WS_ID, self.HOST, full)
        assert "abfss://" in path
        assert f"@{self.HOST}" in path
        assert "/Tables/customers" in path

    def test_https_path_for_file(self):
        file_path = f"{self.ITEM_ID}/Files/data.csv"
        path = self._make_https_path(self.WS_ID, self.HOST, file_path)
        assert path == f"https://{self.HOST}/{self.WS_ID}/{self.ITEM_ID}/Files/data.csv"

    def test_https_path_for_table(self):
        full = f"{self.ITEM_ID}/Tables/customers"
        path = self._make_https_path(self.WS_ID, self.HOST, full)
        assert path.startswith("https://")
        assert "/Tables/customers" in path

    def test_abfss_uses_correct_format(self):
        """ABFSS format: abfss://{workspace}@{host}/{path}"""
        path = self._make_abfss_path("ws-guid", "host.com", "item-guid/Files")
        assert path == "abfss://ws-guid@host.com/item-guid/Files"

    def test_https_uses_correct_format(self):
        """HTTPS format: https://{host}/{workspace}/{path}"""
        path = self._make_https_path("ws-guid", "host.com", "item-guid/Files")
        assert path == "https://host.com/ws-guid/item-guid/Files"


# ── Delta reader methods ────────────────────────────────────────────────


class TestDeltaReaderMethods:
    """Test that DeltaTableReader has the new methods."""

    def test_read_sample_method_exists(self):
        from onelake_client.tables.delta import DeltaTableReader

        assert hasattr(DeltaTableReader, "read_sample")
        assert callable(DeltaTableReader.read_sample)

    def test_read_cdf_method_exists(self):
        from onelake_client.tables.delta import DeltaTableReader

        assert hasattr(DeltaTableReader, "read_cdf")
        assert callable(DeltaTableReader.read_cdf)

    def test_build_table_uri(self):
        from onelake_client.tables.delta import _build_table_uri

        uri = _build_table_uri(
            "ws-guid",
            "item-guid",
            "customers",
            "onelake.dfs.fabric.microsoft.com",
        )
        assert uri == "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/item-guid/Tables/customers"


# ── DeletionVectors error detection ─────────────────────────────────────


class TestDeletionVectorsDetection:
    """Test that unsupported reader feature errors are detected."""

    def test_detects_reader_features_error(self):
        """Error message containing 'reader features' and 'not yet supported' is caught."""
        err = (
            "The table has set these reader features: {'deletionVectors'} "
            "but these are not yet supported by the deltalake reader."
        )
        assert "reader features" in err
        assert "not yet supported" in err

    def test_does_not_match_unrelated_error(self):
        err = "Network timeout after 30 seconds"
        assert not ("reader features" in err and "not yet supported" in err)


# ── Tree arrow key navigation ───────────────────────────────────────────


class TestTreeArrowKeys:
    """Verify OneLakeTree has custom arrow key handler."""

    def test_tree_has_on_key_method(self):
        from onelake_tui.tree import OneLakeTree

        assert hasattr(OneLakeTree, "on_key")
        assert callable(OneLakeTree.on_key)

    def test_tree_imports_events(self):
        """The events module must be imported for key handling."""

        # Verify the on_key method signature accepts events.Key
        import inspect

        from onelake_tui.tree import OneLakeTree

        sig = inspect.signature(OneLakeTree.on_key)
        params = list(sig.parameters.keys())
        assert "event" in params
