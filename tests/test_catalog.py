"""Tests for catalog.py."""

from __future__ import annotations

from patterns.catalog import CatalogConfig, CatalogType, IcebergCatalog, TableIdentifier


class TestTableIdentifier:
    def test_str(self):
        t = TableIdentifier(["db"], "orders")
        assert str(t) == "db.orders"

    def test_from_string(self):
        t = TableIdentifier.from_string("db.schema.table")
        assert t.name == "table"
        assert t.namespace == ["db", "schema"]

    def test_database(self):
        t = TableIdentifier(["mydb"], "t")
        assert t.database() == "mydb"

    def test_database_empty(self):
        t = TableIdentifier([], "t")
        assert t.database() == ""

    def test_no_namespace(self):
        t = TableIdentifier.from_string("mytable")
        assert t.name == "mytable"
        assert t.namespace == []


class TestCatalogConfig:
    def test_is_cloud_glue(self):
        cfg = CatalogConfig("c", CatalogType.GLUE)
        assert cfg.is_cloud() is True

    def test_is_cloud_rest_false(self):
        cfg = CatalogConfig("c", CatalogType.REST)
        assert cfg.is_cloud() is False

    def test_requires_uri_rest(self):
        cfg = CatalogConfig("c", CatalogType.REST, uri="http://...")
        assert cfg.requires_uri() is True

    def test_requires_uri_glue_false(self):
        cfg = CatalogConfig("c", CatalogType.GLUE)
        assert cfg.requires_uri() is False

    def test_to_pyiceberg_config(self):
        cfg = CatalogConfig("c", CatalogType.REST, uri="http://x", warehouse="s3://w")
        d = cfg.to_pyiceberg_config()
        assert d["type"] == "rest"
        assert d["uri"] == "http://x"

    def test_properties_included(self):
        cfg = CatalogConfig("c", properties={"token": "abc"})
        assert cfg.to_pyiceberg_config()["token"] == "abc"


class TestIcebergCatalog:
    def setup_method(self):
        self.cat = IcebergCatalog(CatalogConfig("test"))

    def test_create_namespace(self):
        self.cat.create_namespace("mydb")
        assert self.cat.namespace_exists("mydb") is True

    def test_namespace_not_exists(self):
        assert self.cat.namespace_exists("nope") is False

    def test_list_namespaces(self):
        self.cat.create_namespace("a")
        self.cat.create_namespace("b")
        assert "a" in self.cat.list_namespaces()

    def test_create_table(self):
        t = TableIdentifier(["db"], "orders")
        self.cat.create_table(t)
        assert self.cat.table_exists(t) is True

    def test_table_not_exists(self):
        t = TableIdentifier(["db"], "missing")
        assert self.cat.table_exists(t) is False

    def test_drop_table(self):
        t = TableIdentifier(["db"], "t")
        self.cat.create_table(t)
        assert self.cat.drop_table(t) is True
        assert self.cat.table_exists(t) is False

    def test_drop_missing_table(self):
        t = TableIdentifier(["db"], "nope")
        assert self.cat.drop_table(t) is False

    def test_list_tables(self):
        t = TableIdentifier(["db"], "t1")
        self.cat.create_table(t)
        assert "db.t1" in self.cat.list_tables()

    def test_list_tables_by_namespace(self):
        self.cat.create_table(TableIdentifier(["db"], "t1"))
        self.cat.create_table(TableIdentifier(["other"], "t2"))
        assert all("db." in x for x in self.cat.list_tables("db"))

    def test_rename_table(self):
        src = TableIdentifier(["db"], "old")
        dst = TableIdentifier(["db"], "new")
        self.cat.create_table(src)
        assert self.cat.rename_table(src, dst) is True
        assert self.cat.table_exists(dst) is True
        assert self.cat.table_exists(src) is False

    def test_rename_missing_table(self):
        src = TableIdentifier(["db"], "nope")
        dst = TableIdentifier(["db"], "new")
        assert self.cat.rename_table(src, dst) is False

    def test_table_properties(self):
        t = TableIdentifier(["db"], "t")
        self.cat.create_table(t, {"format-version": "2"})
        assert self.cat.table_properties(t)["format-version"] == "2"

    def test_auto_creates_namespace(self):
        t = TableIdentifier(["auto_ns"], "t")
        self.cat.create_table(t)
        assert self.cat.namespace_exists("auto_ns") is True
