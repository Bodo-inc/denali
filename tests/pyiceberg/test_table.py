import pyarrow as pa
from pyiceberg.types import IntegerType, StringType


def test_create_empty_table(catalog):
    in_schema = pa.schema([("id", pa.int32(), False), ("name", pa.string(), True)])

    created_table = catalog.create_table(
        "default.test_create_table",
        schema=in_schema,
        properties={"creator": "iceberg"}
    )

    table = catalog.load_table("default.test_create_table")
    assert created_table == table

    assert table.identifier == ("rest_catalog", "default", "test_create_table")
    schema = table.schema()
    assert schema.schema_id == 0

    id_col = schema.columns[0]
    assert id_col.name == "id"
    assert isinstance(id_col.field_type, IntegerType)
    assert id_col.required is True 
    
    name_col = schema.columns[1]
    assert name_col.name == "name"
    assert isinstance(name_col.field_type, StringType)
    assert name_col.required is False

    assert table.properties == {"creator": "iceberg"}

    catalog.drop_table("default.test_create_table")


def test_append_table(catalog):
    schema = pa.schema([("id", pa.int32()), ("name", pa.string())])

    table = catalog.create_table(
        "default.test_append_table",
        schema=schema,
    )

    df = pa.table([
        pa.array([1, 2, 3, 4]),
        pa.array(["Alice", "Bob", "Charlie", "David"]),
    ], schema=schema)
    table.append(df)

    read_df = table.scan().to_arrow()
    assert read_df.equals(df)

    catalog.drop_table("default.test_append_table")
