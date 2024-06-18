from pyiceberg.catalog import load_catalog
import pyarrow as pa


# set export PYICEBERG_CATALOG__DEMO__URI=http://localhost:3000
rest = load_catalog("demo")
print(rest.list_namespaces())

rest.create_namespace("westeros")
print(rest.list_namespaces())

# pyarrow dataframe
schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("what_they_know", pa.float64()),
    ]
)

data = pa.table(
    {
        "id": [1],
        "name": ["jon_snow"],
        "what_they_know": [0.0],
    },
    schema=schema,
)


rest.create_table("westeros.my_table", schema=schema)
print(rest.list_tables("westeros"))

table = rest.load_table("westeros.my_table")
# haven't been able to test this part
table.append(data)
df = table.scan().to_pandas()
print(df)
