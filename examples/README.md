# Examples 

This directory contains examples of how to use the Iceberg REST catalog. 


## PySpark Example

The PySpark example demonstrates how to use the Iceberg REST catalog with PySpark. 

The example creates a namespace `westeros` and a table `my_table` in the namespace.
It creates a Spark DataFrame and writes it to the table. It then reads the table into a pandas dataframe.



## PyIceberg Example

The PyIceberg example demonstrates how to use the Iceberg REST catalog with PyIceberg. To run the example, 
you need to set the environment variable  `PYICEBERG_CATALOG__DEMO__URI` to `http://localhost:3000`:
```bash
export PYICEBERG_CATALOG__DEMO__URI=http://localhost:3000
```

The example creates a namespace `westeros` and a table `my_table` in the namespace. 
It then appends a row to the table and scans the table into a pandas dataframe.
