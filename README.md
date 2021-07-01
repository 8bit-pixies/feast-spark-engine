# feast-spark-engine

An attempt at scaling out Feast with Spark without a Spark Serving service

The approach is to lift the relevant parts from `feast-spark` and extend `feast` using custom offline store.

```
pip install koalas pyspark feast
```

The default version is a filebased approach. 

```
python example.py
```

The Spark version which is a copy+paste code version of Spark is:

```
python example_spark.py
```