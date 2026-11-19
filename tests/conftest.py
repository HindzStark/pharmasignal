import os
import sys

import pytest

# Spark workers must use the same interpreter as the test process (matters on Windows / venvs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]").appName("pharmasignal-tests")
               .config("spark.sql.ansi.enabled", "true")          # Glue 6.0 / Spark 4 default
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
