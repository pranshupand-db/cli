from pyspark import pipelines as dp


@dp.materialized_view
def order_totals():
    return spark.range(3).selectExpr("id AS order_id", "id * 10 AS amount")
