from etl.ddl.create_silver_tables import create_tables as create_silver_tables
from etl.ddl.create_silver_tables import drop_tables as drop_silver_tables
from etl.pipelines.sales_mart import SalesMartETL


class TestSalesMartETL:

    def test_get_validate_publish_silver_datasets(self, spark) -> None:
        sm = SalesMartETL("/tmp/delta", database="ecommerce_test")
        bronze_datasets = sm.make_bronze_datasets(spark, partition="YYYY-MM-DD_HH")
        create_silver_tables(spark, path="/tmp/delta", database="ecommerce_test")
        silver_datasets = sm.make_silver_datasets(bronze_datasets, spark)
        drop_silver_tables(spark, database="ecommerce_test")
        assert sorted(silver_datasets.keys()) == ["dim_customer", "fct_orders"]

    def test_get_validate_publish_bronze_datasets(self, spark) -> None:
        pass

    def test_get_validate_publish_gold_datasets(self, spark) -> None:
        pass
