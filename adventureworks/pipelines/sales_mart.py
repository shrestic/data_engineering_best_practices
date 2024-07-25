import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
try:
    from adventureworks.pipelines.utils.create_fake_data import generate_bronze_data
except ModuleNotFoundError:
    from utils.create_fake_data import generate_bronze_data  # type: ignore

logging.basicConfig(level = logging.INFO)

@dataclass
class DeltaDataSet:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    table_name: str
    data_type: str
    database: str
    partition: str
    skip_write: bool = False
    replace_partition: bool = False


class InValidDataException(Exception):
    pass


class StandardETL(ABC):
    def __init__(
        self,
        storage_path: Optional[str] = None,
        database: Optional[str] = None,
        partition: Optional[str] = None,
    ):
        self.STORAGE_PATH = storage_path or "s3a://adventureworks/delta"
        self.DATABASE = database or "adventureworks"
        self.DEFAULT_PARTITION = partition or datetime.now().strftime(
            "%Y-%m-%d-%H-%M-%S"
        )

    def check_required_inputs(
        self, input_datasets: Dict[str, DeltaDataSet], required_datasets: List[str]
    ) -> None:
        # Find missing datasets using sets (more efficient for large lists)
        missing_datasets = set(required_datasets) - set(input_datasets.keys())

        if missing_datasets:
            raise ValueError(
                f"Missing required datasets: {', '.join(missing_datasets)}"
            )

    def construct_join_string(self, keys: List[str]) -> str:
        return " AND ".join([f"target.{key} = source.{key}" for key in keys])

    def write_delta_data(
        self, input_datasets: Dict[str, DeltaDataSet], spark: SparkSession, **kwargs
    ) -> None:
        """Writes data from DeltaDataSets to their respective storage paths, handling partitioning and upserts."""

        # Use the dictionary keys for potential logging or debugging
        for dataset_name, dataset in input_datasets.items():
            if dataset.skip_write:
                continue  # Skip datasets that are not meant to be published

            data_to_write = dataset.curr_data.withColumn(
                "etl_inserted", F.current_timestamp()
            ).withColumn(
                "partition", F.lit(dataset.partition)
            )  # Use F prefix for functions

            if dataset.replace_partition:
                data_to_write.write.format("delta").mode("overwrite").option(
                    "replaceWhere", f"partition = '{dataset.partition}'"
                ).save(dataset.storage_path)
            else:
                target_table = DeltaTable.forPath(spark, dataset.storage_path)
                (
                    target_table.alias("target")
                    .merge(
                        data_to_write.alias("source"),
                        self.construct_join_string(dataset.primary_keys),
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )

    @abstractmethod
    def get_bronze_datasets(
        self, spark: SparkSession, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_silver_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_gold_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        pass

    def run(self, spark: SparkSession, **kwargs):
        partition = kwargs.get("partition")
        bronze_data_sets = self.get_bronze_datasets(spark, partition=partition)
        self.write_delta_data(bronze_data_sets, spark)
        logging.info(
            "Created, validated & published bronze datasets:"
            f" {[ds for ds in bronze_data_sets.keys()]}"
        )

        silver_data_sets = self.get_silver_datasets(
            bronze_data_sets, spark, partition=partition
        )
        self.write_delta_data(silver_data_sets, spark)
        logging.info(
            "Created, validated & published silver datasets:"
            f" {[ds for ds in silver_data_sets.keys()]}"
        )

        gold_data_sets = self.get_gold_datasets(
            silver_data_sets, spark, partition=partition
        )
        self.write_delta_data(gold_data_sets, spark)
        logging.info(
            "Created, validated & published gold datasets:"
            f" {[ds for ds in gold_data_sets.keys()]}"
        )


class SalesMartETL(StandardETL):
    def get_bronze_datasets(
        self, spark: SparkSession, **kwargs
    ) -> Dict[str, DeltaDataSet]:
        customer_df, orders_df = generate_bronze_data(spark)
        return {
            "customer": DeltaDataSet(
                name="customer",
                curr_data=customer_df,
                primary_keys=["id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/customer",
                table_name="customer",
                data_type="delta",
                database=f"{self.DATABASE}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            ),
            "orders": DeltaDataSet(
                name="orders",
                curr_data=orders_df,
                primary_keys=["order_id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/orders",
                table_name="orders",
                data_type="delta",
                database=f"{self.DATABASE}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            ),
        }

    def get_dim_customer(
        self, customer: DeltaDataSet, spark: SparkSession, **kwargs
    ) -> DataFrame:
        customer_df = customer.curr_data
        dim_customer: DataFrame = kwargs["dim_customer"]

        # Generate PK using a simple hashing function (e.g., MD5)
        customer_df = customer_df.withColumn(
            "customer_sur_id", F.md5(F.concat("id", "datetime_updated"))
        )

        # Filter for latest customer rows in dim_customer using the current flag (SCD2 type 2)
        latest_dim_customer_records = dim_customer.where("current = true")

        # Common join condition to reuse
        join_condition = (customer_df.id == latest_dim_customer_records.id) & (
            latest_dim_customer_records.datetime_updated < customer_df.datetime_updated
        )

        def add_insert_columns(df: DataFrame):
            return (
                df.withColumn("current", F.lit(True))
                .withColumn("valid_from", customer_df.datetime_updated)
                .withColumn("valid_to", F.lit("2099-01-01 12:00:00.0000"))
            )

        # Get net new rows to insert (rows NOT in dim_customer)
        new_customer_inserts = customer_df.join(
            latest_dim_customer_records, join_condition, "leftanti"
        ).select(
            customer_df.id,
            customer_df.customer_sur_id,
            customer_df.first_name,
            customer_df.last_name,
            customer_df.state_id,
            customer_df.datetime_created,
            customer_df.datetime_updated,
        )

        # Get rows to insert for existing IDs (rows in dim_customer but outdated)
        # No need to join twice, we can reuse the filtered 'customer_df_insert_net_new'
        existing_customers_to_insert = new_customer_inserts.join(
            latest_dim_customer_records, join_condition  # Use same join_condition
        ).select(
            customer_df.id,
            customer_df.customer_sur_id,
            customer_df.first_name,
            customer_df.last_name,
            customer_df.state_id,
            customer_df.datetime_created,
            customer_df.datetime_updated,
        )

        # Get rows to be updated (rows in dim_customer that need valid_to updated)
        customers_to_update = (
            latest_dim_customer_records.join(customer_df, join_condition)
            .select(
                latest_dim_customer_records.id,
                latest_dim_customer_records.customer_sur_id,
                latest_dim_customer_records.first_name,
                latest_dim_customer_records.last_name,
                latest_dim_customer_records.state_id,
                latest_dim_customer_records.datetime_created,
                customer_df.datetime_updated,
                latest_dim_customer_records.valid_from,
            )
            .withColumn("current", F.lit(False))
            .withColumn("valid_to", customer_df.datetime_updated)
        )

        return (
            add_insert_columns(new_customer_inserts)
            .unionByName(add_insert_columns(existing_customers_to_insert))
            .unionByName(customers_to_update)
        )

    def get_fct_orders(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> DataFrame:
        dim_customer = input_datasets["dim_customer"].curr_data
        orders_df = input_datasets["orders"].curr_data

        dim_customer_curr_df = dim_customer.where("current = true")
        return orders_df.join(
            dim_customer_curr_df,
            orders_df.customer_id == dim_customer_curr_df.id,
            "left",
        ).select(
            orders_df.order_id,
            orders_df.customer_id,
            orders_df.item_id,
            orders_df.item_name,
            orders_df.delivered_on,
            orders_df.datetime_order_placed,
            dim_customer_curr_df.customer_sur_id,
        )

    def get_silver_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        self.check_required_inputs(input_datasets, ["customer", "orders"])
        dim_customer_df = self.get_dim_customer(
            input_datasets["customer"],
            spark,
            dim_customer=spark.read.table(f"{self.DATABASE}.dim_customer"),
        )
        silver_datasets = {}
        silver_datasets["dim_customer"] = DeltaDataSet(
            name="dim_customer",
            curr_data=dim_customer_df,
            primary_keys=["customer_sur_id"],
            storage_path=f"{self.STORAGE_PATH}/dim_customer",
            table_name="dim_customer",
            data_type="delta",
            database=f"{self.DATABASE}",
            partition=kwargs.get("partition", self.DEFAULT_PARTITION),
        )
        self.write_delta_data(silver_datasets, spark)
        silver_datasets["dim_customer"].curr_data = spark.read.table(
            f"{self.DATABASE}.dim_customer"
        )
        silver_datasets["dim_customer"].skip_write = True
        input_datasets["dim_customer"] = silver_datasets["dim_customer"]

        silver_datasets["fct_orders"] = DeltaDataSet(
            name="fct_orders",
            curr_data=self.get_fct_orders(input_datasets, spark),
            primary_keys=["order_id"],
            storage_path=f"{self.STORAGE_PATH}/fct_orders",
            table_name="fct_orders",
            data_type="delta",
            database=f"{self.DATABASE}",
            partition=kwargs.get("partition", self.DEFAULT_PARTITION),
            replace_partition=True,
        )
        return silver_datasets

    def get_sales_mart(
        self, input_datasets: Dict[str, DeltaDataSet], **kwargs
    ) -> DataFrame:
        dim_customer = (
            input_datasets["dim_customer"]
            .curr_data.where("current = true")
            .select("customer_sur_id", "state_id")
        )
        fct_orders = input_datasets["fct_orders"].curr_data

        # Join and aggregate data
        aggregated_orders = (
            fct_orders.alias("fct_orders")
            .join(
                dim_customer.alias("dim_customer"),
                fct_orders.customer_sur_id == dim_customer.customer_sur_id,
                "left",
            )
            .select(
                F.to_date(fct_orders.delivered_on, "yyyy-dd-MM").alias("deliver_date"),
                F.col("dim_customer.state_id").alias("state_id"),
            )
            .groupBy("deliver_date", "state_id")
            .count()
            .withColumnRenamed("count", "num_orders")
        )

        return aggregated_orders

    def get_gold_datasets(
        self,
        input_datasets: Dict[str, DeltaDataSet],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataSet]:
        self.check_required_inputs(input_datasets, ["dim_customer", "fct_orders"])
        sales_mart_df = self.get_sales_mart(input_datasets)
        return {
            "sales_mart": DeltaDataSet(
                name="sales_mart",
                curr_data=sales_mart_df,
                primary_keys=["deliver_date", "state_id", "partition"],
                storage_path=f"{self.STORAGE_PATH}/sales_mart",
                table_name="sales_mart",
                data_type="delta",
                database=f"{self.DATABASE}",
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True,
            )
        }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("adventureworks").enableHiveSupport().getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sm = SalesMartETL()
    partition = datetime.now().strftime("%Y-%m-%d-%H-%M")
    # usually from orchestrator -%S
    sm.run(spark, partition=partition)
    spark.stop
