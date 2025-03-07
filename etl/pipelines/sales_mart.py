from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
import os
from typing import Dict, List, Optional
from delta.tables import *  # noqa: F403
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from utils.metadata import log_metadata
from utils.generate_mock_data import generate_bronze_data
import great_expectations as gx


@dataclass
class DeltaDataset:
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
        self.STORAGE_PATH = storage_path or "s3a://ecommerce/delta"
        self.DATABASE = database or "ecommerce"
        self.DEFAULT_PARTITION = partition or datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    def run_data_validations(self, input_datasets: Dict[str, DeltaDataset]):
        context = gx.get_context(
            context_root_dir=os.path.join(os.getcwd(), "etl", "gx"),
        )
        results = []

        for input_dataset in input_datasets.values():
            definition_name = f"{input_dataset.name}_validation"
            batch_parameters = {"dataframe": input_dataset.curr_data}

            batch_definition = (
                context.data_sources.get("spark_datasource")
                .get_asset(input_dataset.name)
                .get_batch_definition(f"{input_dataset.name}_batch")
            )

            expectation_suite = context.suites.get(name=f"{input_dataset.name}_suite")

            validation_definition = gx.ValidationDefinition(
                data=batch_definition,
                suite=expectation_suite,
                name=definition_name,
            )

            # Check if the validation definition exists
            try:
                context.validation_definitions.get(definition_name)
            except gx.exceptions.DataContextError:
                context.validation_definitions.add(validation_definition)

            checkpoint_name = f"{input_dataset.name}_checkpoint"

            # Check if checkpoint exists before adding
            try:
                checkpoint = context.checkpoints.get(checkpoint_name)
            except gx.exceptions.DataContextError:
                checkpoint = gx.Checkpoint(
                    name=checkpoint_name,
                    validation_definitions=[validation_definition],
                    result_format="COMPLETE",
                )
                context.checkpoints.add(checkpoint)

            # Run the checkpoint
            checkpoint_result = checkpoint.run(batch_parameters=batch_parameters)

            results.append(checkpoint_result)
        return results

    @log_metadata
    def validate_data(self, input_datasets: Dict[str, DeltaDataset], **kwargs) -> bool:
        validation_results = self.run_data_validations(input_datasets)
        results = {}
        for validation in validation_results:
            suite_name = next(iter(validation.run_results.values()), {}).get("suite_name")
            results[suite_name] = validation.success
        for k, v in results.items():
            if not v:
                raise InValidDataException(
                    f"The {k} dataset did not pass validation, please check the metadata db for more information"
                )

        return True

    def check_required_inputs(self, input_datasets: Dict[str, DeltaDataset], required_datasets: List[str]) -> None:
        """
        Validate that all required datasets are present.

        This method verifies that every dataset name specified in 'required_datasets' exists as a key
        in the 'input_datasets' dictionary. It uses set subtraction for efficiency. If any required
        dataset is missing, a ValueError is raised with details about the missing dataset(s).

        Parameters:
            input_datasets (Dict[str, DeltaDataset]):
                A dictionary mapping dataset names to DeltaDataset objects.
            required_datasets (List[str]):
                A list of dataset names that must be present in input_datasets.

        Raises:
            ValueError:
                If one or more required datasets are not found in input_datasets.

        Example:
            input_datasets = {
                "customer": DeltaDataset(...),
                "orders": DeltaDataset(...)
            }
            required_datasets = ["customer", "orders", "transactions"]

            self.check_required_inputs(input_datasets, required_datasets)
            # Raises ValueError: "Missing required datasets: transactions"
        """
        # Find missing datasets using set difference for efficiency
        missing_datasets = set(required_datasets) - set(input_datasets.keys())

        if missing_datasets:
            raise ValueError(f"Missing required datasets: {', '.join(missing_datasets)}")

    def construct_join_string(self, keys: List[str]) -> str:
        """
        Construct a SQL join condition string from a list of join keys.

        This method creates a join condition by generating a comparison expression for each key.
        Each expression is of the form "target.<key> = source.<key>" and the expressions are
        concatenated with " AND " to form a complete SQL join condition suitable for merge operations.

        Parameters:
            keys (List[str]): A list of column names used as join keys.

        Returns:
            str: A SQL join condition string.

        Example:
            For keys = ["id", "date"], the result will be:
                "target.id = source.id AND target.date = source.date"
        """
        return " AND ".join(f"target.{key} = source.{key}" for key in keys)

    def write_delta_data(
        self,
        input_datasets: Dict[str, DeltaDataset],
        spark: SparkSession,
        **kwargs,
    ) -> None:
        """
        Write DeltaDataset data to storage with partitioning and upsert (merge) functionality.

        For each dataset in the input_datasets dictionary, this method performs the following:

        1. Data Preparation:
           - Enhances the dataset's DataFrame (dataset.curr_data) by adding:
             - An "etl_inserted" column with the current timestamp.
             - A "partition" column with the dataset's partition value.
           - This metadata aids in auditing and partition management.

        2. Writing Strategy:
           - If the dataset's 'replace_partition' flag is True:
             - The DataFrame is written using the "overwrite" mode in Delta format.
             - The 'replaceWhere' option restricts the overwrite to rows where the partition matches.
             - The data is saved to the specified storage path.
           - If 'replace_partition' is False:
             - The target Delta table is loaded from the dataset's storage path.
             - A merge (upsert) operation is executed:
               - The target is aliased as "target" and the prepared DataFrame as "source".
               - The merge condition is built using the dataset's primary keys via construct_join_string().
               - Matching rows are updated and non-matching rows are inserted.

        3. Skipping Datasets:
           - Datasets with the 'skip_write' flag set to True are skipped.

        Parameters:
            input_datasets (Dict[str, DeltaDataset]):
                A dictionary mapping dataset names to DeltaDataset objects.
            spark (SparkSession):
                The active Spark session used for Delta Lake operations.
            **kwargs:
                Additional keyword arguments, if any.

        Returns:
            None

        Example:
            Suppose we have two datasets:
              - "customer": replace_partition=True, storage_path="s3a://path/to/customer",
                partition="2025-02-22"
              - "orders": replace_partition=False, storage_path="s3a://path/to/orders",
                partition="2025-02-22"

            When write_delta_data is invoked, the "customer" dataset will be fully overwritten
            for its partition, while the "orders" dataset will be merged (upserted) with the existing data.
        """
        # Example: Iterating over a dictionary of key-value pairs
        # fruits = {'apple': 3, 'banana': 5, 'cherry': 2}
        # for fruit, count in fruits.items():
        #     print(f"{fruit}: {count}")
        # Output:
        # apple: 3
        # banana: 5
        # cherry: 2

        for dataset_name, dataset in input_datasets.items():
            if dataset.skip_write:
                continue  # Skip datasets that are not meant to be published

            # Prepare the DataFrame by adding metadata columns.
            data_to_write = dataset.curr_data.withColumn("etl_inserted", F.current_timestamp()).withColumn(
                "partition", F.lit(dataset.partition)
            )

            # Case 1: Replace Partition
            if dataset.replace_partition:
                data_to_write.write.format("delta").mode("overwrite").option(
                    "replaceWhere", f"partition = '{dataset.partition}'"
                ).save(dataset.storage_path)
            # Case 2: Upsert (Merge)
            else:
                target_table = DeltaTable.forPath(spark, dataset.storage_path)  # noqa: F405
                (
                    target_table.alias("target")
                    .merge(
                        source=data_to_write.alias("source"),
                        condition=self.construct_join_string(dataset.primary_keys),
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )

    @abstractmethod
    def make_bronze_datasets(
        self,
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataset]:
        pass

    @abstractmethod
    def make_silver_datasets(
        self,
        input_datasets: Dict[str, DeltaDataset],
        spark: SparkSession,
        **kwargs,
    ):
        pass

    @abstractmethod
    def make_gold_datasets(
        self,
        input_datasets: Dict[str, DeltaDataset],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataset]:
        pass

    @log_metadata
    def run(self, spark: SparkSession, **kwargs):
        # Extract common parameters from kwargs
        partition = kwargs.get("partition")
        pipeline_id = kwargs.get("pipeline_id")
        run_id = kwargs.get("run_id")

        # Create a log prefix to simplify log messages
        log_prefix = f"pipeline_id: {pipeline_id}, run_id: {run_id}, partition: {partition}"

        logger.info(f"Starting run process for {log_prefix}")

        # Process Bronze Datasets
        logger.info(f"Starting get_bronze_datasets for {log_prefix}")
        bronze_datasets = self.make_bronze_datasets(
            spark,
            partition=partition,
            run_id=run_id,
            pipeline_id=pipeline_id,
        )
        self.validate_data(bronze_datasets)
        self.write_delta_data(bronze_datasets, spark)
        logger.info(f"Created, validated & published bronze datasets: {list(bronze_datasets.keys())}")

        # Process Silver Datasets
        logger.info(f"Starting get_silver_datasets for {log_prefix}")
        silver_datasets = self.make_silver_datasets(
            bronze_datasets,
            spark,
            partition=partition,
            run_id=run_id,
            pipeline_id=pipeline_id,
        )
        self.validate_data(silver_datasets)
        self.write_delta_data(silver_datasets, spark)
        logger.info(f"Created, validated & published silver datasets: {list(silver_datasets.keys())}")

        # Process Gold Datasets
        logger.info(f"Starting get_gold_datasets for {log_prefix}")
        gold_datasets = self.make_gold_datasets(
            silver_datasets,
            spark,
            partition=partition,
            run_id=run_id,
            pipeline_id=pipeline_id,
        )
        self.validate_data(gold_datasets)
        self.write_delta_data(gold_datasets, spark)
        logger.info(f"Created, validated & published gold datasets: {list(gold_datasets.keys())}")


class SalesMartETL(StandardETL):
    @log_metadata
    def make_bronze_datasets(
        self,
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataset]:
        customer_df, orders_df = generate_bronze_data(spark)
        return {
            "customer": DeltaDataset(
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
            "orders": DeltaDataset(
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

    @log_metadata
    def make_dim_customer(
        self,
        customer: DeltaDataset,
        spark: SparkSession,
        **kwargs,
    ) -> DataFrame:
        # Get the current customer source and the dimension table
        customer_df = customer.curr_data
        dim_customer: DataFrame = kwargs["dim_customer"]

        # Generate surrogate key using MD5 hash
        customer_df = customer_df.withColumn("customer_sur_id", F.md5(F.concat("id", "datetime_updated")))

        # Filter the dimension table to current (active) records
        current_dim = dim_customer.filter("current = true")

        # Define the common columns for inserts/updates
        insert_columns = [
            customer_df.id,
            customer_df.customer_sur_id,
            customer_df.first_name,
            customer_df.last_name,
            customer_df.state_id,
            customer_df.datetime_created,
            customer_df.datetime_updated,
        ]

        # --- Identify new customer rows ---
        # These are rows in customer_df with IDs that do NOT exist in current_dim
        new_customer_inserts = customer_df.join(current_dim, on="id", how="leftanti").select(*insert_columns)

        # --- Identify updated customer rows ---
        # These are rows in customer_df that already exist in current_dim and have a more recent update timestamp
        updated_customer_inserts = (
            customer_df.join(current_dim, on="id", how="inner")
            .filter(customer_df.datetime_updated > current_dim.datetime_updated)
            .select(*insert_columns)
        )

        # --- Identify the current dimension records to be closed ---
        # For every updated customer, update the current dimension record with a new valid_to and set current = false.
        customers_to_update = (
            current_dim.join(customer_df, on="id", how="inner")
            .filter(customer_df.datetime_updated > current_dim.datetime_updated)
            .select(
                current_dim.id,
                current_dim.customer_sur_id,
                current_dim.first_name,
                current_dim.last_name,
                current_dim.state_id,
                current_dim.datetime_created,
                customer_df.datetime_updated,  # new update timestamp
                current_dim.valid_from,
            )
            .withColumn("current", F.lit(False))
            .withColumn("valid_to", customer_df.datetime_updated)
        )

        # Helper function to add SCD2 insert metadata columns
        def add_insert_columns(df: DataFrame) -> DataFrame:
            return (
                df.withColumn("current", F.lit(True))
                .withColumn("valid_from", customer_df.datetime_updated)
                .withColumn("valid_to", F.lit("2099-01-01 12:00:00.0000"))
            )

        # Apply the helper function to new and updated inserts
        new_inserts = add_insert_columns(new_customer_inserts)
        updated_inserts = add_insert_columns(updated_customer_inserts)

        # Combine new inserts, new versions for updates, and update the existing records
        dim_customer = new_inserts.unionByName(updated_inserts).unionByName(customers_to_update)

        return dim_customer

    @log_metadata
    def make_fct_orders(
        self,
        input_datasets: Dict[str, DeltaDataset],
        spark: SparkSession,
        **kwargs,
    ) -> DataFrame:
        # Extract current data for dimension customers and orders
        dim_customers_df = input_datasets["dim_customer"].curr_data
        orders_df = input_datasets["orders"].curr_data

        # Filter to current (active) customers only
        current_customers_df = dim_customers_df.filter("current = true")

        # Define join condition on customer_id
        join_condition = orders_df.customer_id == current_customers_df.id

        # Join orders with the current customer dimension and select required columns
        fct_orders = orders_df.join(
            current_customers_df,
            join_condition,
            "left",
        ).select(
            orders_df.order_id,
            orders_df.customer_id,
            orders_df.item_id,
            orders_df.item_name,
            orders_df.delivered_on,
            orders_df.datetime_order_placed,
            current_customers_df.customer_sur_id,
        )

        return fct_orders

    @log_metadata
    def make_silver_datasets(
        self,
        input_datasets: Dict[str, DeltaDataset],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataset]:
        self.check_required_inputs(input_datasets, ["customer", "orders"])
        dim_customer_df = self.make_dim_customer(
            input_datasets["customer"],
            spark,
            dim_customer=spark.read.table(f"{self.DATABASE}.dim_customer"),
        )
        silver_datasets = {}
        silver_datasets["dim_customer"] = DeltaDataset(
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
        silver_datasets["dim_customer"].curr_data = spark.read.table(f"{self.DATABASE}.dim_customer")
        silver_datasets["dim_customer"].skip_write = True
        input_datasets["dim_customer"] = silver_datasets["dim_customer"]

        silver_datasets["fct_orders"] = DeltaDataset(
            name="fct_orders",
            curr_data=self.make_fct_orders(input_datasets, spark),
            primary_keys=["order_id"],
            storage_path=f"{self.STORAGE_PATH}/fct_orders",
            table_name="fct_orders",
            data_type="delta",
            database=f"{self.DATABASE}",
            partition=kwargs.get("partition", self.DEFAULT_PARTITION),
            replace_partition=True,
        )
        return silver_datasets

    @log_metadata
    def make_sales_mart(
        self,
        input_datasets: Dict[str, DeltaDataset],
        **kwargs,
    ):
        dim_customer = (
            input_datasets["dim_customer"].curr_data.where("current = true").select("customer_sur_id", "state_id")
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

    @log_metadata
    def make_gold_datasets(
        self,
        input_datasets: Dict[str, DeltaDataset],
        spark: SparkSession,
        **kwargs,
    ) -> Dict[str, DeltaDataset]:
        self.check_required_inputs(input_datasets, ["dim_customer", "fct_orders"])
        sales_mart_df = self.make_sales_mart(input_datasets)
        return {
            "sales_mart": DeltaDataset(
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
        SparkSession.builder.appName("ecommerce")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .getOrCreate()
    )

    # spark.sparkContext.setLogLevel("ERROR")
    log4j_logger = spark._jvm.org.apache.log4j  # type: ignore
    logger = log4j_logger.LogManager.getLogger("ecommerce_logger")
    logger.info("Starting Sales Mart ETL")

    sm = SalesMartETL()

    # Partition as input, usually from orchestrator
    partition = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    pipeline_id = "sales_mart"
    run_id = f"{pipeline_id}_{partition}_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

    sm.run(spark, partition=partition, run_id=run_id, pipeline_id=pipeline_id)

    spark.stop()
