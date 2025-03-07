import great_expectations as gx
from typing import List, Tuple, Type


def create_data_context(project_root: str = "./etl"):
    """Initialize and return a File Data Context for the ETL project."""
    return gx.get_context(mode="file", project_root_dir=project_root)


def setup_spark_datasource(context, datasource_name: str, asset_names: List[str]):
    """Set up Spark Data Source, Data Assets, Batch Definitions, and create empty Expectation Suites."""
    datasource = context.data_sources.add_spark(name=datasource_name)
    for name in asset_names:
        data_asset = datasource.add_dataframe_asset(name=name)
        # Add Batch Definition for each Data Asset
        batch_definition_name = f"{name}_batch"  # Dynamic batch name based on asset name
        suite_name = f"{name}_suite"  # Dynamic suite name based on asset name
        data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
        context.suites.add(gx.ExpectationSuite(name=suite_name))
    return datasource


def add_expectations_to_suites(
    context, expectations_config: dict[str, List[Tuple[str, Type[gx.expectations.Expectation]]]]
) -> None:
    """Add predefined Expectations to their corresponding Expectation Suites."""
    for suite_name, suite_expectations in expectations_config.items():
        suite = context.suites.get(f"{suite_name}_suite")
        for column, expectation_class in suite_expectations:
            expectation = expectation_class(
                column=column,
                meta={
                    "notes": {
                        "content": f"{column} column is expected to be {expectation_class.__name__.replace('ExpectColumnValuesTo', '').lower()}",
                        "format": "markdown",
                    }
                },
            )
            suite.add_expectation(expectation)


def verify_suites(context, suite_names: List[str]):
    """Verify and print the Expectations in each Suite (optional)."""
    for name in suite_names:
        suite = context.suites.get(f"{name}_suite")
        print(f"Suite '{name}_suite' expectations: {suite.expectations}")


def main():
    """Main function to configure GX project with Spark Data Source, Batch Definitions, and Expectations."""
    # Configuration
    project_root = "./etl"
    datasource_name = "spark_datasource"
    asset_names = ["customer", "orders", "sales_mart", "dim_customer", "fct_orders"]

    # Define Expectations for each Suite
    expectations_config = {
        "customer": [("id", gx.expectations.ExpectColumnValuesToNotBeNull)],
        "dim_customer": [("id", gx.expectations.ExpectColumnValuesToNotBeNull)],
        "fct_orders": [("order_id", gx.expectations.ExpectColumnValuesToBeUnique)],
        "orders": [("order_id", gx.expectations.ExpectColumnValuesToBeUnique)],
        "sales_mart": [("state_id", gx.expectations.ExpectColumnValuesToNotBeNull)],
    }

    # Execute
    context = create_data_context(project_root)
    setup_spark_datasource(context, datasource_name, asset_names)
    add_expectations_to_suites(context, expectations_config)
    verify_suites(context, asset_names)


if __name__ == "__main__":
    main()
