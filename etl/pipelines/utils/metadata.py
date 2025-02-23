import inspect
from utils.db_connection import MetadataConnection


def log_metadata(func):
    def log_wrapper(*args, **kwargs):
        input_params_from_func = dict(zip(list(locals().keys())[:-1], list(locals().values())[:-1]))
        param_names_from_func = list(inspect.signature(func).parameters.keys())
        values_from_arg = input_params_from_func.get("args")

        input_dict = {}
        for v in values_from_arg:
            input_dict[param_names_from_func.pop(0)] = v

        run_id = input_params_from_func.get("kwargs").get("run_id")
        pipeline_id = input_params_from_func.get("kwargs").get("pipeline_id")
        run_params = str(input_dict | input_params_from_func.get("kwargs") | {"function": func.__name__})

        db_conn = MetadataConnection()

        with db_conn.managed_cursor() as cur:
            insert_query = """
            INSERT INTO run_metadata (run_id, pipeline_id, run_params)
            VALUES (%s, %s, %s);
            """
            cur.execute(insert_query, (run_id, pipeline_id, run_params))

        return func(*args, **kwargs)

    return log_wrapper
