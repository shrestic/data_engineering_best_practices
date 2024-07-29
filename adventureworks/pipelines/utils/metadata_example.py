import inspect

def log_metadata_example(func):
    """
    A decorator that logs the input parameters and return value of a function.

    This decorator captures both positional and keyword arguments passed to the
    function, as well as the function's return value. It prints this information
    to the console for debugging or monitoring purposes.

    Args:
        func: The function to be decorated.

    Returns:
        The decorated function (log_wrapper).
    """

    def log_wrapper(*args, **kwargs):
        """
        The inner wrapper function that performs the logging.
        """
        # Capture all local variables (arguments and their values) 
        # except for the last one, which is this function itself
        input_params = dict(
            zip(list(locals().keys())[:-1], list(locals().values())[:-1])
        )

        # Print the keys (names of the local variables) for debugging
        print(list(locals().keys())[:-1])  
        
        # Print the values (data stored in the local variables) for debugging
        print(list(locals().values())[:-1]) 

        # Get the names of the function's parameters, maintaining their order
        param_names = list(inspect.signature(func).parameters.keys())

        # Create a dictionary to store input parameter names and values
        input_dict = {}
        for v in input_params.get("args"):
            # Associate positional argument values with their names
            input_dict[param_names.pop(0)] = v
        
        # Add keyword arguments to input_dict
        input_dict.update(kwargs)

        # Print the function name and input parameters
        print(f"Function {func.__name__} called with inputs: {input_dict}")

        # Call the original function and capture its result
        result = func(*args, **kwargs)

        # Print the function name and return value
        print(f"Function {func.__name__} returned: {result}")

        # Return the result of the original function
        return result

    return log_wrapper

# Example function to be decorated
@log_metadata_example
def example_function(a, b, c=1):
    return a + b + c

# Example usage
example_function(2, 3, c=4)
