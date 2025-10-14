import time
import os,inspect
from functools import wraps

def timed_job(func):
    """Decorator to measure and print job duration with script name."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        frame = inspect.stack()[1]
        caller_file = os.path.basename(frame.filename)

    
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"\n{caller_file} job completed in {elapsed:.2f} seconds.")
        return result
    return wrapper
