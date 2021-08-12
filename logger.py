import time

def log(*args):
    print(time.strftime("%Y-%m-%d %H:%M "), *args, flush=True)
