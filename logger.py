import time

def log(string, *args):
    print(time.strftime("%Y-%m-%d %H:%M  ")+string, *args, flush=True)
