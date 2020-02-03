import importlib
import argparse
from ConfigTyped import ConfigTyped

class Launcher():
    def __init__(self, configFile, variableName, mode='normal'):
        config = ConfigTyped(configFile, mode)
        print(config.opts[variableName], end='')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Load data into database.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    ap.add_argument('-v', '--var', type=str, help="Name of variable")
    args = ap.parse_args()
    l = Launcher(args.config,
                 args.var,
                 mode=args.mode)
