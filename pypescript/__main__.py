"""**pypescript** main entry point."""

import datetime
import argparse

from ._version import __version__
from .utils import setup_logging
from .main import main as pypescript_main
from .mpi import CurrentMPIComm

ascii_art = """\
                                        _       _
                                       (_)     | |
  _ __  _   _ _ __   ___  ___  ___ _ __ _ _ __ | |_
 | '_ \| | | | '_ \ / _ \/ __|/ __| '__| | '_ \| __|
 | |_) | |_| | |_) |  __/\__ \ (__| |  | | |_) | |_
 | .__/ \__, | .__/ \___||___/\___|_|  |_| .__/ \__|
 | |     __/ | |                         | |
 |_|    |___/|_|                         |_|        \n\n""" + \
 """version: {}                     date: {}\n""".format(__version__,datetime.date.today())


def main(args=None):
    if CurrentMPIComm.get().rank == 0: print(ascii_art)
    parser = argparse.ArgumentParser(description=main.__doc__,formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('config_fn', type=str, help='Name of configuration file')
    parser.add_argument('--pipe-graph-fn', type=str, default=None,
                        help='If provided, save graph of the pipeline to this file name')
    parser.add_argument('--log-level', type=str, default='info', choices=['warning','info','debug'],
                        help='Logging level')
    opt = parser.parse_args(args=args)
    setup_logging(level=opt.log_level)
    return pypescript_main(config=opt.config_fn,pipe_graph_fn=opt.pipe_graph_fn)


if __name__ == '__main__':

    main()
