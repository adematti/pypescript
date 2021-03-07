"""**pypescript** main entry point."""

import argparse

from .utils import setup_logging
from .main import main as pypescript_main

ascii_art = """\
                                        _       _
                                       (_)     | |
  _ __  _   _ _ __   ___  ___  ___ _ __ _ _ __ | |_
 | '_ \| | | | '_ \ / _ \/ __|/ __| '__| | '_ \| __|
 | |_) | |_| | |_) |  __/\__ \ (__| |  | | |_) | |_
 | .__/ \__, | .__/ \___||___/\___|_|  |_| .__/ \__|
 | |     __/ | |                         | |
 |_|    |___/|_|                         |_|        \n"""


def main(args=None):
    print(ascii_art)
    parser = argparse.ArgumentParser(description=main.__doc__,formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--config-fn', type=str, required=True,
                        help='Name of configuration file')
    parser.add_argument('--pipe-graph-fn', type=str, default=None,
                        help='If provided, save graph of the pipeline to this file name')
    opt = parser.parse_args(args=args)
    setup_logging()
    return pypescript_main(config=opt.config_fn,pipe_graph_fn=opt.pipe_graph_fn)


if __name__ == '__main__':

    main()
