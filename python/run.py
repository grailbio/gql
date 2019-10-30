#!/usr/bin/env python3

"""Convenience wrapper for GQL. It starts a script then runs a profiler
continuously until the script finishes. It send email when the script starts and
finishes.

Usage: run.py gql/foobar.gql

"""

import argparse
import os
import logging
import re
import sys
import concurrent.futures
from typing import List

import gql

def main() -> None:
    "Main entry point"
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--quintile', action='store_true')
    parser.add_argument('--overwrite', action='store_true',
                        help="Pass --overwrite-files option to gql")
    parser.add_argument('--max_retries', default=0, type=int,
                        help="""Max number of times to retry a failed gql job.
                        Zero (default) means no retries""")
    parser.add_argument('script', type=str)
    parser.add_argument('params', nargs='*')
    args = parser.parse_args()
    args.script = os.path.realpath(args.script)

    gql_flags = []
    if args.overwrite:
        gql_flags.append('--overwrite-files=true')

    gql_params = {}
    for param in args.params:
        m = re.match('--([^=]+)=(.*)', param)
        if not m:
            raise Exception(f'GQL param must be in form --key=value, but got {param}')
        gql_params[m.group(1)] = m.group(2)

    gql_sess = gql.GQL(log_prefix=f'/tmp/{os.path.splitext(os.path.basename(args.script))[0]}',
                       default_flags=gql_flags,
                       default_max_retries=args.max_retries)
    gql.send_mail(f'gql {args.script} started', f'Args: {sys.argv}')


    pool = concurrent.futures.ThreadPoolExecutor(max_workers=128)
    results: List[concurrent.futures.Future] = []

    if args.quintile:
        for quintile in range(0, 5):
            params = dict(gql_params)
            params['quintile'] = f'{quintile}'
            callback = lambda: gql_sess.run(label=f'{quintile}',
                                            script_path=args.script,
                                            gql_params=params)
            results.append(pool.submit(callback))
    else:
        callback = lambda: gql_sess.run(label='', script_path=args.script, gql_params=gql_params)
        results.append(pool.submit(callback))

    gql_status = [r.result() for r in results]
    print(f"Jobs finished with status {gql_status}\n")
    gql.send_mail(f'gql {args.script} finished with {gql_status}', f'Args: {sys.argv}')

main()
