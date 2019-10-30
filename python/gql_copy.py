#!/usr/bin/env python3.6

"Script for copying one tsv-like file to another.

Example:

gql_copy foo.btsv bar.tsv

"""
import argparse
import logging
import subprocess
import gql

def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    p = argparse.ArgumentParser()
    p.add_argument('src')
    p.add_argument('dest')
    args = p.parse_args()

    gql_path = gql.compile()
    logging.info(f'Copying {args.src} -> {args.dest}')
    gql.copy_tsv(gql_path, "/tmp/cp", args.src, args.dest)

main()
