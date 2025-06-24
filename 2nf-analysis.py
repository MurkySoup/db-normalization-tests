#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Normalized Forms DB Schema Analyzer for MySQL/MariaDB databases
Version 0.3-Alpha (Do Not Distribute)
By Rick Pelletier (galiagante@gmail.com), 23 June 2025
Last Update: 23 June 2025

AI Content: Original code by MS CoPilot


Example usage:

# ./2nf-analysis.py --host "${DB_HOST}" --user "${DB_USER}" --password "${DB_PWD}" --name "${DB_NAME}"

Second Normal Form (2NF): Each non-key attribute must rely on the entire primary key.
"""


import sys
from sqlalchemy import create_engine, inspect
import pandas as pd
import pymysql
import argparse
from itertools import combinations


def open_database(host, user, password, database):
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

        print()
        print(f'Connection to mysql://{host}/{database} established')
    except Exception as e:
        print(e)
        print()

        return False

    return engine


def close_database(engine):
    try:
        engine.dispose()

        print(f'Database connection closed')
        print()
    except Exception as e:
        print(e)
        print()

        return False

    return True


def analyze_mysql_second_normal_form(engine):
    inspector = inspect(engine)
    issues = []

    print()

    for table in inspector.get_table_names():
        print(f'Analyzing table: {table}')
        df = pd.read_sql_table(table, con=engine)

        # Get primary key columns
        pk = inspector.get_pk_constraint(table).get('constrained_columns', [])

        if not pk:
            print(f'  Skipping: Table "{table}" has no primary key.')
            continue

        # Identify non-key attributes
        non_key_attrs = [col for col in df.columns if col not in pk]

        # If composite key, check for partial dependencies
        if len(pk) > 1:
            for attr in non_key_attrs:
                for key_part in pk:
                    grouped = df.groupby(key_part)[attr].nunique()

                    if (grouped > 1).any():
                        issues.append(f'Table "{table}" attribute "{attr}" may depend only on part of the composite key "{key_part}".')
    print()
    print('2NF Violations Detected:')

    if issues:
        for issue in issues:
            print(f'-> {issue}')
    else:
        print('-> None')

    print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', '-t', type=str, required=True)
    parser.add_argument('--user', '-u', type=str, required=True)
    parser.add_argument('--password', '-p', type=str, required=True)
    parser.add_argument('--name', '-n', type=str, required=True)
    args = parser.parse_args()

    if (engine := open_database(args.host, args.user, args.password, args.name)) is False:
        sys.exit(1)

    analyze_mysql_second_normal_form(engine)

    if (flag := close_database(engine)) is False:
        sys.exit(1)

    sys.exit(0)
else:
    sys.exit(1)

# end of script
