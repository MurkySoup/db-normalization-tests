#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
First Normal Form (1NF) DB Schema Analyzer for MySQL/MariaDB databases
Version 0.3-Alpha (Do Not Distribute)
By Rick Pelletier (galiagante@gmail.com), 23 June 2025
Last Update: 23 June 2025

AI Content: Original code by MS CoPilot


Example usage:

# ./1nf-analysis.py --host "${DB_HOST}" --user "${DB_USER}" --password "${DB_PWD}" --name "${DB_NAME}"

First Normal Form (1NF):
- Each table cell should contain only a single value.
- Each column should have a unique name.
- Using row order to convey information is not permitted.
- Mixing data types within the same column is not permitted.
- Having a table without a primary key is not permitted.
- Repeating groups are not permitted.
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


def analyze_mysql_first_normal_form(engine):
    inspector = inspect(engine)
    issues = []

    print()

    for table in inspector.get_table_names():
        print(f'Analyzing table: {table}')
        df = pd.read_sql_table(table, con=engine)

        # Rule 1: Each table cell should contain only a single value
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, str) and (',' in x or ';' in x)).any():
                issues.append(f'Table "{table}" column "{col}" might contain multiple values in a single cell.')

        # Rule 2: Each column should have a unique name
        if len(df.columns) != len(set(df.columns)):
            issues.append(f'Table "{table}" has duplicate column names.')

        # Rule 3: Using row order to convey information is not permitted.
        # XXX This cannot be programmatically verified with understanding the context of the data being analyzed

        # Rule 4: Mixing data types within the same column is not permitted
        for col in df.columns:
            types = df[col].dropna().map(type).unique()
            if len(types) > 1:
                issues.append(f'Table "{table}" column "{col}" contains mixed data types: {types}')

        # Rule 5: Having a table without a primary key is not permitted
        pk = inspector.get_pk_constraint(table)
        if not pk.get('constrained_columns'):
            issues.append(f'Table "{table}" does not have a primary key.')

        # Rule 6: Repeating groups are not permitted
        repeating_cols = [col for col in df.columns if any(char.isdigit() for char in col)]
        if repeating_cols:
            issues.append(f'Table "{table}" might contain repeating groups: {repeating_cols}')

    print()
    print('1NF Violations Detected:')

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

    analyze_mysql_first_normal_form(engine)

    if (flag := close_database(engine)) is False:
        sys.exit(1)

    sys.exit(0)
else:
    sys.exit(1)

# end of script
