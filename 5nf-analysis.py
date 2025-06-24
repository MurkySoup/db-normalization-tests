#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fifth Normal Forms (5NF) DB Schema Analyzer for MySQL/MariaDB databases
Version 0.3-Alpha (Do Not Distribute)
By Rick Pelletier (galiagante@gmail.com), 23 June 2025
Last Update: 23 June 2025

AI Content: Original code by MS CoPilot


Example usage:

# ./5nf-analysis.py --host "${DB_HOST}" --user "${DB_USER}" --password "${DB_PWD}" --name "${DB_NAME}"

Fifth Normnal Form (5NF): A given table (already in 4NF) cannot be described as the logical result
  of joining some other tables together.
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


def get_candidate_keys(df):
    # Heuristic: Try all combinations of columns to find minimal superkeys
    columns = df.columns.tolist()

    for r in range(1, len(columns) + 1):
        for combo in combinations(columns, r):
            if df.duplicated(subset=combo).sum() == 0:
                yield set(combo)


def analyze_mysql_fifth_normal_form(engine):
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    print()
    print('Fifth Normal Form (5NF) Analysis\n')

    for table in tables:
        print(f'Analyzing table: {table}')
        df = pd.read_sql_table(table, con=engine)

        if df.empty or len(df.columns) < 3:
            print(f'  Skipping: Table "{table}" has insufficient data or columns.')
            continue

        candidate_keys = list(get_candidate_keys(df))
        if not candidate_keys:
            print(f'  Warning: No candidate keys found for table "{table}".')

            continue

        columns = df.columns.tolist()
        violations = []

        # Check all 3-way projections for join dependency violations
        for attrs in combinations(columns, 3):
            proj1 = df[[attrs[0], attrs[1]]].drop_duplicates()
            proj2 = df[[attrs[0], attrs[2]]].drop_duplicates()
            proj3 = df[[attrs[1], attrs[2]]].drop_duplicates()

            # Join projections
            join1 = pd.merge(proj1, proj2, on=attrs[0])
            join2 = pd.merge(join1, proj3, on=[attrs[1], attrs[2]])

            # Compare with original projection
            original = df[list(attrs)].drop_duplicates()

            if not original.equals(join2):
                if not any(set(attrs).issuperset(key) for key in candidate_keys):
                    violations.append(attrs)

        if violations:
            print(f'Violations detected in table "{table}":')

            for v in violations:
                print(f'-> Join dependency not preserved on attributes: {v}')
        else:
            print('-> No violations detected in table "{table}".')

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

    analyze_mysql_fifth_normal_form(engine)

    if (flag := close_database(engine)) is False:
        sys.exit(1)

    sys.exit(0)
else:
    sys.exit(1)

# end of script
