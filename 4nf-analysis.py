#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fourth Normal Form (4NF) DB Schema Analyzer for MySQL/MariaDB databases
Version 0.3-Alpha (Do Not Distribute)
By Rick Pelletier (galiagante@gmail.com), 23 June 2025
Last Update: 23 June 2025

AI Content: Original code by MS CoPilot


Example usage:

# ./4nf-analysis.py --host "${DB_HOST}" --user "${DB_USER}" --password "${DB_PWD}" --name "${DB_NAME}"

Fourth Normal Form (4NF) Multi-value dependencies in a table must use multi-value dependencies on the key.
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


def attribute_closure(attributes, functional_dependencies):
    closure = set(attributes)
    changed = True

    while changed:
        changed = False

        for lhs, rhs in functional_dependencies:
            if lhs.issubset(closure) and not rhs.issubset(closure):
                closure.update(rhs)
                changed = True

    return closure


def is_superkey(attributes, all_attributes, functional_dependencies):
    return attribute_closure(attributes, functional_dependencies) >= set(all_attributes)


def discover_mvd_candidates(df):
    columns = df.columns.tolist()
    mvd_candidates = []

    for lhs_col in columns:
        for rhs1, rhs2 in combinations([col for col in columns if col != lhs_col], 2):
            grouped1 = df.groupby(lhs_col)[rhs1].nunique()
            grouped2 = df.groupby(lhs_col)[rhs2].nunique()

            if (grouped1 > 1).any() and (grouped2 > 1).any():
                mvd_candidates.append((lhs_col, rhs1, rhs2))

    return mvd_candidates


def analyze_mysql_fourth_normal_form(engine):
    inspector = inspect(engine)
    issues = []

    for table in inspector.get_table_names():
        print(f'Analyzing table: {table}')
        df = pd.read_sql_table(table, con=engine)
        all_attrs = set(df.columns)

        pk = inspector.get_pk_constraint(table).get('constrained_columns', [])

        if not pk:
            print(f'  Skipping: Table "{table}" has no primary key.')
            continue

        # Assume functional dependencies from primary key
        fds = [(set(pk), {attr}) for attr in df.columns if attr not in pk]

        mvd_candidates = discover_mvd_candidates(df)

        for lhs, rhs1, rhs2 in mvd_candidates:
            if not is_superkey({lhs}, all_attrs, fds):
                issues.append(f'Table "{table}" violates 4NF: {lhs} ↠ {rhs1}, {rhs2} (LHS is not a superkey)')

    print()
    print('4NF Violations Detected:')

    if issues:
        for issue in issues:
            print(f'-> {issue}')
    else:
        print('-> None')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', '-t', type=str, required=True)
    parser.add_argument('--user', '-u', type=str, required=True)
    parser.add_argument('--password', '-p', type=str, required=True)
    parser.add_argument('--name', '-n', type=str, required=True)
    args = parser.parse_args()

    if (engine := open_database(args.host, args.user, args.password, args.name)) is False:
        sys.exit(1)

    analyze_mysql_fourth_normal_form(engine)

    if (flag := close_database(engine)) is False:
        sys.exit(1)

    sys.exit(0)
else:
    sys.exit(1)

# end of script
