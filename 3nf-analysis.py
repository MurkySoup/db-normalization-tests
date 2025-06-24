#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Normal Forms (Test Code) DB Schema Analyzer for MySQL/MariaDB databases
Version 0.3-Alpha (Do Not Distribute)
By Rick Pelletier (galiagante@gmail.com), 23 June 2025
Last Update: 23 June 2025

AI Content: Original code by MS CoPilot


Example usage:

# ./3nf-analysis.py --host "${DB_HOST}" --user "${DB_USER}" --password "${DB_PWD}" --name "${DB_NAME}"

Third Normal Form (3NF): Every attribute in a table should depend on the key,
the whole key and nothing buy the key. This is also called Boyce-Codd Normal Form.
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


def discover_functional_dependencies(engine):
    inspector = inspect(engine)
    fd_results = {}

    for table in inspector.get_table_names():
        df = pd.read_sql_table(table, con=engine)
        columns = df.columns.tolist()
        fds = []

        for rhs in columns:
            lhs_candidates = [col for col in columns if col != rhs]

            for r in range(1, len(lhs_candidates) + 1):
                for lhs in combinations(lhs_candidates, r):
                    grouped = df.groupby(list(lhs))[rhs].nunique()

                    if (grouped <= 1).all():
                        fds.append((set(lhs), {rhs}))

                        break  # Stop at the smallest LHS that determines RHS

        fd_results[table] = fds

    return fd_results


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


def analyze_mysql_third_normal_form(engine, functional_dependencies):
    inspector = inspect(engine)
    issues = []

    print()

    for table in inspector.get_table_names():
        print(f'Analyzing table: {table}')
        df = pd.read_sql_table(table, con=engine)
        all_attrs = set(df.columns)

        # Get primary key
        pk = inspector.get_pk_constraint(table).get('constrained_columns', [])
        if not pk:
            print(f'  Skipping: Table "{table}" has no primary key.')
            continue

        # Check each functional dependency
        for lhs, rhs in functional_dependencies.get(table, []):
            if not is_superkey(lhs, all_attrs, functional_dependencies[table]):
                issues.append(f'Table "{table}" violates 3NF/BCNF: {lhs} → {rhs} (LHS is not a superkey)')

    engine.dispose()

    print()
    print('3NF/BCNF Violations Detected:')

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

    analyze_mysql_third_normal_form(engine, discover_functional_dependencies(engine))

    if (flag := close_database(engine)) is False:
        sys.exit(1)

    sys.exit(0)
else:
    sys.exit(1)

# end of script
