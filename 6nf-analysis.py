#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sixth Normal Form (6NF) DB Schema Analyzer for MySQL/MariaDB databases
Version 0.3-Alpha (Do Not Distribute)
By Rick Pelletier (galiagante@gmail.com), 23 June 2025
Last Update: 23 June 2025

AI Content: Original code by MS CoPilot


Example usage:

# ./6nf-analysis.py --host "${DB_HOST}" --user "${DB_USER}" --password "${DB_PWD}" --name "${DB_NAME}"

Sixth Normal Form (6NF): A database must not contain constraints other than domain constraints
and key constraints (also called Domain Key Normal Form).
"""


import sys
from sqlalchemy import create_engine, inspect, text
import pymysql
import argparse


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


def analyze_mysql_sixth_normal_form(engine):
    inspector = inspect(engine)
    issues = []

    print()
    print('Analyzing Domain-Key Normal Form (6NF/DKNF) Compliance\n')

    for table in inspector.get_table_names():
        print(f'Checking table: {table}')
        pk = inspector.get_pk_constraint(table).get('constrained_columns', [])
        columns = inspector.get_columns(table)

        # Check for domain constraints (data types and nullability)
        for col in columns:
            if col['type'] is None:
                issues.append(f'Table "{table}" column "{col["name"]}" has no domain constraint (type unspecified).')

        # Check for key constraints
        if not pk:
            issues.append(f'Table "{table}" has no primary key defined.')

        # Check for other constraints (foreign keys, unique, check constraints)
        fks = inspector.get_foreign_keys(table)

        if fks:
            issues.append(f'Table "{table}" has foreign key constraints: {fks}')

        uniques = inspector.get_unique_constraints(table)
        for uc in uniques:
            if set(uc['column_names']) != set(pk):
                issues.append(f'Table "{table}" has unique constraint on {uc["column_names"]} not part of primary key.')

        # Check for CHECK constraints (not supported in older MySQL versions)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT CONSTRAINT_NAME, CHECK_CLAUSE 
                FROM information_schema.CHECK_CONSTRAINTS 
                WHERE CONSTRAINT_SCHEMA = DATABASE() 
                  AND CONSTRAINT_NAME IN (
                      SELECT CONSTRAINT_NAME 
                      FROM information_schema.TABLE_CONSTRAINTS 
                      WHERE TABLE_NAME = :table
                  )
            """), {'table': table})
            checks = result.fetchall()
            if checks:
                issues.append(f'Table "{table}" has CHECK constraints: {checks}')

    print()
    print('DKNF Violations Detected:')

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

    analyze_mysql_sixth_normal_form(engine)

    if (flag := close_database(engine)) is False:
        sys.exit(1)

    sys.exit(0)
else:
    sys.exit(1)

# end of script
