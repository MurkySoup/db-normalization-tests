# db-normalization-tests

Various basic scripts to test MySQL/MariaDB databases for adherence to Normal Forms

## Description

This collection of scripts is intended to provide basic testing to help a DBA work through Normal Forms as a "best practice".

These tools are rather basic and provide little more than guide-points and a given database will likely still require an in-context analysis by a DBA. The goal of these tools is to provide points of interest for more in-depth study, saving the DBA time and allowing for more focused efforts.

## Prerequisites

Requires Python 3.x (preferrably 3.8+) and uses the following libraries:

* sys
* argparse
* sqlalchemy
* pandas
* pymysql
* itertools

## Primer on Database Normalization Theory

Normalize Your Way to Correct Database Design:

First Normal Form (1NF):
* Each table cell should contain only a single value.
* Each column should have a unique name.
* Using row order to convey information is not permitted.
* Mixing data types within the same column is not permitted.
* Having a table without a primary key is not permitted.
* Repeating groups are not permitted.

Second Normal Form (2NF): Each non-key attribute must rely on the entire primary key.

Third Normal Form (3NF): Every attribute in a table should depend on the key, the whole key and nothing buy the key. This is also called Boyce-Codd Normal Form (BCNF).

Fourth Normal Form (4NF): Multi-value dependencies in a table must use multi-value dependencies on the key.

Fifth Normal Form (5NF): The table (already in 4NF) cannot be described as the logical result of joining some other tables together.

Sixth Normal Form (6NF): A database must not contain constraints other than domain constraints and key constraints. This also called Domain Key Normal Form (DKNF).

Rules that Define Primary Keys:
* Every row must have a primary key value.
* Two rows cannot have the same primary key value.
* The primary key field cannot be null.
* The value in a primary key column can never be modified or updated if any foreign key refers to that primary key.

Notes on Types of Database Keys:
* Super Key: A super key is a group of single or multiple keys which identifies rows in a table.
* Primary Key: A column or group of columns in a table that uniquely identify every row in that table.
* Candidate Key: A set of attributes that uniquely identify tuples in a table. Candidate Key is a super key with no repeated attributes.
* Alternate Key: A column or group of columns in a table that uniquely identify every row in that table.
* Foreign Key: A column that creates a relationship between two tables. The purpose of Foreign keys is to maintain data integrity and allow navigation between two different instances of an entity.
* Compound Key: Has two or more attributes that allow you to uniquely recognize a specific record. It is possible that each column may not be unique by itself within the database.
* Composite Key: A combination of two or more columns that uniquely identify rows in a table. The combination of columns guarantees uniqueness, though individual uniqueness is not guaranteed.
* Surrogate Key: An artificial key which aims to uniquely identify each record is called a surrogate key. These kind of keys are unique because they are created when you don't have any natural primary key.

## Built With

* [Python](https://www.python.org) designed by Guido van Rossum

## Author

**Rick Pelletier** - [Gannett Co., Inc. (USA Today Network)](https://www.usatoday.com/)
