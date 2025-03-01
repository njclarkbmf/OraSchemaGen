# OraSchemaGen: Oracle Schema Export Generator with Shift-JIS Support

OraSchemaGen is a specialized Python utility that generates realistic Oracle Database schema export files with customizable data volumes and full Shift-JIS encoding support. It's particularly valuable for developers, testers, and database administrators who need Oracle-compatible SQL files without access to an actual Oracle instance.

## Key Features

- Generates authentic Oracle Database export files that closely mimic real Oracle exports
- Creates comprehensive schema definitions including tables, constraints, indexes, sequences, and comments  
- Produces realistic sample data with intelligent value generation based on column names and types
- Supports Japanese data through built-in Shift-JIS encoding
- Includes oracle-style PL/SQL objects (procedures, functions, packages, and triggers)
- Offers full control over generation parameters through a simple command-line interface
- Features progress bars and detailed statistics for monitoring large file generation
- Compatible with tools like ora2pg for Oracle-to-PostgreSQL migration testing

## Use Cases

OraSchemaGen is ideal for:

- Testing Oracle-to-PostgreSQL migration tools like ora2pg without requiring an Oracle license
- Setting up development and testing environments with realistic Oracle-compatible schema structures
- Training and educational environments where Oracle syntax and structure demonstration is needed
- Preparing mock data for application testing that requires Japanese character support
- Benchmarking database import operations with controllable data volumes

## Differentiation

Unlike generic SQL generators, OraSchemaGen specifically emulates Oracle's export format, including:

- Oracle-specific storage clauses and tablespace parameters
- Proper constraint, sequence, and index creation syntax
- PL/SQL objects with correct Oracle syntax
- Export headers and footers that match Oracle's export utility output
- Japanese character support through Shift-JIS encoding (critical for applications used in Japan)

Whether you're planning database migrations, testing applications that require Oracle compatibility, or simply need realistic Oracle-like structures without the licensing costs, OraSchemaGen offers a streamlined solution for generating high-quality Oracle schema exports on demand.
