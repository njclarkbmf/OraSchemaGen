# OraSchemaGen

OraSchemaGen is a modular Python tool that generates Oracle DDL and PL/SQL as SQL scripts. It produces CREATE TABLE statements, constraints, indexes, sequences, comments, triggers, procedures, functions, packages, and LOB utility code -- along with bilingual English/Japanese sample data using the Faker library.

**What it does:** Generates SQL scripts that you can review and execute against an Oracle database yourself.

**What it does NOT do:**
- Does NOT connect to a live Oracle database
- Does NOT execute the generated SQL
- Does NOT guarantee 100% Oracle compatibility for all edge cases
- Is NOT a replacement for proper Oracle schema design

All generated SQL **must be reviewed** before executing against any database, especially production.

## Supported Oracle Versions

Oracle Database 12c Release 2 through 19c.

## Prerequisites

- Python 3.7+
- `faker` library
- `tqdm` library

## Installation

1. Clone the repository:

```bash
git clone git@github.com:njclarkbmf/OraSchemaGen.git
cd OraSchemaGen
```

2. Create and activate a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic

```bash
python main.py
```

Generates 5 tables with 100 rows of sample data each, plus default counts of triggers, procedures, functions, packages, and LOB objects. Output goes to the `generated_sql/` directory.

### Common Examples

```bash
# Custom counts and output directory
python main.py --tables 8 --data-rows 100 --output-dir my_output

# Single consolidated SQL file
python main.py --single-file

# Disable progress bars (cleaner for scripting/logging)
python main.py --no-progress

# Multiple schemas
python main.py --schemas "HR,FINANCE" --tables 10

# Full options
python main.py \
  --schemas "HR" \
  --tables 10 \
  --data-rows 500 \
  --triggers 5 \
  --procedures 5 \
  --functions 3 \
  --packages 2 \
  --lobs 1 \
  --output-dir "my_sql_files" \
  --single-file \
  --verbose
```

### Command-line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--schemas` | Comma-separated list of schema names | `TEST_SCHEMA` |
| `--tables` | Number of tables per schema | `5` |
| `--data-rows` | Rows of sample data per table | `100` |
| `--triggers` | Number of triggers per schema | `3` |
| `--procedures` | Number of procedures per schema | `3` |
| `--functions` | Number of functions per schema | `3` |
| `--packages` | Number of packages per schema | `1` |
| `--lobs` | Number of LOB operation objects per schema | `1` |
| `--output-dir` | Output directory for SQL files | `generated_sql` |
| `--single-file` | Write one SQL file instead of multiple | `False` |
| `--shift-jis` | Convert output to Shift-JIS encoding | `False` |
| `--verbose` | Enable debug-level logging | `False` |
| `--quiet` | Suppress INFO-level logging | `False` |
| `--no-progress` | Disable progress bars | `False` |

### Selective Generation

Object types are included based on their count flags. Set a count to `0` to skip that object type entirely:

```bash
# Tables and data only -- no PL/SQL objects
python main.py --triggers 0 --procedures 0 --functions 0 --packages 0 --lobs 0
```

## Generated Object Types

### DDL Objects

- **Tables** with column definitions and data types
- **Primary key constraints**
- **Unique constraints**
- **Check constraints**
- **Foreign key constraints** (only for tables that are actually created)
- **Indexes** (on foreign key and frequently queried columns)
- **Sequences** (for primary key auto-increment)
- **Comments** on tables and columns (bilingual EN/JP)
- **Storage clauses** (assumes a `USERS` tablespace exists)

### PL/SQL Objects

- **Triggers** -- including BEFORE INSERT (for PK population), BEFORE UPDATE (audit columns), and system-level triggers (AFTER DDL ON DATABASE, AFTER LOGON ON DATABASE). System triggers require DBA privileges.
- **Procedures** -- CRUD-style procedures for generated tables. Procedures use `VARCHAR2('Y'/'N')` instead of `BOOLEAN` for OUT parameters so they are callable from SQL (Oracle does not support BOOLEAN in SQL context).
- **Functions** -- Utility functions (date formatting, validation, lookups).
- **Packages** -- Grouped procedures and functions with matching spec and body signatures. Package spec and body signatures are guaranteed to match.
- **LOB utilities** -- Helper procedures for CLOB/BLOB read, write, and append operations.

### Sample Data

- Bilingual INSERT statements with English and Japanese values
- Columns ending in `_JP` or `_JAPANESE` contain Japanese-generated content
- DATE and TIMESTAMP columns use explicit `TO_DATE` / `TO_TIMESTAMP` with format masks

## Output Format

Output is UTF-8 encoded SQL scripts organized as:

```
generated_sql/
├── DDL/
│   ├── 01_tables.sql
│   ├── 02_constraints.sql
│   ├── 03_indexes.sql
│   ├── 04_sequences.sql
│   └── 05_comments.sql
├── DATA/
│   └── 01_data.sql
├── TRIGGERS/
│   └── 01_triggers.sql
├── PROCEDURES/
│   └── 01_procedures.sql
├── FUNCTIONS/
│   └── 01_functions.sql
├── PACKAGES/
│   ├── 01_package_spec.sql
│   └── 02_package_body.sql
└── LOBS/
    └── 01_lob_utilities.sql
```

Use `--single-file` to consolidate all output into one `.sql` file. Use `--shift-jis` for Shift-JIS encoded output (only supported with `--single-file`).

## Validation

A test suite is included to verify basic generation behavior:

```bash
python -m unittest tests.test_sql_generation -v
```

## Known Limitations

1. **Review required:** All generated SQL must be reviewed before executing against production databases. The output is a starting point, not a finished product.
2. **Storage clauses:** Table storage clauses assume a `USERS` tablespace exists. Adjust or remove if your environment differs.
3. **System triggers:** AFTER DDL ON DATABASE and AFTER LOGON ON DATABASE triggers require DBA privileges and are not appropriate for all environments.
4. **VARCHAR2 instead of BOOLEAN:** Generated procedures use `VARCHAR2('Y'/'N')` instead of `BOOLEAN` for OUT parameters because Oracle does not support BOOLEAN in SQL context.
5. **Date literals:** DATE and TIMESTAMP columns use explicit `TO_DATE` / `TO_TIMESTAMP` calls with format masks in generated INSERT statements.
6. **Foreign keys:** Foreign key constraints are only generated for tables that are actually created within the same generation run. Cross-schema or pre-existing table references are not handled.
7. **Package consistency:** Package spec and body signatures are guaranteed to match, but the generated procedure/function bodies are templates -- adjust logic to your needs.
8. **Oracle version coverage:** Tested against Oracle 12cR2 through 19c behavior. Older versions (11g and earlier) and newer versions (21c+) are not guaranteed to work without modification.
9. **No database connection:** This tool generates SQL files only. It does not connect to, query, or execute against any database.

## Architecture

OraSchemaGen uses a modular generator architecture:

| Module | Responsibility |
|--------|---------------|
| `core.py` | Base classes (`OracleObject`, `OracleObjectGenerator`, `OracleObjectFactory`, `OutputHandler`, `TableInfo`) |
| `schema_generator.py` | Table DDL, constraints, indexes, sequences, comments |
| `data_generator.py` | Bilingual sample data (INSERT statements) via Faker |
| `trigger_generator.py` | Row-level and system-level triggers |
| `procedure_generator.py` | CRUD-style procedures |
| `function_generator.py` | Utility functions |
| `package_generator.py` | Package specs and bodies |
| `lob_generator.py` | CLOB/BLOB utility procedures |
| `main.py` | CLI entry point, orchestration |

Each generator implements `OracleObjectGenerator` and produces `OracleObject` instances. The `OracleObjectFactory` creates the appropriate generators based on CLI flags. `OutputHandler` writes the results to files.

## Documentation

- **Architecture Decision Records (ADRs):** See `docs/adr/` for recorded design decisions.
- **Design Principles and Patterns:** See `docs/principles-and-patterns.md` for the architectural principles guiding this project.

## Contributing

Contributions are welcome.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Run the test suite: `python -m unittest tests.test_sql_generation -v`
5. Commit and push
6. Open a Pull Request

Please keep changes focused and include tests where applicable.

## License

MIT. See the [LICENSE](LICENSE) file for details.
