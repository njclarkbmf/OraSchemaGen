# OraSchemaGen

OraSchemaGen is a modular Python tool for generating Oracle database schemas, including tables, constraints, indexes, data, and PL/SQL objects (triggers, procedures, functions, and packages). It features bilingual support for English and Japanese content.

## Features

- Generate complete Oracle database schemas
- Create realistic sample data
- Generate PL/SQL objects (triggers, procedures, functions, packages)
- Bilingual support (English and Japanese)
- Modular architecture for easy extension
- Configurable via command-line options

## Progress Tracking

OraSchemaGen includes visual progress tracking using tqdm to provide real-time feedback during schema generation:

- Progress bars for each schema being generated
- Component-specific progress (tables, data, triggers, etc.)
- File generation progress

### Progress Options

| Option | Description |
|--------|-------------|
| `--no-progress` | Disable progress bars for cleaner log output |

Progress bars are particularly useful when generating large schemas with many rows of data, making it easy to track the generation status and estimate completion time.


## Installation

### Prerequisites

- Python 3.7+
- Oracle client libraries (if connecting to Oracle databases)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/OraSchemaGen.git
cd OraSchemaGen
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

To generate a basic schema with default settings:

```bash
python main.py
```

This will create a schema with 5 tables and minimal data in the `generated_sql` directory.

### Advanced Usage

```bash
python main.py --schemas "HR,FINANCE" --tables 10 --data-rows 500 --triggers 5 --procedures 5 --functions 3 --packages 2 --lobs 1 --output-dir "my_sql_files" --verbose
```

### Command-line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--schemas` | Comma-separated list of schema names | TEST_SCHEMA |
| `--tables` | Number of tables to generate per schema | 5 |
| `--data-rows` | Number of data rows to generate per table | 100 |
| `--triggers` | Number of triggers to generate per schema | 3 |
| `--procedures` | Number of procedures to generate per schema | 3 |
| `--functions` | Number of functions to generate per schema | 3 |
| `--packages` | Number of packages to generate per schema | 1 |
| `--lobs` | Number of LOB operations to generate per schema | 1 |
| `--output-dir` | Directory to save generated SQL | generated_sql |
| `--single-file` | Generate a single SQL file instead of multiple files | False |
| `--shift-jis` | Convert output files to Shift-JIS encoding | False |
| `--verbose` | Enable verbose logging | False |

## Project Structure

The project follows a modular architecture with the following components:

- `main.py` - Main program that ties everything together
- `core.py` - Core classes and base functionality
- `schema_generator.py` - Table and basic schema generation
- `data_generator.py` - Test data generation
- `trigger_generator.py` - Trigger generation
- `procedure_generator.py` - Procedure generation
- `function_generator.py` - Function generation
- `package_generator.py` - Package generation
- `lob_generator.py` - LOB operations generation

### Architecture

OraSchemaGen uses a modular architecture based on object generators:

1. `OracleObject` - Base class for database objects
2. `OracleObjectGenerator` - Abstract base class for generators
3. `OracleObjectFactory` - Factory for creating generators
4. `OutputHandler` - Handles writing to files

Each generator inherits from `OracleObjectGenerator` and implements a `generate()` method that produces `OracleObject` instances.

## Example Generated Objects

### Tables

```sql
CREATE TABLE EMPLOYEES 
(
  EMPLOYEE_ID NUMBER(6) NOT NULL,
  FIRST_NAME VARCHAR2(20),
  LAST_NAME VARCHAR2(25) NOT NULL,
  FIRST_NAME_JP VARCHAR2(20),
  LAST_NAME_JP VARCHAR2(25),
  EMAIL VARCHAR2(25) NOT NULL,
  PHONE_NUMBER VARCHAR2(20),
  HIRE_DATE DATE NOT NULL,
  JOB_ID VARCHAR2(10) NOT NULL,
  SALARY NUMBER(8,2),
  COMMISSION_PCT NUMBER(2,2),
  MANAGER_ID NUMBER(6),
  DEPARTMENT_ID NUMBER(4),
  NOTES_JP CLOB,
  CONSTRAINT EMPLOYEES_PK PRIMARY KEY (EMPLOYEE_ID)
)
TABLESPACE USERS PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
NOLOGGING STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT);
```

### Sample Data

```sql
INSERT INTO EMPLOYEES (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, FIRST_NAME_JP, LAST_NAME_JP, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB_ID, SALARY, COMMISSION_PCT, MANAGER_ID, DEPARTMENT_ID) 
VALUES (EMPLOYEES_SEQ.NEXTVAL, 'John', 'Smith', '山田', '太郎', 'JSMITH', '555-123-4567', '2020-05-15', 'IT_PROG', '65000', NULL, '103', '60');
```

### PL/SQL Packages

```sql
CREATE OR REPLACE PACKAGE HR_PKG_1 AS
    -- Constants
    C_MAX_ATTEMPTS CONSTANT INTEGER := 3;
    C_DEFAULT_PAGE_SIZE CONSTANT INTEGER := 25;

    -- Functions
    -- Format a date using specified format
    FUNCTION get_formatted_date(p_date IN DATE, p_format IN VARCHAR2 DEFAULT 'YYYY-MM-DD') RETURN VARCHAR2;

    -- Check if a string contains only numeric characters
    FUNCTION is_numeric(p_string IN VARCHAR2) RETURN BOOLEAN;

    -- Procedures
    -- Validate EMPLOYEES record
    PROCEDURE validate_employees(p_employee_id IN NUMBER, p_is_valid OUT BOOLEAN, p_error_message OUT VARCHAR2);
END HR_PKG_1;
/
```

## Bilingual Support

OraSchemaGen generates bilingual content for tables and data. Fields ending with `_JP` or `_JAPANESE` will contain Japanese content.

For example:

```sql
INSERT INTO EMPLOYEES (FIRST_NAME, LAST_NAME, FIRST_NAME_JP, LAST_NAME_JP) 
VALUES ('John', 'Smith', '山田', '太郎');
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- The Oracle documentation for SQL and PL/SQL syntax
- The Faker library for generating realistic sample data
