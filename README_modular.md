# OraSchemaGen - Modular Oracle Schema Generator

OraSchemaGen is a powerful Python utility for generating Oracle database schema exports with realistic tables, data, and advanced PL/SQL objects. It's particularly useful for development environments, testing, and educational purposes where an actual Oracle database may not be available.

## Key Features

- **Complete Schema Generation**: Creates tables, constraints, indexes, and sequences that mimic real Oracle databases
- **Realistic Test Data**: Generates appropriate sample data based on column names and types
- **Advanced PL/SQL Objects**: Produces triggers, procedures, functions, packages, and LOB operations
- **Japanese Language Support**: Full support for Japanese text with Shift-JIS encoding
- **Modular Architecture**: Select which object types to generate based on your needs
- **Progress Tracking**: Visual progress indicators for lengthy generation tasks
- **Flexible Output Options**: Generate a single file or split by object type

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/OraSchemaGen.git
cd OraSchemaGen

# Install required dependencies
pip install -r requirements.txt
```

## Requirements

- Python 3.6+
- Required packages:
  - faker
  - tqdm

## Usage

### Basic Usage

Generate a complete Oracle schema export with default settings:

```bash
python modular_OraSchemaGen.py
```

This will create `oracle_export.sql` with 6 tables, 20 rows per table, and all PL/SQL object types in Shift-JIS encoding.

### Command-line Options

```
usage: modular_OraSchemaGen.py [-h] [--tables TABLES] [--rows ROWS] [--output OUTPUT]
                  [--encoding {shift-jis,utf-8}] [--objects OBJECTS] [--no-header]
                  [--no-storage] [--split] [--output-dir OUTPUT_DIR]

Generate Oracle schema and objects with Shift-JIS encoding

optional arguments:
  -h, --help                Show this help message and exit
  --tables TABLES           Number of tables to generate (max 8) [default: 6]
  --rows ROWS               Number of rows per table [default: 20]
  --output OUTPUT           Output SQL file name [default: oracle_export.sql]
  --encoding {shift-jis,utf-8}
                            Output file encoding [default: shift-jis]
  --objects OBJECTS         Comma-separated list of object types to generate
                            (table,data,trigger,procedure,function,package,lob,all)
                            [default: all]
  --no-header               Skip export header and footer
  --no-storage              Skip storage clauses in CREATE statements
  --split                   Split output into multiple files by object type
  --output-dir OUTPUT_DIR   Output directory for split files
```

### Examples

Generate a large schema with 1,000 rows per table:
```bash
python modular_OraSchemaGen.py --rows 1000 --output large_export.sql
```

Generate only tables and data, no PL/SQL objects:
```bash
python modular_OraSchemaGen.py --objects table,data --output simple_export.sql
```

Generate only tables with triggers and procedures:
```bash
python modular_OraSchemaGen.py --objects table,trigger,procedure
```

Generate a schema with separate files for each object type:
```bash
python modular_OraSchemaGen.py --split --output-dir ./export_files
```

Generate a UTF-8 encoded export instead of Shift-JIS:
```bash
python modular_OraSchemaGen.py --encoding utf-8
```

Create a clean schema without storage clauses:
```bash
python modular_OraSchemaGen.py --no-storage
```

## Generated Objects

The following object types can be generated:

### Tables
- EMPLOYEES - Employee information with Japanese name fields
- DEPARTMENTS - Department information
- JOBS - Job titles and salary ranges
- LOCATIONS - Location information
- PRODUCTS - Product details
- CUSTOMERS - Customer information with Japanese name fields
- ORDERS - Order header information
- ORDER_ITEMS - Order line items

### Advanced PL/SQL Objects
- **Triggers**: Complex compound triggers, before/after triggers, system event triggers
- **Procedures**: Employee management, department operations, data validation, test data generation
- **Functions**: Date handling, financial calculations, employee metrics, customer analysis
- **Packages**: Employee management, Japanese text utilities, data processing utilities
- **LOB Handling**: Document storage and retrieval, text extraction, thumbnail generation

## Architecture

OraSchemaGen employs a modular design with these key components:

1. **Generators**: Specialized classes for each object type (SchemaGenerator, TriggerGenerator, etc.)
2. **OracleObject**: Represents an individual Oracle database object
3. **OracleObjectFactory**: Creates appropriate generators based on requested object types
4. **OutputHandler**: Manages writing objects to files with proper dependency ordering
5. **EncodingHandler**: Handles character encoding conversion

## Customization

The code is designed to be easily extensible:

1. Add new object generators by extending the `OracleObjectGenerator` base class
2. Modify existing generators to add new object types
3. Expand the data generation logic to produce more specialized data

## Use Cases

- **Development Environments**: Create test Oracle schemas without needing a full Oracle installation
- **Migration Testing**: Test Oracle-to-PostgreSQL migrations with ora2pg
- **Education**: Learn Oracle database programming with realistic examples
- **Demos**: Generate sample data for application demos
- **Localization Testing**: Test applications with mixed English/Japanese data

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- The Faker library for realistic data generation
- The tqdm library for progress visualization
