# Install required dependencies
pip install faker==19.13.0 tqdm==4.66.1

# Generate a basic Oracle SQL export with default settings (6 tables, 20 rows each, including PL/SQL)
python oracle-sql-generator.py

# Generate a larger export with more rows
python oracle-sql-generator.py --rows 100 --output large_export.sql

# Generate a smaller export with just 3 tables and 10 rows each, without PL/SQL
python oracle-sql-generator.py --tables 3 --rows 10 --output small_export.sql --no-plsql

# Generate an export without storage clauses (for cleaner output)
python oracle-sql-generator.py --no-storage

# Generate an export without headers and footers (just pure SQL)
python oracle-sql-generator.py --no-header

# Generate a massive export with 1 million rows (careful with file size!)
python oracle-sql-generator.py --rows 1000000 --output massive_export.sql


## in case the file can't be read..
iconv -f SHIFT_JIS -t UTF-8 large_dump.sql -o converted_large_dump.sql
