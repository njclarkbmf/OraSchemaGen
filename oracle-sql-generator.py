import random
import string
import datetime
import os
import argparse
import time
from faker import Faker
from tqdm import tqdm

# Create a Faker instance with Japanese locale for generating Japanese text
fake = Faker(['ja_JP'])

def generate_create_table(table_name, columns, include_storage=True):
    """Generate a CREATE TABLE statement in Oracle export style."""
    column_definitions = []
    constraints = []
    pk_columns = []
    
    # First pass to identify primary key and collect column definitions
    for col in columns:
        col_def = f"{col['name']} {col['type']}"
        if 'constraints' in col and col['constraints']:
            if 'NOT NULL' in col['constraints']:
                col_def += " NOT NULL"
            if 'PRIMARY KEY' in col['constraints']:
                pk_columns.append(col['name'])
        column_definitions.append(col_def)
    
    # Add primary key constraint if any
    if pk_columns:
        pk_constraint = f"CONSTRAINT {table_name}_PK PRIMARY KEY ({', '.join(pk_columns)})"
        if include_storage:
            pk_constraint += f"\n  USING INDEX TABLESPACE USERS PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS \n  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645\n  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1\n  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)"
        constraints.append(pk_constraint)
    
    # Build the complete CREATE TABLE statement
    create_stmt = f"CREATE TABLE {table_name} \n(\n  " + ",\n  ".join(column_definitions)
    
    # Add constraint definitions
    if constraints:
        create_stmt += ",\n  " + ",\n  ".join(constraints)
    
    # Add storage parameters if requested
    if include_storage:
        create_stmt += f"\n)\nTABLESPACE USERS PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 \nNOLOGGING STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645\nPCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1\nBUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)"
    else:
        create_stmt += "\n)"
    
    create_stmt += ";"
    return create_stmt

def generate_insert_statement(table_name, column_names, values):
    """Generate an INSERT statement in Oracle export style."""
    columns_str = ', '.join(column_names)
    values_str = ', '.join(values)
    return f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"

def format_value(value, data_type):
    """Format a value according to its data type."""
    if value is None:
        return "NULL"
    elif "VARCHAR" in data_type or "CHAR" in data_type or data_type == "CLOB":
        # Escape single quotes
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"
    elif "DATE" in data_type:
        return f"TO_DATE('{value}', 'YYYY-MM-DD')"
    elif "TIMESTAMP" in data_type:
        return f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS')"
    else:
        return str(value)

def generate_sample_data(table_structure, num_rows):
    """Generate sample data for a table based on its structure."""
    inserts = []
    
    for _ in range(num_rows):
        column_names = []
        values = []
        
        for column in table_structure:
            column_names.append(column['name'])
            data_type = column['type'].upper()
            
            # Generate appropriate value based on data type
            if "VARCHAR" in data_type or "CHAR" in data_type:
                if column['name'].lower().endswith('_jp') or column['name'].lower().endswith('_japanese'):
                    # Generate Japanese text
                    if 'NAME' in column['name'].upper():
                        value = fake.name()
                    elif 'ADDRESS' in column['name'].upper():
                        value = fake.address()
                    elif 'CITY' in column['name'].upper():
                        value = fake.city()
                    elif 'PROVINCE' in column['name'].upper() or 'STATE' in column['name'].upper():
                        value = fake.prefecture()
                    elif 'COUNTRY' in column['name'].upper():
                        value = "日本"
                    elif 'TITLE' in column['name'].upper():
                        value = fake.job()
                    else:
                        # Get max length from the data type
                        max_length = int(''.join(filter(str.isdigit, data_type))) if any(c.isdigit() for c in data_type) else 10
                        value = fake.text(max_nb_chars=min(max_length, 20))
                else:
                    # Generate English text
                    if 'NAME' in column['name'].upper() and 'FIRST' in column['name'].upper():
                        value = fake.first_name_male() if random.choice([True, False]) else fake.first_name_female()
                    elif 'NAME' in column['name'].upper() and 'LAST' in column['name'].upper():
                        value = fake.last_name()
                    elif 'EMAIL' in column['name'].upper():
                        value = fake.email()
                    elif 'PHONE' in column['name'].upper():
                        value = fake.phone_number()
                    elif 'ADDRESS' in column['name'].upper():
                        value = fake.street_address()
                    elif 'CITY' in column['name'].upper():
                        value = fake.city()
                    elif 'STATE' in column['name'].upper() or 'PROVINCE' in column['name'].upper():
                        # Using fallback since state() might not be available in all locales
                        try:
                            value = fake.state()
                        except AttributeError:
                            value = fake.city()  # Fallback to city if state is not available
                    elif 'POSTAL' in column['name'].upper():
                        value = fake.postcode()
                    elif 'COUNTRY' in column['name'].upper():
                        value = fake.country()
                    else:
                        max_length = int(''.join(filter(str.isdigit, data_type))) if any(c.isdigit() for c in data_type) else 10
                        value = fake.text(max_nb_chars=min(max_length, 20))
            elif data_type == "NUMBER" or data_type.startswith("NUMBER("):
                if ',' in data_type:  # Has decimal places
                    value = round(random.uniform(0, 10000), 2)
                else:
                    digits = ''.join(filter(str.isdigit, data_type))
                    max_val = 10 ** (int(digits) if digits else 6) - 1
                    value = random.randint(1, min(max_val, 1000000))
            elif data_type == "DATE":
                value = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
            elif "TIMESTAMP" in data_type:
                value = fake.date_time_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
            elif data_type == "CLOB":
                if column['name'].lower().endswith('_jp') or column['name'].lower().endswith('_japanese'):
                    value = '\n'.join([fake.paragraph() for _ in range(2)])
                else:
                    value = '\n'.join([fake.paragraph() for _ in range(2)])
            elif data_type == "BLOB":
                value = "EMPTY_BLOB()"  # Use Oracle's empty BLOB function
            else:
                value = "NULL"  # Default for unsupported types
                
            values.append(format_value(value, data_type))
        
        inserts.append(generate_insert_statement(table_structure[0]['table'], column_names, values))
    
    return inserts

def generate_common_oracle_tables():
    """Generate common Oracle database table structures."""
    tables = []
    
    # Employees table
    employees = [
        {'table': 'EMPLOYEES', 'name': 'EMPLOYEE_ID', 'type': 'NUMBER(6)', 'constraints': 'PRIMARY KEY'},
        {'table': 'EMPLOYEES', 'name': 'FIRST_NAME', 'type': 'VARCHAR2(20)'},
        {'table': 'EMPLOYEES', 'name': 'LAST_NAME', 'type': 'VARCHAR2(25)', 'constraints': 'NOT NULL'},
        {'table': 'EMPLOYEES', 'name': 'FIRST_NAME_JP', 'type': 'VARCHAR2(20)'},
        {'table': 'EMPLOYEES', 'name': 'LAST_NAME_JP', 'type': 'VARCHAR2(25)'},
        {'table': 'EMPLOYEES', 'name': 'EMAIL', 'type': 'VARCHAR2(25)', 'constraints': 'UNIQUE'},
        {'table': 'EMPLOYEES', 'name': 'PHONE_NUMBER', 'type': 'VARCHAR2(20)'},
        {'table': 'EMPLOYEES', 'name': 'HIRE_DATE', 'type': 'DATE', 'constraints': 'NOT NULL'},
        {'table': 'EMPLOYEES', 'name': 'JOB_ID', 'type': 'VARCHAR2(10)', 'constraints': 'NOT NULL'},
        {'table': 'EMPLOYEES', 'name': 'SALARY', 'type': 'NUMBER(8,2)'},
        {'table': 'EMPLOYEES', 'name': 'COMMISSION_PCT', 'type': 'NUMBER(2,2)'},
        {'table': 'EMPLOYEES', 'name': 'MANAGER_ID', 'type': 'NUMBER(6)'},
        {'table': 'EMPLOYEES', 'name': 'DEPARTMENT_ID', 'type': 'NUMBER(4)'},
        {'table': 'EMPLOYEES', 'name': 'NOTES_JP', 'type': 'CLOB'}
    ]
    tables.append(employees)
    
    # Departments table
    departments = [
        {'table': 'DEPARTMENTS', 'name': 'DEPARTMENT_ID', 'type': 'NUMBER(4)', 'constraints': 'PRIMARY KEY'},
        {'table': 'DEPARTMENTS', 'name': 'DEPARTMENT_NAME', 'type': 'VARCHAR2(30)', 'constraints': 'NOT NULL'},
        {'table': 'DEPARTMENTS', 'name': 'DEPARTMENT_NAME_JP', 'type': 'VARCHAR2(30)'},
        {'table': 'DEPARTMENTS', 'name': 'MANAGER_ID', 'type': 'NUMBER(6)'},
        {'table': 'DEPARTMENTS', 'name': 'LOCATION_ID', 'type': 'NUMBER(4)'},
        {'table': 'DEPARTMENTS', 'name': 'DESCRIPTION_JP', 'type': 'CLOB'}
    ]
    tables.append(departments)
    
    # Jobs table
    jobs = [
        {'table': 'JOBS', 'name': 'JOB_ID', 'type': 'VARCHAR2(10)', 'constraints': 'PRIMARY KEY'},
        {'table': 'JOBS', 'name': 'JOB_TITLE', 'type': 'VARCHAR2(35)', 'constraints': 'NOT NULL'},
        {'table': 'JOBS', 'name': 'JOB_TITLE_JP', 'type': 'VARCHAR2(35)'},
        {'table': 'JOBS', 'name': 'MIN_SALARY', 'type': 'NUMBER(6)'},
        {'table': 'JOBS', 'name': 'MAX_SALARY', 'type': 'NUMBER(6)'},
        {'table': 'JOBS', 'name': 'JOB_DESCRIPTION', 'type': 'CLOB'},
        {'table': 'JOBS', 'name': 'JOB_DESCRIPTION_JP', 'type': 'CLOB'}
    ]
    tables.append(jobs)
    
    # Locations table
    locations = [
        {'table': 'LOCATIONS', 'name': 'LOCATION_ID', 'type': 'NUMBER(4)', 'constraints': 'PRIMARY KEY'},
        {'table': 'LOCATIONS', 'name': 'STREET_ADDRESS', 'type': 'VARCHAR2(40)'},
        {'table': 'LOCATIONS', 'name': 'STREET_ADDRESS_JP', 'type': 'VARCHAR2(40)'},
        {'table': 'LOCATIONS', 'name': 'POSTAL_CODE', 'type': 'VARCHAR2(12)'},
        {'table': 'LOCATIONS', 'name': 'CITY', 'type': 'VARCHAR2(30)', 'constraints': 'NOT NULL'},
        {'table': 'LOCATIONS', 'name': 'CITY_JP', 'type': 'VARCHAR2(30)'},
        {'table': 'LOCATIONS', 'name': 'STATE_PROVINCE', 'type': 'VARCHAR2(25)'},
        {'table': 'LOCATIONS', 'name': 'STATE_PROVINCE_JP', 'type': 'VARCHAR2(25)'},
        {'table': 'LOCATIONS', 'name': 'COUNTRY_ID', 'type': 'CHAR(2)'}
    ]
    tables.append(locations)
    
    # Products table
    products = [
        {'table': 'PRODUCTS', 'name': 'PRODUCT_ID', 'type': 'NUMBER(6)', 'constraints': 'PRIMARY KEY'},
        {'table': 'PRODUCTS', 'name': 'PRODUCT_NAME', 'type': 'VARCHAR2(50)', 'constraints': 'NOT NULL'},
        {'table': 'PRODUCTS', 'name': 'PRODUCT_NAME_JP', 'type': 'VARCHAR2(50)'},
        {'table': 'PRODUCTS', 'name': 'DESCRIPTION', 'type': 'VARCHAR2(2000)'},
        {'table': 'PRODUCTS', 'name': 'DESCRIPTION_JP', 'type': 'VARCHAR2(2000)'},
        {'table': 'PRODUCTS', 'name': 'CATEGORY_ID', 'type': 'NUMBER(4)'},
        {'table': 'PRODUCTS', 'name': 'STANDARD_COST', 'type': 'NUMBER(9,2)'},
        {'table': 'PRODUCTS', 'name': 'LIST_PRICE', 'type': 'NUMBER(9,2)'},
        {'table': 'PRODUCTS', 'name': 'CREATED_DATE', 'type': 'DATE'},
        {'table': 'PRODUCTS', 'name': 'MODIFIED_DATE', 'type': 'DATE'}
    ]
    tables.append(products)
    
    # Customers table
    customers = [
        {'table': 'CUSTOMERS', 'name': 'CUSTOMER_ID', 'type': 'NUMBER(6)', 'constraints': 'PRIMARY KEY'},
        {'table': 'CUSTOMERS', 'name': 'FIRST_NAME', 'type': 'VARCHAR2(20)'},
        {'table': 'CUSTOMERS', 'name': 'LAST_NAME', 'type': 'VARCHAR2(25)', 'constraints': 'NOT NULL'},
        {'table': 'CUSTOMERS', 'name': 'FIRST_NAME_JP', 'type': 'VARCHAR2(20)'},
        {'table': 'CUSTOMERS', 'name': 'LAST_NAME_JP', 'type': 'VARCHAR2(25)'},
        {'table': 'CUSTOMERS', 'name': 'EMAIL', 'type': 'VARCHAR2(50)', 'constraints': 'UNIQUE'},
        {'table': 'CUSTOMERS', 'name': 'PHONE', 'type': 'VARCHAR2(20)'},
        {'table': 'CUSTOMERS', 'name': 'ADDRESS', 'type': 'VARCHAR2(100)'},
        {'table': 'CUSTOMERS', 'name': 'ADDRESS_JP', 'type': 'VARCHAR2(100)'},
        {'table': 'CUSTOMERS', 'name': 'CITY', 'type': 'VARCHAR2(30)'},
        {'table': 'CUSTOMERS', 'name': 'CITY_JP', 'type': 'VARCHAR2(30)'},
        {'table': 'CUSTOMERS', 'name': 'STATE', 'type': 'VARCHAR2(20)'},
        {'table': 'CUSTOMERS', 'name': 'STATE_JP', 'type': 'VARCHAR2(20)'},
        {'table': 'CUSTOMERS', 'name': 'POSTAL_CODE', 'type': 'VARCHAR2(10)'},
        {'table': 'CUSTOMERS', 'name': 'COUNTRY', 'type': 'VARCHAR2(20)'},
        {'table': 'CUSTOMERS', 'name': 'COUNTRY_JP', 'type': 'VARCHAR2(20)'},
        {'table': 'CUSTOMERS', 'name': 'CREDIT_LIMIT', 'type': 'NUMBER(9,2)'},
        {'table': 'CUSTOMERS', 'name': 'REGISTRATION_DATE', 'type': 'DATE'}
    ]
    tables.append(customers)
    
    return tables

def generate_indexes_and_constraints(tables):
    """Generate additional indexes and constraints for tables."""
    statements = []
    
    # Generate unique indexes for unique constraints
    for table_structure in tables:
        table_name = table_structure[0]['table']
        for col in table_structure:
            if 'constraints' in col and 'UNIQUE' in col['constraints']:
                idx_name = f"{table_name}_{col['name']}_UK"
                stmt = f"""
-- Unique Index for {col['name']} column
CREATE UNIQUE INDEX {idx_name} ON {table_name}({col['name']})
TABLESPACE USERS PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT);
"""
                statements.append(stmt)
    
    # Generate foreign key constraints
    statements.append("""
-- Foreign Key Constraints
ALTER TABLE EMPLOYEES ADD CONSTRAINT EMP_DEPT_FK
  FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS (DEPARTMENT_ID)
  ENABLE VALIDATE;
  
ALTER TABLE EMPLOYEES ADD CONSTRAINT EMP_JOB_FK
  FOREIGN KEY (JOB_ID) REFERENCES JOBS (JOB_ID)
  ENABLE VALIDATE;

ALTER TABLE EMPLOYEES ADD CONSTRAINT EMP_MANAGER_FK
  FOREIGN KEY (MANAGER_ID) REFERENCES EMPLOYEES (EMPLOYEE_ID)
  ENABLE VALIDATE;
  
ALTER TABLE DEPARTMENTS ADD CONSTRAINT DEPT_MGR_FK
  FOREIGN KEY (MANAGER_ID) REFERENCES EMPLOYEES (EMPLOYEE_ID)
  ENABLE VALIDATE;
  
ALTER TABLE DEPARTMENTS ADD CONSTRAINT DEPT_LOC_FK
  FOREIGN KEY (LOCATION_ID) REFERENCES LOCATIONS (LOCATION_ID)
  ENABLE VALIDATE;
""")
    
    # Add check constraints
    statements.append("""
-- Check Constraints
ALTER TABLE EMPLOYEES ADD CONSTRAINT EMP_SALARY_MIN
  CHECK (SALARY > 0) ENABLE VALIDATE;
  
ALTER TABLE JOBS ADD CONSTRAINT JOB_SALARY_RANGE
  CHECK (MIN_SALARY < MAX_SALARY) ENABLE VALIDATE;
""")
    
    # Add comments on tables and columns
    statements.append("""
-- Table and Column Comments
COMMENT ON TABLE EMPLOYEES IS 'Contains employee information including Japanese name fields';
COMMENT ON COLUMN EMPLOYEES.EMPLOYEE_ID IS 'Primary key of employees table';
COMMENT ON COLUMN EMPLOYEES.FIRST_NAME_JP IS 'First name in Japanese';
COMMENT ON COLUMN EMPLOYEES.LAST_NAME_JP IS 'Last name in Japanese';
COMMENT ON COLUMN EMPLOYEES.HIRE_DATE IS 'Date when the employee was hired';

COMMENT ON TABLE DEPARTMENTS IS 'Contains department information';
COMMENT ON COLUMN DEPARTMENTS.DEPARTMENT_NAME_JP IS 'Department name in Japanese';

COMMENT ON TABLE JOBS IS 'Contains job information including salary ranges';
COMMENT ON COLUMN JOBS.JOB_TITLE_JP IS 'Job title in Japanese';
""")
    
    # Add sequences
    statements.append("""
-- Sequences for primary key generation
CREATE SEQUENCE EMPLOYEES_SEQ
  START WITH 1000
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;

CREATE SEQUENCE DEPARTMENTS_SEQ
  START WITH 100
  INCREMENT BY 10
  NOCACHE
  NOCYCLE;

CREATE SEQUENCE LOCATIONS_SEQ
  START WITH 1000
  INCREMENT BY 100
  NOCACHE
  NOCYCLE;

CREATE SEQUENCE PRODUCTS_SEQ
  START WITH 10000
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;

CREATE SEQUENCE CUSTOMERS_SEQ
  START WITH 1
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;
""")
    
    return statements

def generate_plsql_objects():
    """Generate basic PL/SQL objects in Oracle export style."""
    plsql_code = []
    
    # 1. Simple procedure
    plsql_code.append("""
CREATE OR REPLACE PROCEDURE add_employee (
  p_first_name IN VARCHAR2,
  p_last_name IN VARCHAR2,
  p_email IN VARCHAR2,
  p_job_id IN VARCHAR2,
  p_department_id IN NUMBER)
AS
  v_employee_id NUMBER;
BEGIN
  SELECT employees_seq.NEXTVAL INTO v_employee_id FROM dual;
  
  INSERT INTO employees (
    employee_id, first_name, last_name, email, 
    phone_number, hire_date, job_id, 
    department_id)
  VALUES (
    v_employee_id, p_first_name, p_last_name, p_email,
    NULL, SYSDATE, p_job_id, p_department_id);
    
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END add_employee;
/
""")
    
    # 2. Simple function
    plsql_code.append("""
CREATE OR REPLACE FUNCTION get_employee_count (
  p_department_id IN NUMBER)
RETURN NUMBER
AS
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM employees
  WHERE department_id = p_department_id;
  
  RETURN v_count;
EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
END get_employee_count;
/
""")
    
    # 3. Simple trigger
    plsql_code.append("""
CREATE OR REPLACE TRIGGER employees_bi_trg
BEFORE INSERT ON employees
FOR EACH ROW
BEGIN
  IF :NEW.hire_date IS NULL THEN
    :NEW.hire_date := SYSDATE;
  END IF;
END;
/
""")
    
    # 4. Package specification
    plsql_code.append("""
CREATE OR REPLACE PACKAGE emp_pkg AS
  
  FUNCTION get_salary(p_emp_id IN NUMBER) RETURN NUMBER;
  
  PROCEDURE update_salary(
    p_emp_id IN NUMBER,
    p_salary IN NUMBER);
    
END emp_pkg;
/
""")
    
    # 5. Package body
    plsql_code.append("""
CREATE OR REPLACE PACKAGE BODY emp_pkg AS

  FUNCTION get_salary(p_emp_id IN NUMBER) RETURN NUMBER IS
    v_salary employees.salary%TYPE;
  BEGIN
    SELECT salary INTO v_salary
    FROM employees
    WHERE employee_id = p_emp_id;
    
    RETURN v_salary;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN NULL;
  END get_salary;
  
  PROCEDURE update_salary(
    p_emp_id IN NUMBER,
    p_salary IN NUMBER) IS
  BEGIN
    UPDATE employees
    SET salary = p_salary
    WHERE employee_id = p_emp_id;
    
    IF SQL%ROWCOUNT = 0 THEN
      RAISE_APPLICATION_ERROR(-20001, 'Employee not found');
    END IF;
    
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE;
  END update_salary;
  
END emp_pkg;
/
""")

    # 6. Japanese name utility function
    plsql_code.append("""
CREATE OR REPLACE FUNCTION format_jp_name(
  p_first_name_jp IN VARCHAR2,
  p_last_name_jp IN VARCHAR2)
RETURN VARCHAR2
AS
BEGIN
  -- In Japanese format, family name comes before given name
  RETURN p_last_name_jp || ' ' || p_first_name_jp;
END format_jp_name;
/
""")
    
    return plsql_code

def generate_sql_file(output_file, tables, rows_per_table=10, include_plsql=True, include_header=True):
    """Generate SQL file in Oracle export style format."""
    total_tables = len(tables)
    total_operations = total_tables + total_tables  # CREATE + INSERT operations
    
    if include_plsql:
        total_operations += 1
        
    print(f"Starting SQL generation process...")
    print(f"- Writing to: {output_file}")
    print(f"- Tables: {total_tables}")
    print(f"- Rows per table: {rows_per_table}")
    print(f"- Total rows: {total_tables * rows_per_table}")
    print(f"- PL/SQL objects: {'Yes' if include_plsql else 'No'}")
    
    # Estimate file size (rough estimate)
    estimated_row_size = 500  # bytes per row (average)
    estimated_file_size_mb = (total_tables * rows_per_table * estimated_row_size) / (1024 * 1024)
    print(f"- Estimated file size: {estimated_file_size_mb:.1f} MB")
    
    # Start a progress bar for overall progress
    with tqdm(total=total_operations, desc="Overall Progress", position=0) as overall_pbar:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write file header
            if include_header:
                timestamp = datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")
                f.write(f"""-- Export dump file
-- Version: Oracle Database 19c Enterprise Edition Release 19.0.0.0.0
-- Export Timestamp: {timestamp}
-- Character Set: SHIFT-JIS

-- Export Parameters:
--   userid=system
--   schemas=SAMPLE_SCHEMA
--   dumpfile=export.dmp
--   logfile=export_log.log
--   full=N
--   consistent=Y

-- Export Session Information:
--   username: SYSTEM
--   export instance: ORCL

-- Note: This SQL script was generated with a custom tool
-- Connected to: Oracle Database 19c Enterprise Edition Release 19.0.0.0.0
-- Export started at: {timestamp}

-- Disable Foreign Key Constraints before Data Load
BEGIN
   FOR c IN (SELECT table_name, constraint_name 
             FROM user_constraints 
             WHERE constraint_type = 'R'
             AND status = 'ENABLED') LOOP
      EXECUTE IMMEDIATE 'ALTER TABLE ' || c.table_name || 
                        ' DISABLE CONSTRAINT ' || c.constraint_name;
   END LOOP;
END;
/

""")
            
            # Write DROP TABLE statements (in reverse order to handle foreign keys)
            f.write("-- Drop existing tables\n")
            for table_structure in reversed(tables):
                f.write(f"DROP TABLE {table_structure[0]['table']} CASCADE CONSTRAINTS;\n")
            f.write("\n")
            
            # Write CREATE TABLE statements
            f.write("-- Create tables\n")
            for table_structure in tables:
                table_name = table_structure[0]['table']
                f.write(generate_create_table(table_name, table_structure) + "\n\n")
                overall_pbar.update(1)  # Update progress for each CREATE TABLE
            
            # Generate and write indexes and constraints
            f.write("-- Create indexes, constraints, comments and sequences\n")
            index_statements = generate_indexes_and_constraints(tables)
            for stmt in index_statements:
                f.write(stmt + "\n")
            
            # Set a reasonable batch size to avoid memory issues
            if rows_per_table > 100000:
                batch_size = 10000
            elif rows_per_table > 10000:
                batch_size = 5000
            else:
                batch_size = 1000
            
            # Write INSERT statements for sample data
            f.write("-- Load table data\n")
            for table_structure in tables:
                table_name = table_structure[0]['table']
                f.write(f"-- Data for {table_name}\n")
                
                # Create a nested progress bar for this table
                table_desc = f"Table: {table_name}"
                remaining_rows = rows_per_table
                
                with tqdm(total=rows_per_table, desc=table_desc, position=1, leave=False) as table_pbar:
                    while remaining_rows > 0:
                        current_batch = min(batch_size, remaining_rows)
                        inserts = generate_sample_data(table_structure, current_batch)
                        
                        for insert in inserts:
                            f.write(insert + "\n")
                        
                        # Add intermediate commits for large datasets
                        if rows_per_table > batch_size:
                            f.write("COMMIT;\n")
                        
                        remaining_rows -= current_batch
                        table_pbar.update(current_batch)
                
                f.write("\n")
                overall_pbar.update(1)  # Update progress for each completed table's INSERT statements
            
            # Add final commit
            f.write("\n-- Commit to finalize all transactions\n")
            f.write("COMMIT;\n\n")
            
            # Re-enable foreign key constraints
            f.write("""-- Re-enable Foreign Key Constraints after Data Load
BEGIN
   FOR c IN (SELECT table_name, constraint_name 
             FROM user_constraints 
             WHERE constraint_type = 'R'
             AND status = 'DISABLED') LOOP
      EXECUTE IMMEDIATE 'ALTER TABLE ' || c.table_name || 
                        ' ENABLE CONSTRAINT ' || c.constraint_name;
   END LOOP;
END;
/

""")
            
            # Add PL/SQL objects if requested
            if include_plsql:
                f.write("-- Create PL/SQL objects\n")
                
                # Generate PL/SQL objects
                plsql_objects = generate_plsql_objects()
                
                with tqdm(total=len(plsql_objects), desc="Generating PL/SQL", position=1, leave=False) as plsql_pbar:
                    for plsql_obj in plsql_objects:
                        f.write(plsql_obj)
                        plsql_pbar.update(1)
                
                overall_pbar.update(1)  # Update overall progress for PL/SQL objects
                
            # Add export completion footer
            if include_header:
                f.write(f"""
-- Export completed successfully
-- Export Timestamp: {datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")}
-- Total tables exported: {total_tables}
-- Total rows exported: {total_tables * rows_per_table}
-- Total PL/SQL objects exported: {len(generate_plsql_objects()) if include_plsql else 0}
""")

def convert_to_shift_jis(input_file, output_file):
    """Convert UTF-8 file to Shift-JIS encoding with progress updates."""
    print(f"Converting file to Shift-JIS encoding...")
    
    # Get file size for progress reporting
    file_size = os.path.getsize(input_file)
    print(f"- File size: {file_size / (1024 * 1024):.1f} MB")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Try to convert content to Shift-JIS
    try:
        with open(output_file, 'w', encoding='shift_jis') as f:
            f.write(content)
        print(f"✓ Successfully converted to Shift-JIS: {output_file}")
    except UnicodeEncodeError as e:
        print(f"⚠ Warning: Some characters couldn't be encoded in Shift-JIS. Error: {e}")
        print("  Attempting to replace problematic characters and continue...")
        
        # Fallback with character replacement
        with open(output_file, 'w', encoding='shift_jis', errors='replace') as f:
            f.write(content)
        print(f"✓ Converted to Shift-JIS with character replacements: {output_file}")

def main():
    # Add a banner
    print("=" * 80)
    print(" Oracle SQL Schema Export Generator (with Shift-JIS encoding)")
    print("=" * 80)
    
    parser = argparse.ArgumentParser(description='Generate Oracle SQL schema export with Shift-JIS encoding')
    parser.add_argument('--tables', type=int, default=6, help='Number of tables to generate (max 6)')
    parser.add_argument('--rows', type=int, default=20, help='Number of rows per table')
    parser.add_argument('--output', default='oracle_export.sql', help='Output SQL file name')
    parser.add_argument('--no-plsql', action='store_true', help='Skip PL/SQL object generation')
    parser.add_argument('--no-header', action='store_true', help='Skip export header and footer')
    parser.add_argument('--no-storage', action='store_true', help='Skip storage clauses in CREATE statements')
    args = parser.parse_args()
    
    # Validate inputs
    table_count = min(args.tables, 6)  # Cap at 6 tables
    row_count = args.rows  # No artificial limit, allow any number of rows
    include_plsql = not args.no_plsql  # Include PL/SQL by default unless --no-plsql flag is used
    include_header = not args.no_header  # Include header by default
    
    # Show start time
    start_time = time.time()
    print(f"Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get table structures
    all_tables = generate_common_oracle_tables()
    tables_to_generate = all_tables[:table_count]
    
    # Generate temporary UTF-8 file
    temp_file = args.output + ".temp"
    generate_sql_file(temp_file, tables_to_generate, row_count, include_plsql, include_header)
    
    # Convert to Shift-JIS
    convert_to_shift_jis(temp_file, args.output)
    
    # Clean up temp file
    os.remove(temp_file)
    
    # Show summary
    elapsed_time = time.time() - start_time
    print("\nGeneration Summary:")
    print(f"- SQL export generated successfully: {args.output}")
    print(f"- Total tables: {table_count}")
    print(f"- Total rows: {table_count * row_count}")
    print(f"- PL/SQL objects: {len(generate_plsql_objects()) if include_plsql else 0}")
    print(f"- Time taken: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
    
    # Show file size
    file_size = os.path.getsize(args.output) / (1024 * 1024)  # Size in MB
    print(f"- Final file size: {file_size:.2f} MB")
    print("\nComplete!")

if __name__ == "__main__":
    main()