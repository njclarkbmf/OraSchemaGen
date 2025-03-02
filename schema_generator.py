#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
schema_generator.py - Table and schema generation for OraSchemaGen

This module provides the SchemaGenerator class which creates Oracle tables,
constraints, indexes, and other schema objects.

Author: John Clark Naldoza
"""

from typing import List, Dict, Any
from core import OracleObjectGenerator, OracleObject, TableInfo

class SchemaGenerator(OracleObjectGenerator):
    """
    Generates Oracle schema objects (tables, constraints, etc.)
    """
    def __init__(self):
        super().__init__()
        self.tables: List[TableInfo] = []
        
    def generate(self, table_count: int = 6, include_storage: bool = True, **kwargs) -> List[OracleObject]:
        """Generate Oracle schema objects"""
        # Generate tables
        table_structures = self._generate_common_oracle_tables()
        table_count = min(table_count, len(table_structures))
        
        tables_to_generate = table_structures[:table_count]
        
        # Create TableInfo objects for each table
        for table_structure in tables_to_generate:
            table_name = table_structure[0]['table']
            self.tables.append(TableInfo(table_name, table_structure))
            
            # Create table object
            table_obj = OracleObject(table_name, "TABLE")
            table_obj.sql = self._generate_create_table(table_name, table_structure, include_storage)
            self.objects.append(table_obj)
            
        # Generate additional schema objects
        self._generate_indexes_and_constraints(include_storage)
        self._generate_sequences()
        self._generate_comments()
        
        return self.objects
        
    def _generate_create_table(self, table_name: str, columns: List[Dict[str, Any]], include_storage: bool = True) -> str:
        """Generate a CREATE TABLE statement"""
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
    
    def _generate_common_oracle_tables(self) -> List[List[Dict[str, Any]]]:
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
        
        # Orders table (additional table for relationships)
        orders = [
            {'table': 'ORDERS', 'name': 'ORDER_ID', 'type': 'NUMBER(12)', 'constraints': 'PRIMARY KEY'},
            {'table': 'ORDERS', 'name': 'CUSTOMER_ID', 'type': 'NUMBER(6)', 'constraints': 'NOT NULL'},
            {'table': 'ORDERS', 'name': 'STATUS', 'type': 'VARCHAR2(20)', 'constraints': 'NOT NULL'},
            {'table': 'ORDERS', 'name': 'SALESPERSON_ID', 'type': 'NUMBER(6)'},
            {'table': 'ORDERS', 'name': 'ORDER_DATE', 'type': 'DATE', 'constraints': 'NOT NULL'},
            {'table': 'ORDERS', 'name': 'SHIPPING_DATE', 'type': 'DATE'},
            {'table': 'ORDERS', 'name': 'SHIPPING_ADDRESS', 'type': 'VARCHAR2(255)'},
            {'table': 'ORDERS', 'name': 'SHIPPING_ADDRESS_JP', 'type': 'VARCHAR2(255)'},
            {'table': 'ORDERS', 'name': 'SHIPPING_CITY', 'type': 'VARCHAR2(30)'},
            {'table': 'ORDERS', 'name': 'SHIPPING_CITY_JP', 'type': 'VARCHAR2(30)'},
            {'table': 'ORDERS', 'name': 'SHIPPING_STATE', 'type': 'VARCHAR2(20)'},
            {'table': 'ORDERS', 'name': 'SHIPPING_ZIP', 'type': 'VARCHAR2(10)'},
            {'table': 'ORDERS', 'name': 'SHIPPING_COUNTRY', 'type': 'VARCHAR2(20)'},
            {'table': 'ORDERS', 'name': 'PAYMENT_METHOD', 'type': 'VARCHAR2(20)'},
            {'table': 'ORDERS', 'name': 'ORDER_TOTAL', 'type': 'NUMBER(10,2)'},
            {'table': 'ORDERS', 'name': 'NOTES', 'type': 'CLOB'},
            {'table': 'ORDERS', 'name': 'NOTES_JP', 'type': 'CLOB'}
        ]
        tables.append(orders)
        
        # Order Items table
        order_items = [
            {'table': 'ORDER_ITEMS', 'name': 'ORDER_ID', 'type': 'NUMBER(12)', 'constraints': 'NOT NULL'},
            {'table': 'ORDER_ITEMS', 'name': 'PRODUCT_ID', 'type': 'NUMBER(6)', 'constraints': 'NOT NULL'},
            {'table': 'ORDER_ITEMS', 'name': 'UNIT_PRICE', 'type': 'NUMBER(10,2)', 'constraints': 'NOT NULL'},
            {'table': 'ORDER_ITEMS', 'name': 'QUANTITY', 'type': 'NUMBER(8)', 'constraints': 'NOT NULL'},
            {'table': 'ORDER_ITEMS', 'name': 'DISCOUNT_PERCENT', 'type': 'NUMBER(4,2)'},
            {'table': 'ORDER_ITEMS', 'name': 'LINE_TOTAL', 'type': 'NUMBER(10,2)'},
            {'table': 'ORDER_ITEMS', 'name': 'NOTES', 'type': 'VARCHAR2(500)'},
            {'table': 'ORDER_ITEMS', 'name': 'NOTES_JP', 'type': 'VARCHAR2(500)'}
        ]
        tables.append(order_items)
        
        return tables
        
    def _generate_indexes_and_constraints(self, include_storage: bool = True) -> None:
        """Generate indexes and constraints for tables"""
        # Generate unique indexes for unique constraints
        for table_info in self.tables:
            table_name = table_info.name
            for col in table_info.columns:
                if 'constraints' in col and 'UNIQUE' in col['constraints']:
                    idx_name = f"{table_name}_{col['name']}_UK"
                    
                    # Create index object
                    index_obj = OracleObject(idx_name, "INDEX")
                    storage_clause = ""
                    if include_storage:
                        storage_clause = """
TABLESPACE USERS PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)"""
                    
                    index_obj.sql = f"""-- Unique Index for {col['name']} column
CREATE UNIQUE INDEX {idx_name} ON {table_name}({col['name']}){storage_clause};"""
                    index_obj.add_dependency(table_name)
                    self.objects.append(index_obj)
        
        # Generate foreign key constraints
        constraint_obj = OracleObject("FOREIGN_KEYS", "CONSTRAINT")
        constraint_obj.sql = """-- Foreign Key Constraints
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

ALTER TABLE ORDERS ADD CONSTRAINT ORD_CUST_FK
  FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS (CUSTOMER_ID)
  ENABLE VALIDATE;
  
ALTER TABLE ORDERS ADD CONSTRAINT ORD_EMP_FK
  FOREIGN KEY (SALESPERSON_ID) REFERENCES EMPLOYEES (EMPLOYEE_ID)
  ENABLE VALIDATE;
  
ALTER TABLE ORDER_ITEMS ADD CONSTRAINT ORDITM_ORD_FK
  FOREIGN KEY (ORDER_ID) REFERENCES ORDERS (ORDER_ID)
  ENABLE VALIDATE;
  
ALTER TABLE ORDER_ITEMS ADD CONSTRAINT ORDITM_PROD_FK
  FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCTS (PRODUCT_ID)
  ENABLE VALIDATE;"""
        constraint_obj.add_dependency("EMPLOYEES")
        constraint_obj.add_dependency("DEPARTMENTS")
        constraint_obj.add_dependency("JOBS")
        constraint_obj.add_dependency("LOCATIONS")
        constraint_obj.add_dependency("ORDERS")
        constraint_obj.add_dependency("ORDER_ITEMS")
        constraint_obj.add_dependency("PRODUCTS")
        constraint_obj.add_dependency("CUSTOMERS")
        self.objects.append(constraint_obj)
        
        # Add check constraints
        check_obj = OracleObject("CHECK_CONSTRAINTS", "CONSTRAINT")
        check_obj.sql = """-- Check Constraints
ALTER TABLE EMPLOYEES ADD CONSTRAINT EMP_SALARY_MIN
  CHECK (SALARY > 0) ENABLE VALIDATE;
  
ALTER TABLE JOBS ADD CONSTRAINT JOB_SALARY_RANGE
  CHECK (MIN_SALARY < MAX_SALARY) ENABLE VALIDATE;
  
ALTER TABLE ORDER_ITEMS ADD CONSTRAINT ORDITM_QTY_MIN
  CHECK (QUANTITY > 0) ENABLE VALIDATE;
  
ALTER TABLE PRODUCTS ADD CONSTRAINT PROD_PRICE_MIN
  CHECK (LIST_PRICE >= 0) ENABLE VALIDATE;"""
        check_obj.add_dependency("EMPLOYEES")
        check_obj.add_dependency("JOBS")
        check_obj.add_dependency("ORDER_ITEMS")
        check_obj.add_dependency("PRODUCTS")
        self.objects.append(check_obj)
        
    def _generate_sequences(self) -> None:
        """Generate sequences for tables"""
        seq_obj = OracleObject("SEQUENCES", "SEQUENCE")
        seq_obj.sql = """-- Sequences for primary key generation
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
  
CREATE SEQUENCE ORDERS_SEQ
  START WITH 10000
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;"""
        self.objects.append(seq_obj)
    
    def _generate_comments(self) -> None:
        """Generate comments for tables and columns"""
        comment_obj = OracleObject("COMMENTS", "COMMENT")
        comment_obj.sql = """-- Table and Column Comments
COMMENT ON TABLE EMPLOYEES IS 'Contains employee information including Japanese name fields';
COMMENT ON COLUMN EMPLOYEES.EMPLOYEE_ID IS 'Primary key of employees table';
COMMENT ON COLUMN EMPLOYEES.FIRST_NAME_JP IS 'First name in Japanese';
COMMENT ON COLUMN EMPLOYEES.LAST_NAME_JP IS 'Last name in Japanese';
COMMENT ON COLUMN EMPLOYEES.HIRE_DATE IS 'Date when the employee was hired';

COMMENT ON TABLE DEPARTMENTS IS 'Contains department information';
COMMENT ON COLUMN DEPARTMENTS.DEPARTMENT_NAME_JP IS 'Department name in Japanese';

COMMENT ON TABLE JOBS IS 'Contains job information including salary ranges';
COMMENT ON COLUMN JOBS.JOB_TITLE_JP IS 'Job title in Japanese';

COMMENT ON TABLE CUSTOMERS IS 'Customer information table';
COMMENT ON COLUMN CUSTOMERS.FIRST_NAME_JP IS 'Customer first name in Japanese';
COMMENT ON COLUMN CUSTOMERS.LAST_NAME_JP IS 'Customer last name in Japanese';

COMMENT ON TABLE ORDERS IS 'Order header information';
COMMENT ON COLUMN ORDERS.SHIPPING_ADDRESS_JP IS 'Shipping address in Japanese';

COMMENT ON TABLE ORDER_ITEMS IS 'Order line items';
COMMENT ON COLUMN ORDER_ITEMS.NOTES_JP IS 'Item notes in Japanese';"""
        comment_obj.add_dependency("EMPLOYEES")
        comment_obj.add_dependency("DEPARTMENTS")
        comment_obj.add_dependency("JOBS")
        comment_obj.add_dependency("CUSTOMERS")
        comment_obj.add_dependency("ORDERS")
        comment_obj.add_dependency("ORDER_ITEMS")
        self.objects.append(comment_obj)
    
    def get_tables(self) -> List[TableInfo]:
        """Get the list of tables"""
        return self.tables
