#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data_generator.py - Test data generation for OraSchemaGen

This module provides the DataGenerator class which creates realistic
sample data for Oracle tables.

Author: John Clark Naldoza
"""

import random
from typing import List, Dict, Any
from core import OracleObjectGenerator, OracleObject, TableInfo
from faker import Faker

class DataGenerator(OracleObjectGenerator):
    """
    Generates sample data for Oracle tables
    """
    def __init__(self):
        super().__init__()
        # Create separate Faker instances for different locales
        self.fake_en = Faker('en_US')
        self.fake_jp = Faker('ja_JP')
        
    def generate(self, tables: List[TableInfo], rows_per_table: int = 10, **kwargs) -> List[OracleObject]:
        """Generate sample data for tables"""
        for table_info in tables:
            table_name = table_info.name
            data_obj = OracleObject(f"{table_name}_DATA", "DATA")
            
            # Generate insert statements
            inserts = self._generate_sample_data(table_info.columns, rows_per_table)
            data_obj.sql = f"-- Data for {table_name}\n" + "\n".join(inserts)
            data_obj.add_dependency(table_name)
            
            self.objects.append(data_obj)
            
        return self.objects
    
    def _generate_sample_data(self, table_structure: List[Dict[str, Any]], num_rows: int) -> List[str]:
        """Generate sample data for a table based on its structure."""
        inserts = []
        table_name = table_structure[0]['table']
        
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
                            value = self.fake_jp.name()
                        elif 'ADDRESS' in column['name'].upper():
                            value = self.fake_jp.address()
                        elif 'CITY' in column['name'].upper():
                            value = self.fake_jp.city()
                        elif 'PROVINCE' in column['name'].upper() or 'STATE' in column['name'].upper():
                            value = self.fake_jp.prefecture()
                        elif 'COUNTRY' in column['name'].upper():
                            value = "日本"
                        elif 'TITLE' in column['name'].upper():
                            value = self.fake_jp.job()
                        else:
                            # Get max length from the data type
                            max_length = int(''.join(filter(str.isdigit, data_type))) if any(c.isdigit() for c in data_type) else 10
                            value = self.fake_jp.text(max_nb_chars=min(max_length, 20))
                    else:
                        # Generate English text
                        if 'NAME' in column['name'].upper() and 'FIRST' in column['name'].upper():
                            value = self.fake_en.first_name_male() if random.choice([True, False]) else self.fake_en.first_name_female()
                        elif 'NAME' in column['name'].upper() and 'LAST' in column['name'].upper():
                            value = self.fake_en.last_name()
                        elif 'EMAIL' in column['name'].upper():
                            value = self.fake_en.email()
                        elif 'PHONE' in column['name'].upper():
                            value = self.fake_en.phone_number()
                        elif 'ADDRESS' in column['name'].upper():
                            value = self.fake_en.street_address()
                        elif 'CITY' in column['name'].upper():
                            value = self.fake_en.city()
                        elif 'STATE' in column['name'].upper() or 'PROVINCE' in column['name'].upper():
                            # Using fallback since state() might not be available in all locales
                            try:
                                value = self.fake_en.state()
                            except AttributeError:
                                value = self.fake_en.city()  # Fallback to city if state is not available
                        elif 'POSTAL' in column['name'].upper() or 'ZIP' in column['name'].upper():
                            value = self.fake_en.postcode()
                        elif 'COUNTRY' in column['name'].upper():
                            value = self.fake_en.country()
                        elif 'STATUS' in column['name'].upper():
                            value = random.choice(['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'])
                        elif 'PAYMENT' in column['name'].upper() and 'METHOD' in column['name'].upper():
                            value = random.choice(['CREDIT', 'DEBIT', 'BANK_TRANSFER', 'PAYPAL', 'CASH'])
                        else:
                            max_length = int(''.join(filter(str.isdigit, data_type))) if any(c.isdigit() for c in data_type) else 10
                            value = self.fake_en.text(max_nb_chars=min(max_length, 20))
                elif data_type == "NUMBER" or data_type.startswith("NUMBER("):
                    if column['name'] == 'EMPLOYEE_ID':
                        value = f"EMPLOYEES_SEQ.NEXTVAL"
                    elif column['name'] == 'DEPARTMENT_ID' and table_name != 'DEPARTMENTS':
                        value = str(random.randint(10, 90) * 10)  # 100, 200, etc.
                    elif column['name'] == 'DEPARTMENT_ID' and table_name == 'DEPARTMENTS':
                        value = f"DEPARTMENTS_SEQ.NEXTVAL"
                    elif column['name'] == 'LOCATION_ID':
                        value = str(random.randint(1, 9) * 1000 + random.randint(1, 999))
                    elif column['name'] == 'PRODUCT_ID':
                        value = f"PRODUCTS_SEQ.NEXTVAL"
                    elif column['name'] == 'CUSTOMER_ID':
                        value = f"CUSTOMERS_SEQ.NEXTVAL"
                    elif column['name'] == 'ORDER_ID':
                        value = f"ORDERS_SEQ.NEXTVAL"
                    elif column['name'] == 'MANAGER_ID':
                        value = str(random.randint(100, 999))
                    elif ',' in data_type:  # Has decimal places
                        if 'PRICE' in column['name'].upper() or 'COST' in column['name'].upper() or 'TOTAL' in column['name'].upper():
                            value = str(round(random.uniform(10, 1000), 2))
                        elif 'PCT' in column['name'].upper() or 'PERCENT' in column['name'].upper():
                            value = str(round(random.uniform(0, 0.5), 2))
                        else:
                            value = str(round(random.uniform(0, 10000), 2))
                    else:
                        if 'SALARY' in column['name'].upper():
                            value = str(random.randint(3000, 20000))
                        elif 'QUANTITY' in column['name'].upper():
                            value = str(random.randint(1, 100))
                        else:
                            digits = ''.join(filter(str.isdigit, data_type))
                            max_val = 10 ** (int(digits) if digits else 6) - 1
                            value = str(random.randint(1, min(max_val, 1000000)))
                elif data_type == "DATE":
                    if 'HIRE' in column['name'].upper() or 'REGISTRATION' in column['name'].upper():
                        value = self.fake_en.date_between(start_date='-5y', end_date='-1y').strftime('%Y-%m-%d')
                    elif 'ORDER' in column['name'].upper():
                        value = self.fake_en.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
                    elif 'SHIPPING' in column['name'].upper() or 'DELIVERY' in column['name'].upper():
                        value = self.fake_en.date_between(start_date='-6m', end_date='today').strftime('%Y-%m-%d')
                    else:
                        value = self.fake_en.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
                elif "TIMESTAMP" in data_type:
                    value = self.fake_en.date_time_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
                elif data_type == "CLOB":
                    if column['name'].lower().endswith('_jp') or column['name'].lower().endswith('_japanese'):
                        value = '\n'.join([self.fake_jp.paragraph() for _ in range(2)])
                    else:
                        value = '\n'.join([self.fake_en.paragraph() for _ in range(2)])
                elif data_type == "BLOB":
                    value = "EMPTY_BLOB()"  # Use Oracle's empty BLOB function
                else:
                    value = "NULL"  # Default for unsupported types
                    
                # Format value for SQL
                if isinstance(value, str) and not value.endswith("NEXTVAL") and value != "EMPTY_BLOB()":
                    # Escape single quotes
                    value = value.replace("'", "''")
                    values.append(f"'{value}'")
                else:
                    values.append(value)
            
            # Generate INSERT statement
            insert_stmt = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(values)});"
            inserts.append(insert_stmt)
        
        return inserts
