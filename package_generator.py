#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Package Generator module for SQL database objects.
Generates PL/SQL package specifications and bodies.
"""

import os
import random
import logging
from typing import Dict, List, Optional, Tuple, Union

from core import OracleObjectGenerator, OracleObject, TableInfo

logger = logging.getLogger(__name__)


class PackageGenerator(OracleObjectGenerator):
    """Generates PL/SQL package specifications and bodies."""
    
    def __init__(self):
        """Initialize the PackageGenerator."""
        super().__init__()
    
    def generate(self, tables: List[TableInfo], num_packages: int = 2, **kwargs) -> List[OracleObject]:
        """
        Generate Oracle PL/SQL packages.
        
        Args:
            tables: List of TableInfo objects
            num_packages: Number of packages to generate
            **kwargs: Additional parameters
            
        Returns:
            List of OracleObject instances
        """
        schemas = kwargs.get('schemas', ['HR', 'FINANCE'])
        output_dir = kwargs.get('output_dir')
        
        for i in range(num_packages):
            schema = random.choice(schemas) if schemas else None
            package_name = f"{schema}_PKG_{i+1}" if schema else f"UTIL_PKG_{i+1}"
            
            # Create package object
            package_obj = OracleObject(package_name, "PACKAGE")
            
            # Generate spec and body
            package_spec, package_body = self._generate_package(package_name, tables, schema)
            
            # Add dependencies
            for table in tables:
                package_obj.add_dependency(table.name)
                
            # Save the SQL
            package_obj.sql = package_spec + "\n\n" + package_body
            
            # Add to objects list
            self.objects.append(package_obj)
            
            # Save to output_dir if provided
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                spec_file = os.path.join(output_dir, f"{package_name}_spec.sql")
                body_file = os.path.join(output_dir, f"{package_name}_body.sql")
                
                with open(spec_file, 'w', encoding='utf-8') as f:
                    f.write(package_spec)
                    
                with open(body_file, 'w', encoding='utf-8') as f:
                    f.write(package_body)
                    
                logger.info(f"Package spec saved to {spec_file}")
                logger.info(f"Package body saved to {body_file}")
        
        return self.objects
    
    def _generate_package(self, package_name: str, tables: List[TableInfo], schema: Optional[str] = None) -> Tuple[str, str]:
        """
        Generate a package specification and body.
        
        Args:
            package_name: Name of the package
            tables: List of table info objects to use for package operations
            schema: Schema name (optional)
            
        Returns:
            Tuple containing (specification_sql, body_sql)
        """
        # Generate random table-related operations
        selected_tables = random.sample(tables, min(3, len(tables)))
        
        # Generate function and procedure definitions
        funcs, procs = self._generate_package_members(selected_tables)
        
        # Generate package specification
        spec = self._generate_package_spec(package_name, funcs, procs, schema)
        
        # Generate package body
        body = self._generate_package_body(package_name, funcs, procs, schema)
        
        return spec, body
    
    def _generate_package_members(self, tables: List[TableInfo]) -> Tuple[List[Dict], List[Dict]]:
        """
        Generate package member definitions.
        
        Args:
            tables: List of table info objects
            
        Returns:
            Tuple containing (functions, procedures)
        """
        functions = []
        procedures = []
        
        # Generate common utility functions
        functions.append({
            'name': 'get_formatted_date',
            'parameters': [
                {'name': 'p_date', 'data_type': 'DATE', 'in_out': 'IN'},
                {'name': 'p_format', 'data_type': 'VARCHAR2', 'in_out': 'IN', 'default': "'YYYY-MM-DD'"}
            ],
            'return_type': 'VARCHAR2',
            'description': 'Format a date using specified format',
            'body': '''
    IS
        v_result VARCHAR2(100);
    BEGIN
        RETURN TO_CHAR(p_date, p_format);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;'''
        })
        
        functions.append({
            'name': 'is_numeric',
            'parameters': [
                {'name': 'p_string', 'data_type': 'VARCHAR2', 'in_out': 'IN'}
            ],
            'return_type': 'BOOLEAN',
            'description': 'Check if a string contains only numeric characters',
            'body': '''
    IS
    BEGIN
        RETURN REGEXP_LIKE(p_string, '^[0-9]+$');
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END;'''
        })
        
        # Generate table-specific functions/procedures
        for table in tables:
            table_name = table.name
            pk_columns = table.get_primary_key_columns()
            if not pk_columns:
                continue  # Skip tables without primary key
                
            pk_col = pk_columns[0]
            
            # Generate get_[table]_by_id function
            functions.append({
                'name': f"get_{table_name.lower()}_by_id",
                'parameters': [
                    {'name': f"p_{pk_col.lower()}", 'data_type': 'NUMBER', 'in_out': 'IN'}
                ],
                'return_type': 'BOOLEAN',
                'description': f"Check if {table_name} record exists by ID",
                'body': f'''
    IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM {table_name}
        WHERE {pk_col} = p_{pk_col.lower()};
        
        RETURN (v_count > 0);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END;'''
            })
            
            # Generate validate_[table] procedure
            regular_columns = [col for col in table.get_column_names() if col != pk_col]
            if regular_columns:
                sample_column = random.choice(regular_columns)
                procedures.append({
                    'name': f"validate_{table_name.lower()}",
                    'parameters': [
                        {'name': f"p_{pk_col.lower()}", 'data_type': 'NUMBER', 'in_out': 'IN'},
                        {'name': 'p_is_valid', 'data_type': 'BOOLEAN', 'in_out': 'OUT'},
                        {'name': 'p_error_message', 'data_type': 'VARCHAR2', 'in_out': 'OUT'}
                    ],
                    'description': f"Validate {table_name} record",
                    'body': f'''
    IS
        v_record {table_name}%ROWTYPE;
    BEGIN
        -- Initialize output parameters
        p_is_valid := TRUE;
        p_error_message := NULL;
        
        -- Check if record exists
        BEGIN
            SELECT * INTO v_record
            FROM {table_name}
            WHERE {pk_col} = p_{pk_col.lower()};
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                p_is_valid := FALSE;
                p_error_message := '{table_name} record not found';
                RETURN;
        END;
        
        -- Perform validation checks
        IF v_record.{sample_column} IS NULL THEN
            p_is_valid := FALSE;
            p_error_message := '{sample_column} cannot be null';
            RETURN;
        END IF;
        
        -- Record is valid
        p_is_valid := TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            p_is_valid := FALSE;
            p_error_message := 'Error: ' || SQLERRM;
    END;'''
                })
            
            # Generate delete procedure
            procedures.append({
                'name': f"delete_{table_name.lower()}",
                'parameters': [
                    {'name': f"p_{pk_col.lower()}", 'data_type': 'NUMBER', 'in_out': 'IN'},
                    {'name': 'p_rows_deleted', 'data_type': 'NUMBER', 'in_out': 'OUT'}
                ],
                'description': f"Delete {table_name} record by ID",
                'body': f'''
    IS
    BEGIN
        DELETE FROM {table_name}
        WHERE {pk_col} = p_{pk_col.lower()};
        
        p_rows_deleted := SQL%ROWCOUNT;
        
        IF p_rows_deleted > 0 THEN
            COMMIT;
        ELSE
            p_rows_deleted := 0;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END;'''
            })
        
        return functions, procedures
    
    def _generate_package_spec(self, package_name: str, functions: List[Dict], procedures: List[Dict], schema: Optional[str] = None) -> str:
        """
        Generate a package specification.
        
        Args:
            package_name: Name of the package
            functions: List of function definitions
            procedures: List of procedure definitions
            schema: Schema name (optional)
            
        Returns:
            SQL script for package specification
        """
        schema_prefix = f"{schema}." if schema else ""
        lines = [
            f"CREATE OR REPLACE PACKAGE {schema_prefix}{package_name} AS",
            ""
        ]
        
        # Add package description as comment
        description = f"Utility package for database operations"
        lines.insert(0, "/*")
        lines.insert(1, f" * {description}")
        lines.insert(2, " */")
        lines.insert(3, "")
        
        # Add constants
        lines.append("    -- Constants")
        lines.append("    C_MAX_ATTEMPTS CONSTANT INTEGER := 3;")
        lines.append("    C_DEFAULT_PAGE_SIZE CONSTANT INTEGER := 25;")
        lines.append("")
        
        # Add function declarations
        if functions:
            lines.append("    -- Functions")
            for func in functions:
                params = ', '.join([f"{p['name']} {p['in_out']} {p['data_type']}{' DEFAULT ' + p['default'] if 'default' in p else ''}" 
                                    for p in func['parameters']])
                lines.append(f"    -- {func.get('description', 'Function')}")
                lines.append(f"    FUNCTION {func['name']}({params}) RETURN {func['return_type']};")
                lines.append("")
        
        # Add procedure declarations
        if procedures:
            lines.append("    -- Procedures")
            for proc in procedures:
                params = ', '.join([f"{p['name']} {p['in_out']} {p['data_type']}{' DEFAULT ' + p['default'] if 'default' in p else ''}"
                                    for p in proc['parameters']])
                lines.append(f"    -- {proc.get('description', 'Procedure')}")
                lines.append(f"    PROCEDURE {proc['name']}({params});")
                lines.append("")
        
        # End package spec
        lines.append(f"END {package_name};")
        lines.append("/")
        
        return "\n".join(lines)
    
    def _generate_package_body(self, package_name: str, functions: List[Dict], procedures: List[Dict], schema: Optional[str] = None) -> str:
        """
        Generate a package body.
        
        Args:
            package_name: Name of the package
            functions: List of function definitions
            procedures: List of procedure definitions
            schema: Schema name (optional)
            
        Returns:
            SQL script for package body
        """
        schema_prefix = f"{schema}." if schema else ""
        lines = [
            f"CREATE OR REPLACE PACKAGE BODY {schema_prefix}{package_name} AS",
            ""
        ]
        
        # Add function implementations
        if functions:
            for func in functions:
                params = ', '.join([f"{p['name']} {p['in_out']} {p['data_type']}{' DEFAULT ' + p['default'] if 'default' in p else ''}" 
                                    for p in func['parameters']])
                
                lines.append(f"    -- {func.get('description', 'Function')}")
                lines.append(f"    FUNCTION {func['name']}({params}) RETURN {func['return_type']}")
                lines.append(f"    {func['body']}")
                lines.append("")
        
        # Add procedure implementations
        if procedures:
            for proc in procedures:
                params = ', '.join([f"{p['name']} {p['in_out']} {p['data_type']}{' DEFAULT ' + p['default'] if 'default' in p else ''}"
                                    for p in proc['parameters']])
                
                lines.append(f"    -- {proc.get('description', 'Procedure')}")
                lines.append(f"    PROCEDURE {proc['name']}({params})")
                lines.append(f"    {proc['body']}")
                lines.append("")
        
        # End package body
        lines.append(f"END {package_name};")
        lines.append("/")
        
        return "\n".join(lines)
