#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
function_generator.py - Function generation for OraSchemaGen

This module provides the FunctionGenerator class which creates Oracle database
functions for various database operations.

Author: John Clark Naldoza
"""

import os
import random
import logging
from typing import List, Dict, Any

from core import OracleObjectGenerator, OracleObject, TableInfo

class FunctionGenerator(OracleObjectGenerator):
    """
    Generates Oracle PL/SQL functions for database schemas
    """
    def __init__(self):
        """
        Initialize the FunctionGenerator
        """
        super().__init__()

    def generate(self, tables: List[TableInfo], num_functions: int = 3, **kwargs) -> List[OracleObject]:
        """
        Generate Oracle PL/SQL functions
        
        Args:
            tables: List of TableInfo objects
            num_functions: Number of functions to generate
            **kwargs: Additional parameters
            
        Returns:
            List of OracleObject instances
        """
        schemas = kwargs.get('schemas', ['HR', 'FINANCE'])
        output_dir = kwargs.get('output_dir')
        
        # Generate schema-specific functions
        for schema in schemas:
            schema_obj = OracleObject(f"{schema.lower()}_func_pkg", "PACKAGE")
            schema_obj.sql = self._generate_schema_function_package(schema, num_functions)
            
            # Add dependencies to the tables used
            for table in tables[:min(3, len(tables))]:
                schema_obj.add_dependency(table.name)
                
            self.objects.append(schema_obj)
            
            # If output_dir is provided, also save to file
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                schema_function_file = os.path.join(output_dir, f"{schema.lower()}_functions.sql")
                
                with open(schema_function_file, 'w') as f:
                    f.write(schema_obj.sql)
                
                logging.getLogger(__name__).info(f"Generated {num_functions} functions for {schema} schema")
        
        return self.objects

    def _generate_function_name(self, schema: str) -> str:
        """
        Generate a descriptive function name
        
        Args:
            schema: Schema name to use in function generation
            
        Returns:
            Generated function name
        """
        function_prefixes = [
            'get', 'calculate', 'validate', 'retrieve', 'compute'
        ]
        function_subjects = [
            'employee', 'salary', 'department', 'project', 
            'budget', 'performance', 'tax', 'bonus'
        ]
        
        prefix = random.choice(function_prefixes)
        subject = random.choice(function_subjects)
        
        return f"{prefix}_{subject}_{schema.lower()}"

    def _generate_function_body(self, function_name: str) -> str:
        """
        Generate a meaningful PL/SQL function body
        
        Args:
            function_name: Name of the function to generate
            
        Returns:
            PL/SQL function body
        """
        data_types = ['NUMBER', 'VARCHAR2(100)', 'DATE']
        return_type = random.choice(data_types)
        
        input_params = [
            f"p_{random.choice(['id', 'code', 'name'])}_in {random.choice(data_types)}"
        ]
        
        function_templates = [
            # Simple calculation function
            f"""
    FUNCTION {function_name} ({input_params[0]}) RETURN {return_type} IS
        v_result {return_type};
    BEGIN
        -- Function logic
        v_result := CASE 
            WHEN {input_params[0].split()[0]} IS NULL THEN NULL
            ELSE ROUND(DBMS_RANDOM.VALUE(1, 1000), 2)
        END;
        
        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END {function_name};
            """,
            
            # Validation function
            f"""
    FUNCTION {function_name} ({input_params[0]}) RETURN {return_type} IS
        v_is_valid {return_type};
    BEGIN
        -- Validation logic
        v_is_valid := CASE 
            WHEN LENGTH({input_params[0].split()[0]}) > 0 THEN 'Y'
            ELSE 'N'
        END;
        
        RETURN v_is_valid;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END {function_name};
            """
        ]
        
        return random.choice(function_templates)

    def _generate_schema_function_package(self, schema: str, num_functions: int = 3) -> str:
        """
        Generate PL/SQL function package for a schema
        
        Args:
            schema: Schema name
            num_functions: Number of functions to generate
            
        Returns:
            SQL script with function package
        """
        package_sql = f"-- Functions for {schema} Schema\n"
        package_sql += f"CREATE OR REPLACE PACKAGE {schema.lower()}_func_pkg AS\n"
        
        generated_functions = []
        for _ in range(num_functions):
            function_name = self._generate_function_name(schema)
            function_body = self._generate_function_body(function_name)
            generated_functions.append(function_name)
            
            package_sql += f"    {function_body}\n"
        
        package_sql += f"END {schema.lower()}_func_pkg;\n"
        package_sql += "/\n"
        
        # Add package body with implementations
        package_sql += f"\nCREATE OR REPLACE PACKAGE BODY {schema.lower()}_func_pkg AS\n"
        
        for func_name in generated_functions:
            function_body = self._generate_function_body(func_name)
            package_sql += f"    {function_body}\n"
        
        package_sql += f"END {schema.lower()}_func_pkg;\n"
        package_sql += "/\n"
        
        return package_sql
    
    def generate_function_signature(self, function_name: str, parameters: List[Dict], return_type: str, include_as: bool = True) -> str:
        """
        Generate a function signature for use in other generators
        
        Args:
            function_name: Name of the function
            parameters: List of parameter dictionaries
            return_type: Return data type
            include_as: Whether to include 'AS' or 'IS' at the end
            
        Returns:
            Function signature string
        """
        param_strs = []
        for param in parameters:
            default_str = f" DEFAULT {param['default']}" if 'default' in param else ""
            param_strs.append(f"{param['name']} {param['in_out']} {param['data_type']}{default_str}")
        
        params_str = ", ".join(param_strs)
        
        if include_as:
            return f"{function_name}({params_str}) RETURN {return_type} IS"
        else:
            return f"{function_name}({params_str}) RETURN {return_type}"
