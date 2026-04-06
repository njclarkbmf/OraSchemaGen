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
from typing import List, Dict, Any, Tuple

from core import OracleObjectGenerator, OracleObject, TableInfo


class FunctionGenerator(OracleObjectGenerator):
    """
    Generates Oracle PL/SQL functions for database schemas
    """
    def __init__(self):
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

        # Generate schema-specific function packages
        for schema in schemas:
            schema_obj = OracleObject(f"{schema.lower()}_func_pkg", "PACKAGE")
            schema_obj.sql = self._generate_schema_function_package(schema, num_functions, tables)

            # Add dependencies to the tables used
            for table in tables[:min(3, len(tables))]:
                schema_obj.add_dependency(table.name)

            self.objects.append(schema_obj)

            # If output_dir is provided, also save to file
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                schema_function_file = os.path.join(output_dir, f"{schema.lower()}_functions.sql")

                with open(schema_function_file, 'w', encoding='utf-8') as f:
                    f.write(schema_obj.sql)

                logging.getLogger(__name__).info(f"Generated {num_functions} functions for {schema} schema")

        return self.objects

    def _generate_schema_function_package(self, schema: str, num_functions: int,
                                          tables: List[TableInfo]) -> str:
        """
        Generate a PL/SQL package containing function declarations (spec) and
        implementations (body) with matching signatures.

        The package spec contains only function declarations, NOT implementations.
        The package body contains the matching implementations.
        """
        # Define function templates with fixed signatures so spec and body match
        function_defs = self._get_function_definitions(schema, num_functions, tables)

        # Build package specification (declarations only)
        spec_lines = [
            f"-- Functions for {schema} Schema",
            f"CREATE OR REPLACE PACKAGE {schema.lower()}_func_pkg AS",
            "",
        ]
        for func_def in function_defs:
            func_name, params_decl, return_type, description = func_def[0], func_def[1], func_def[2], func_def[3]
            spec_lines.append(f"    -- {description}")
            spec_lines.append(f"    FUNCTION {func_name}({params_decl}) RETURN {return_type};")
            spec_lines.append("")

        spec_lines.append(f"END {schema.lower()}_func_pkg;")
        spec_lines.append("/")
        spec_lines.append("")

        # Build package body (implementations)
        body_lines = [
            f"CREATE OR REPLACE PACKAGE BODY {schema.lower()}_func_pkg AS",
            "",
        ]
        for func_def in function_defs:
            func_name, params_decl, return_type, description, body_impl = func_def
            body_lines.append(f"    -- {description}")
            body_lines.append(f"    FUNCTION {func_name}({params_decl}) RETURN {return_type}")
            body_lines.append(f"    {body_impl}")
            body_lines.append("")

        body_lines.append(f"END {schema.lower()}_func_pkg;")
        body_lines.append("/")

        return "\n".join(spec_lines) + "\n" + "\n".join(body_lines)

    def _get_function_definitions(self, schema: str, count: int,
                                   tables: List[TableInfo]) -> List[Tuple]:
        """
        Return a list of function definitions. Each tuple contains:
        (function_name, params_declaration, return_type, description, body_impl)

        Signatures are generated once and reused for both spec and body.
        """
        definitions = []

        # Function 1: date formatting
        definitions.append((
            f"format_date_{schema.lower()}",
            "p_date IN DATE, p_format IN VARCHAR2 DEFAULT 'YYYY-MM-DD'",
            "VARCHAR2",
            "Format a date using the specified format model",
            """IS
    BEGIN
        RETURN TO_CHAR(p_date, p_format);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;""",
        ))

        # Function 2: numeric check (SQL-callable, returns VARCHAR2 'Y'/'N')
        definitions.append((
            f"is_valid_number_{schema.lower()}",
            "p_value IN VARCHAR2",
            "VARCHAR2",
            "Check if a string is a valid numeric value. Returns 'Y' or 'N'.",
            """IS
    BEGIN
        IF REGEXP_LIKE(p_value, '^[0-9]+(\\.[0-9]+)?$') THEN
            RETURN 'Y';
        ELSE
            RETURN 'N';
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'N';
    END;""",
        ))

        # Function 3: count records in first available table
        if tables:
            tbl = tables[0]
            pk_cols = tbl.get_primary_key_columns()
            pk_col = pk_cols[0] if pk_cols else tbl.columns[0]['name'] if tbl.columns else 'ID'
            definitions.append((
                f"count_{tbl.name.lower()}_{schema.lower()}",
                f"p_schema_owner IN VARCHAR2 DEFAULT USER",
                "NUMBER",
                f"Count records in {tbl.name} table",
                f"""IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM {tbl.name};
    RETURN v_count;
EXCEPTION
    WHEN OTHERS THEN
        RETURN -1;
END;""",
            ))

        # Add more functions up to count
        extra_functions = [
            (
                f"compute_metric_{schema.lower()}",
                "p_factor IN NUMBER DEFAULT 1.0",
                "NUMBER",
                "Compute a generic metric for reporting",
                """IS
    v_result NUMBER;
BEGIN
    v_result := ROUND(DBMS_RANDOM.VALUE(0, 1000) * p_factor, 2);
    RETURN v_result;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;""",
            ),
            (
                f"get_schema_version_{schema.lower()}",
                "",
                "VARCHAR2",
                "Return the schema version string",
                """IS
BEGIN
    RETURN '1.0.0';
END;""",
            ),
        ]

        idx = len(definitions)
        while idx < count:
            definitions.append(extra_functions[(idx - len(definitions)) % len(extra_functions)])
            idx += 1

        return definitions[:count]

    def generate_function_signature(self, function_name: str, parameters: List[Dict],
                                     return_type: str, include_as: bool = True) -> str:
        """
        Generate a function signature for use in other generators.

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
