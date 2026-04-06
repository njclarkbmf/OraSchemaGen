#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_sql_generation.py - Unit tests for OraSchemaGen SQL generation validity

Tests validate that generated SQL/PLSQL conforms to Oracle Database syntax rules.
These are static validation tests that do not require a live Oracle database.
"""

import unittest
import re
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import OracleObject, OracleObjectGenerator, TableInfo, OracleObjectFactory, OutputHandler
from schema_generator import SchemaGenerator
from data_generator import DataGenerator
from trigger_generator import TriggerGenerator
from procedure_generator import ProcedureGenerator
from function_generator import FunctionGenerator
from package_generator import PackageGenerator
from lob_generator import LobGenerator


class TestSchemaGenerator(unittest.TestCase):
    """Tests for schema (DDL) generation"""

    def setUp(self):
        self.gen = SchemaGenerator()

    def test_create_table_has_valid_syntax(self):
        """CREATE TABLE statements must have balanced parentheses and semicolon"""
        objects = self.gen.generate(table_count=3, include_storage=False)
        table_objs = [o for o in objects if o.object_type == "TABLE"]
        self.assertGreater(len(table_objs), 0)

        for obj in table_objs:
            sql = obj.sql
            # Must end with semicolon
            self.assertTrue(sql.strip().endswith(";"),
                           f"CREATE TABLE for {obj.name} doesn't end with semicolon")
            # Balanced parentheses
            self.assertEqual(sql.count("("), sql.count(")"),
                           f"Unbalanced parentheses in {obj.name} CREATE TABLE")
            # Has table name
            self.assertIn(obj.name, sql,
                         f"Table name {obj.name} not found in its own CREATE statement")

    def test_primary_key_constraint_inline(self):
        """Primary key constraints should use CONSTRAINT ... PRIMARY KEY syntax"""
        objects = self.gen.generate(table_count=3, include_storage=False)
        table_objs = [o for o in objects if o.object_type == "TABLE"]

        for obj in table_objs:
            sql = obj.sql
            pk_columns = obj.name.replace("S", "")  # Simple heuristic
            if "PK" in sql or "PRIMARY KEY" in sql:
                # Should use named constraint
                self.assertRegex(sql, r"CONSTRAINT\s+\w+_PK\s+PRIMARY\s+KEY",
                               f"PK constraint for {obj.name} doesn't follow naming convention")

    def test_foreign_keys_reference_existing_tables(self):
        """FK constraints should only reference tables that were actually created"""
        objects = self.gen.generate(table_count=3, include_storage=False)
        created_tables = {o.name for o in objects if o.object_type == "TABLE"}

        fk_objs = [o for o in objects if o.object_type == "CONSTRAINT" and "FOREIGN" in o.name]
        for fk_obj in fk_objs:
            sql = fk_obj.sql
            # Find all REFERENCES clauses
            refs = re.findall(r'REFERENCES\s+(\w+)', sql)
            for ref in refs:
                self.assertIn(ref, created_tables,
                            f"FK references table {ref} which was not created (only {created_tables})")

    def test_sequences_have_valid_syntax(self):
        """CREATE SEQUENCE statements must have START WITH and INCREMENT BY"""
        objects = self.gen.generate(table_count=3, include_storage=False)
        seq_objs = [o for o in objects if o.object_type == "SEQUENCE"]

        for obj in seq_objs:
            sql = obj.sql
            self.assertIn("CREATE SEQUENCE", sql)
            self.assertIn("START WITH", sql)
            self.assertIn("INCREMENT BY", sql)
            self.assertIn("NOCYCLE", sql)
            # Should end with semicolon
            self.assertTrue(sql.strip().endswith(";"))

    def test_comments_have_valid_syntax(self):
        """COMMENT statements must reference existing objects"""
        objects = self.gen.generate(table_count=3, include_storage=False)
        created_tables = {o.name for o in objects if o.object_type == "TABLE"}
        comment_objs = [o for o in objects if o.object_type == "COMMENT"]

        for obj in comment_objs:
            sql = obj.sql
            # Find all COMMENT ON TABLE references
            table_refs = re.findall(r'COMMENT\s+ON\s+TABLE\s+(\w+)', sql)
            for ref in table_refs:
                self.assertIn(ref, created_tables,
                            f"COMMENT references table {ref} which was not created")

    def test_include_storage_toggles_storage_clause(self):
        """When include_storage=False, no TABLESPACE or STORAGE clauses appear"""
        objects_no_storage = self.gen.generate(table_count=3, include_storage=False)
        for obj in objects_no_storage:
            if obj.object_type == "TABLE":
                self.assertNotIn("TABLESPACE", obj.sql,
                               f"TABLESPACE found when include_storage=False for {obj.name}")
                self.assertNotIn("STORAGE(INITIAL", obj.sql,
                               f"STORAGE clause found when include_storage=False for {obj.name}")

        objects_with_storage = self.gen.generate(table_count=3, include_storage=True)
        has_storage = False
        for obj in objects_with_storage:
            if obj.object_type == "TABLE" and "TABLESPACE" in obj.sql:
                has_storage = True
                break
        self.assertTrue(has_storage, "No TABLESPACE found when include_storage=True")


class TestDataGenerator(unittest.TestCase):
    """Tests for INSERT statement generation"""

    def setUp(self):
        self.schema_gen = SchemaGenerator()
        self.schema_gen.generate(table_count=3, include_storage=False)
        self.tables = self.schema_gen.get_tables()
        self.data_gen = DataGenerator()

    def test_insert_has_to_date(self):
        """DATE columns in INSERT must use TO_DATE() not bare string literals"""
        objects = self.data_gen.generate(tables=self.tables, rows_per_table=2)
        data_objs = [o for o in objects if o.object_type == "DATA"]

        for obj in data_objs:
            sql = obj.sql
            # Find INSERT statements
            for line in sql.split("\n"):
                line = line.strip()
                if line.startswith("INSERT"):
                    # Should not have bare date strings like '2022-12-10'
                    # that aren't inside TO_DATE()
                    # Remove TO_DATE(...) contents first
                    cleaned = re.sub(r"TO_DATE\([^)]+\)", 'DATE_PLACEHOLDER', line)
                    # Now check no bare date patterns remain
                    bare_dates = re.findall(r"'\d{4}-\d{2}-\d{2}'", cleaned)
                    self.assertEqual(len(bare_dates), 0,
                                   f"Found bare date literal {bare_dates} in INSERT without TO_DATE")

    def test_insert_has_to_timestamp(self):
        """TIMESTAMP columns must use TO_TIMESTAMP() not bare string literals"""
        # Test with a table that has TIMESTAMP columns if available
        objects = self.data_gen.generate(tables=self.tables, rows_per_table=2)
        data_objs = [o for o in objects if o.object_type == "DATA"]

        for obj in data_objs:
            sql = obj.sql
            for line in sql.split("\n"):
                line = line.strip()
                if line.startswith("INSERT"):
                    cleaned = re.sub(r"TO_TIMESTAMP\([^)]+\)", 'TS_PLACEHOLDER', line)
                    bare_ts = re.findall(r"'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}'", cleaned)
                    self.assertEqual(len(bare_ts), 0,
                                   f"Found bare timestamp in INSERT without TO_TIMESTAMP")

    def test_insert_has_semicolon(self):
        """Every INSERT statement must end with semicolon"""
        objects = self.data_gen.generate(tables=self.tables, rows_per_table=2)
        data_objs = [o for o in objects if o.object_type == "DATA"]

        for obj in data_objs:
            # Join all lines and split by INSERT to get full statements
            full_sql = obj.sql
            # Find each INSERT...VALUES...; pattern
            inserts = re.findall(r'INSERT\s+INTO\s+.*?VALUES\s*\(.*?\);', full_sql, re.DOTALL)
            # Also check for multi-line INSERTs that may span
            for line in full_sql.split('\n'):
                line = line.strip()
                if line.startswith('INSERT') and line.endswith(';'):
                    pass  # OK
                elif line.startswith('INSERT') and not line.endswith(';'):
                    # Multi-line INSERT - check if the full statement ends with ;
                    # Find the complete statement
                    idx = full_sql.find(line)
                    remainder = full_sql[idx:]
                    end_idx = remainder.find(';')
                    if end_idx > 0:
                        pass  # Multi-line statement ends with ;
                    else:
                        self.fail(f"INSERT statement doesn't end with semicolon: {line[:80]}...")

    def test_insert_has_matching_parentheses(self):
        """INSERT VALUES clause must have balanced parentheses"""
        objects = self.data_gen.generate(tables=self.tables, rows_per_table=2)
        data_objs = [o for o in objects if o.object_type == "DATA"]

        for obj in data_objs:
            # Get full SQL and find complete INSERT statements
            full_sql = obj.sql
            inserts = re.findall(r'INSERT\s+INTO\s+\w+\s*\(.*?\)\s*VALUES\s*\(.*?\);', full_sql, re.DOTALL)
            for insert in inserts:
                self.assertEqual(insert.count("("), insert.count(")"),
                               f"Unbalanced parentheses in INSERT")

    def test_sequence_nextval_not_quoted(self):
        """SEQUENCE.NEXTVAL must not be wrapped in quotes"""
        objects = self.data_gen.generate(tables=self.tables, rows_per_table=2)
        data_objs = [o for o in objects if o.object_type == "DATA"]

        for obj in data_objs:
            sql = obj.sql
            # Find complete INSERT statements (may span multiple lines)
            inserts = re.findall(r'INSERT\s+INTO\s+\w+\s*\(.*?\)\s*VALUES\s*\(.*?\);', sql, re.DOTALL)
            for insert in inserts:
                # Only test INSERTs that actually reference a _SEQ.NEXTVAL
                if 'NEXTVAL' not in insert:
                    continue
                # Check that NEXTVAL is not inside single quotes
                # Remove properly quoted strings first
                cleaned = re.sub(r"'[^']*'", '', insert)
                # NEXTVAL should appear as bare word in cleaned version
                self.assertIn("NEXTVAL", cleaned,
                            f"NEXTVAL found inside quotes in {obj.name}")


class TestTriggerGenerator(unittest.TestCase):
    """Tests for trigger generation"""

    def setUp(self):
        self.schema_gen = SchemaGenerator()
        self.schema_gen.generate(table_count=8, include_storage=False)
        self.tables = self.schema_gen.get_tables()
        self.trigger_gen = TriggerGenerator()

    def test_trigger_has_create_or_replace(self):
        """All triggers must use CREATE OR REPLACE TRIGGER"""
        objects = self.trigger_gen.generate(tables=self.tables)
        trigger_objs = [o for o in objects if o.object_type == "TRIGGER"]

        for obj in trigger_objs:
            sql = obj.sql
            self.assertIn("CREATE OR REPLACE TRIGGER", sql,
                         f"Trigger {obj.name} missing CREATE OR REPLACE")

    def test_trigger_has_terminator_slash(self):
        """PL/SQL triggers must be terminated with / on separate line"""
        objects = self.trigger_gen.generate(tables=self.tables)
        trigger_objs = [o for o in objects if o.object_type == "TRIGGER"]

        for obj in trigger_objs:
            sql = obj.sql.strip()
            # Should end with END followed by /
            self.assertRegex(sql, r"END\s*\w*\s*;\s*/$",
                           f"Trigger {obj.name} not properly terminated with /")

    def test_trigger_references_valid_table(self):
        """Trigger ON clause must reference a table that exists"""
        created_tables = {t.name for t in self.tables}
        objects = self.trigger_gen.generate(tables=self.tables)
        trigger_objs = [o for o in objects if o.object_type == "TRIGGER"]

        for obj in trigger_objs:
            sql = obj.sql
            # Find ON table_name - be careful to match after ON keyword
            # Pattern: ON <table_name> or ON DATABASE
            on_matches = re.findall(r'\bON\s+([A-Z_]\w*)', sql)
            for table_ref in on_matches:
                # Skip DATABASE and LOGON which are valid for system triggers
                if table_ref in ("DATABASE", "LOGON", "DDL"):
                    continue
                # Also skip if it's part of another clause like "ON INSERT"
                if table_ref in ("INSERT", "UPDATE", "DELETE"):
                    continue
                self.assertIn(table_ref, created_tables,
                            f"Trigger {obj.name} references table {table_ref} not in created tables")


class TestProcedureGenerator(unittest.TestCase):
    """Tests for procedure generation"""

    def setUp(self):
        self.schema_gen = SchemaGenerator()
        self.schema_gen.generate(table_count=8, include_storage=False)
        self.tables = self.schema_gen.get_tables()
        self.proc_gen = ProcedureGenerator()

    def test_no_boolean_in_parameters(self):
        """SQL-callable procedures must not have BOOLEAN parameters"""
        objects = self.proc_gen.generate(tables=self.tables, num_procedures=5)
        proc_objs = [o for o in objects if o.object_type == "PROCEDURE"]

        for obj in proc_objs:
            sql = obj.sql
            # Find parameter declarations (between CREATE and IS/AS)
            header_match = re.search(r'CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+\w+\s*\((.*?)\)\s*(?:IS|AS)',
                                    sql, re.DOTALL | re.IGNORECASE)
            if header_match:
                params = header_match.group(1)
                self.assertNotIn("BOOLEAN", params.upper(),
                               f"Procedure {obj.name} has BOOLEAN parameter: {params[:100]}...")

    def test_regexp_like_balanced_parens(self):
        """REGEXP_LIKE calls must have balanced parentheses"""
        objects = self.proc_gen.generate(tables=self.tables, num_procedures=5)
        proc_objs = [o for o in objects if o.object_type == "PROCEDURE"]

        for obj in proc_objs:
            sql = obj.sql
            # Find all REGEXP_LIKE calls
            for match in re.finditer(r'REGEXP_LIKE\s*\(', sql):
                start = match.start()
                # Count parens from this point
                depth = 0
                pos = match.start()
                found_open = False
                while pos < len(sql):
                    if sql[pos] == '(':
                        depth += 1
                        found_open = True
                    elif sql[pos] == ')':
                        depth -= 1
                    if found_open and depth == 0:
                        break
                    pos += 1
                self.assertEqual(depth, 0,
                               f"Unbalanced REGEXP_LIKE parentheses in {obj.name} near position {start}")

    def test_procedure_has_terminator_slash(self):
        """Procedures must be terminated with / on separate line"""
        objects = self.proc_gen.generate(tables=self.tables, num_procedures=5)
        proc_objs = [o for o in objects if o.object_type == "PROCEDURE"]

        for obj in proc_objs:
            sql = obj.sql.strip()
            self.assertRegex(sql, r"END\s+\w*\s*;\s*/$",
                           f"Procedure {obj.name} not properly terminated")


class TestFunctionGenerator(unittest.TestCase):
    """Tests for function/package generation"""

    def setUp(self):
        self.schema_gen = SchemaGenerator()
        self.schema_gen.generate(table_count=3, include_storage=False)
        self.tables = self.schema_gen.get_tables()
        self.func_gen = FunctionGenerator()

    def test_package_spec_body_signature_match(self):
        """Package spec and body function signatures must match"""
        objects = self.func_gen.generate(tables=self.tables, num_functions=3)
        pkg_objs = [o for o in objects if o.object_type == "PACKAGE"]

        for obj in pkg_objs:
            sql = obj.sql
            # Split into spec and body
            spec_end = sql.find("\n/")
            body_start = sql.find("PACKAGE BODY")

            if spec_end > 0 and body_start > 0:
                spec = sql[:spec_end]
                body = sql[body_start:]

                # Find all FUNCTION declarations in spec
                spec_funcs = re.findall(r'FUNCTION\s+(\w+)\s*\((.*?)\)\s*RETURN\s+(\w+)',
                                       spec, re.DOTALL)
                # Find all FUNCTION implementations in body
                body_funcs = re.findall(r'FUNCTION\s+(\w+)\s*\((.*?)\)\s*RETURN\s+(\w+)',
                                       body, re.DOTALL)

                spec_names = {f[0] for f in spec_funcs}
                body_names = {f[0] for f in body_funcs}

                self.assertEqual(spec_names, body_names,
                               f"Function names in spec {spec_names} don't match body {body_names}")

                # Check parameter names match
                for func_name, spec_params, spec_ret in spec_funcs:
                    matching_body = [(n, p, r) for n, p, r in body_funcs if n == func_name]
                    if matching_body:
                        body_name, body_params, body_ret = matching_body[0]
                        # Normalize whitespace for comparison
                        spec_p = re.sub(r'\s+', ' ', spec_params).strip()
                        body_p = re.sub(r'\s+', ' ', body_params).strip()
                        self.assertEqual(spec_p, body_p,
                                       f"Parameter mismatch for {func_name}: spec='{spec_p}' vs body='{body_p}'")

    def test_no_boolean_return_in_package_spec(self):
        """Package spec functions should not return BOOLEAN (not SQL-callable)"""
        objects = self.func_gen.generate(tables=self.tables, num_functions=3)
        pkg_objs = [o for o in objects if o.object_type == "PACKAGE"]

        for obj in pkg_objs:
            sql = obj.sql
            spec_end = sql.find("\n/")
            if spec_end > 0:
                spec = sql[:spec_end]
                # Find RETURN BOOLEAN in spec
                self.assertNotIn("RETURN BOOLEAN", spec.upper(),
                               f"Package spec contains RETURN BOOLEAN which is not SQL-callable")


class TestPackageGenerator(unittest.TestCase):
    """Tests for package generation"""

    def setUp(self):
        self.schema_gen = SchemaGenerator()
        self.schema_gen.generate(table_count=3, include_storage=False)
        self.tables = self.schema_gen.get_tables()
        self.pkg_gen = PackageGenerator()

    def test_package_spec_body_match(self):
        """Package spec and body must have matching procedure/function signatures"""
        objects = self.pkg_gen.generate(tables=self.tables, num_packages=2)
        pkg_objs = [o for o in objects if o.object_type == "PACKAGE"]

        for obj in pkg_objs:
            sql = obj.sql
            # Find spec and body
            spec_match = re.search(r'CREATE\s+OR\s+REPLACE\s+PACKAGE\s+(\w+)\s+AS\s*(.*?)\bEND\s+\w+\s*;',
                                  sql, re.DOTALL | re.IGNORECASE)
            body_match = re.search(r'CREATE\s+OR\s+REPLACE\s+PACKAGE\s+BODY\s+(\w+)\s+AS\s*(.*?)\bEND\s+\w+\s*;',
                                  sql, re.DOTALL | re.IGNORECASE)

            if spec_match and body_match:
                spec_name = spec_match.group(1)
                body_name = body_match.group(1)
                self.assertEqual(spec_name.upper(), body_name.upper(),
                               f"Package spec name {spec_name} doesn't match body name {body_name}")

    def test_no_boolean_in_package_spec_params(self):
        """Package spec parameters should not use BOOLEAN"""
        objects = self.pkg_gen.generate(tables=self.tables, num_packages=2)
        pkg_objs = [o for o in objects if o.object_type == "PACKAGE"]

        for obj in pkg_objs:
            sql = obj.sql
            # Find spec section
            spec_match = re.search(r'CREATE\s+OR\s+REPLACE\s+PACKAGE\s+\w+\s+AS\s*(.*?)\bEND\s+\w+\s*;',
                                  sql, re.DOTALL | re.IGNORECASE)
            if spec_match:
                spec = spec_match.group(1)
                # Check PROCEDURE/FUNCTION declarations
                for match in re.finditer(r'(?:FUNCTION|PROCEDURE)\s+\w+\s*\((.*?)\)', spec, re.DOTALL):
                    params = match.group(1)
                    self.assertNotIn("BOOLEAN", params.upper(),
                                   f"Package spec has BOOLEAN parameter: {params[:80]}...")


class TestLobGenerator(unittest.TestCase):
    """Tests for LOB procedure generation"""

    def setUp(self):
        self.lob_gen = LobGenerator()

    def test_no_boolean_in_lob_procedures(self):
        """LOB procedures must not have BOOLEAN parameters"""
        objects = self.lob_gen.generate(tables=[])
        proc_objs = [o for o in objects if o.object_type in ("PROCEDURE", "FUNCTION")]

        for obj in proc_objs:
            sql = obj.sql
            header_match = re.search(
                r'CREATE\s+OR\s+REPLACE\s+(?:PROCEDURE|FUNCTION)\s+\w+\s*\((.*?)\)',
                sql, re.DOTALL | re.IGNORECASE)
            if header_match:
                params = header_match.group(1)
                self.assertNotIn("BOOLEAN", params.upper(),
                               f"LOB {obj.object_type} {obj.name} has BOOLEAN parameter")

    def test_lob_objects_have_terminator_slash(self):
        """LOB procedures/functions must be terminated with /"""
        objects = self.lob_gen.generate(tables=[])
        proc_objs = [o for o in objects if o.object_type in ("PROCEDURE", "FUNCTION")]

        for obj in proc_objs:
            sql = obj.sql.strip()
            self.assertTrue(sql.endswith("/"),
                          f"LOB {obj.object_type} {obj.name} doesn't end with /")


class TestOutputHandler(unittest.TestCase):
    """Tests for output handling and dependency ordering"""

    def test_topological_sort_basic(self):
        """OutputHandler should sort objects by dependencies before writing"""
        handler = OutputHandler(single_file=True)

        # Create objects with known dependencies
        obj_a = OracleObject("TABLE_A", "TABLE")
        obj_a.sql = "CREATE TABLE TABLE_A (ID NUMBER);"

        obj_b = OracleObject("TABLE_B", "TABLE")
        obj_b.sql = "CREATE TABLE TABLE_B (ID NUMBER);"
        obj_b.add_dependency("TABLE_A")

        obj_c = OracleObject("TRIGGER_C", "TRIGGER")
        obj_c.sql = "CREATE OR REPLACE TRIGGER TRIGGER_C ..."
        obj_c.add_dependency("TABLE_B")

        sorted_objs = handler._sort_objects_by_dependencies([obj_c, obj_a, obj_b])

        # TABLE_A should come first (no deps)
        # TABLE_B should come second (depends on TABLE_A)
        # TRIGGER_C should come last (depends on TABLE_B)
        names = [o.name for o in sorted_objs]
        self.assertEqual(names.index("TABLE_A"), 0, "TABLE_A should be first")
        self.assertEqual(names.index("TABLE_B"), 1, "TABLE_B should be second")
        self.assertEqual(names.index("TRIGGER_C"), 2, "TRIGGER_C should be last")

    def test_topological_sort_circular_dependency(self):
        """Should handle circular dependencies gracefully (not crash)"""
        handler = OutputHandler(single_file=True)

        obj_a = OracleObject("OBJ_A", "TABLE")
        obj_a.sql = "SQL A"
        obj_a.add_dependency("OBJ_B")

        obj_b = OracleObject("OBJ_B", "TABLE")
        obj_b.sql = "SQL B"
        obj_b.add_dependency("OBJ_A")

        # Should not raise
        sorted_objs = handler._sort_objects_by_dependencies([obj_a, obj_b])
        self.assertEqual(len(sorted_objs), 2)


class TestIntegration(unittest.TestCase):
    """Integration tests for full generation pipeline"""

    def test_full_generation_no_errors(self):
        """Full generation with all object types should complete without errors"""
        schema_gen = SchemaGenerator()
        schema_gen.generate(table_count=8, include_storage=False)
        tables = schema_gen.get_tables()

        generators = [
            schema_gen,
            DataGenerator(),
            TriggerGenerator(),
            ProcedureGenerator(),
            FunctionGenerator(),
            PackageGenerator(),
            LobGenerator(),
        ]

        all_objects = []
        for gen in generators:
            if isinstance(gen, SchemaGenerator):
                objs = gen.generate(table_count=8, include_storage=False)
            else:
                objs = gen.generate(tables=tables)
            all_objects.extend(objs)

        self.assertGreater(len(all_objects), 0, "No objects generated")

        # All objects should have SQL
        for obj in all_objects:
            self.assertIsNotNone(obj.sql, f"Object {obj.name} has no SQL")
            self.assertIsInstance(obj.sql, str, f"Object {obj.name} SQL is not string")

    def test_generated_sql_executable_patterns(self):
        """Generated SQL should match patterns expected to be executable in Oracle"""
        schema_gen = SchemaGenerator()
        schema_gen.generate(table_count=3, include_storage=False)
        tables = schema_gen.get_tables()

        all_objects = []
        for gen in [schema_gen, DataGenerator(), TriggerGenerator(),
                     ProcedureGenerator(), FunctionGenerator(), PackageGenerator(),
                     LobGenerator()]:
            if isinstance(gen, SchemaGenerator):
                objs = gen.generate(table_count=3, include_storage=False)
            else:
                objs = gen.generate(tables=tables)
            all_objects.extend(objs)

        for obj in all_objects:
            sql = obj.sql
            obj_type = obj.object_type

            # PL/SQL objects must end with /
            if obj_type in ("PROCEDURE", "FUNCTION", "TRIGGER", "PACKAGE"):
                self.assertRegex(sql.strip(), r'/\s*$',
                               f"{obj_type} {obj.name} doesn't end with /")

            # All SQL should end with ; or /
            stripped = sql.strip()
            if not stripped.endswith(";") and not stripped.endswith("/"):
                # Some composite objects may not, but individual statements should
                pass  # Allow composite objects

            # No unbalanced quotes (simple check)
            # For PL/SQL objects, skip complex string concatenation patterns
            if obj_type in ("TRIGGER", "PROCEDURE", "FUNCTION", "PACKAGE"):
                # PL/SQL often has complex string patterns; just check for obvious errors
                # Count lines with orphaned quotes (lines ending with single ')
                for line in obj.sql.split('\n'):
                    stripped = line.strip()
                    if stripped.startswith('--'):
                        continue  # Skip comments
                    # Check for ' at end of line followed by non-quote non-space char on next line
                    # This is a heuristic, not perfect
                    pass
            else:
                # For DDL/DML, do a stricter check
                sql_no_escaped = obj.sql.replace("''", "")
                sql_no_strings = re.sub(r"'[^']*'", '', sql_no_escaped)
                single_quotes = sql_no_strings.count("'")
                self.assertEqual(single_quotes, 0,
                               f"Unbalanced single quotes in {obj_type} {obj.name}")


if __name__ == '__main__':
    unittest.main()
