#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
core.py - Core classes for the OraSchemaGen modular Oracle schema generator

This module provides the foundational classes used throughout the OraSchemaGen system,
including the base class for Oracle objects and generators.

Author: John Clark Naldoza
"""

import os
import datetime
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union, Set, Tuple
from faker import Faker

class OracleObject:
    """
    Base class representing an Oracle database object
    """
    def __init__(self, name: str, object_type: str):
        self.name = name
        self.object_type = object_type
        self.dependencies: List[str] = []
        self.sql: str = ""
        
    def add_dependency(self, dependency: str):
        """Add a dependency to this object"""
        if dependency not in self.dependencies:
            self.dependencies.append(dependency)
            
    def __str__(self) -> str:
        return f"{self.object_type} {self.name}"

class OracleObjectGenerator(ABC):
    """
    Abstract base class for Oracle object generators
    """
    def __init__(self):
        self.fake = Faker(['en_US', 'ja_JP'])
        self.objects: List[OracleObject] = []
        
    @abstractmethod
    def generate(self, **kwargs) -> List[OracleObject]:
        """Generate Oracle objects with the given parameters"""
        pass
    
    def get_objects(self) -> List[OracleObject]:
        """Get the generated objects"""
        return self.objects

class TableInfo:
    """Class to store table information"""
    def __init__(self, name: str, columns: List[Dict[str, Any]]):
        self.name = name
        self.columns = columns
        
    def get_primary_key_columns(self) -> List[str]:
        """Get the primary key columns of this table"""
        return [col['name'] for col in self.columns 
                if 'constraints' in col and 'PRIMARY KEY' in col['constraints']]
                
    def get_column_names(self) -> List[str]:
        """Get all column names of this table"""
        return [col['name'] for col in self.columns]
        
    def get_column_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a column by name"""
        for col in self.columns:
            if col['name'] == name:
                return col
        return None

class EncodingHandler:
    """
    Handles character encoding conversion
    """
    @staticmethod
    def convert_to_shift_jis(input_file: str, output_file: str) -> bool:
        """
        Convert a file from UTF-8 to Shift-JIS encoding
        
        Returns:
            bool: True if conversion was successful, False otherwise
        """
        try:
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
                return True
            except UnicodeEncodeError as e:
                print(f"⚠ Warning: Some characters couldn't be encoded in Shift-JIS. Error: {e}")
                print("  Attempting to replace problematic characters and continue...")
                
                # Fallback with character replacement
                with open(output_file, 'w', encoding='shift_jis', errors='replace') as f:
                    f.write(content)
                print(f"✓ Converted to Shift-JIS with character replacements: {output_file}")
                return True
        except Exception as e:
            print(f"Error converting to Shift-JIS: {str(e)}")
            return False

class OracleObjectFactory:
    """
    Factory for creating Oracle object generators
    """
    @staticmethod
    def create_generators(object_types: List[str]) -> List[OracleObjectGenerator]:
        """Create generators based on object types"""
        from schema_generator import SchemaGenerator
        from data_generator import DataGenerator
        from trigger_generator import TriggerGenerator
        from procedure_generator import ProcedureGenerator
        from function_generator import FunctionGenerator
        from package_generator import PackageGenerator
        from lob_generator import LobGenerator
        
        generators = []
        
        # Always include schema generator for tables
        generators.append(SchemaGenerator())
        
        # Include data generator if specified
        if 'data' in object_types or 'all' in object_types:
            generators.append(DataGenerator())
        
        # Include PL/SQL objects if specified
        if 'trigger' in object_types or 'all' in object_types:
            generators.append(TriggerGenerator())
            
        if 'procedure' in object_types or 'all' in object_types:
            generators.append(ProcedureGenerator())
            
        if 'function' in object_types or 'all' in object_types:
            generators.append(FunctionGenerator())
            
        if 'package' in object_types or 'all' in object_types:
            generators.append(PackageGenerator())
            
        if 'lob' in object_types or 'all' in object_types:
            generators.append(LobGenerator())
        
        return generators

class OutputHandler:
    """
    Handles output for generated Oracle objects
    """
    def __init__(self, output_dir: Optional[str] = None, single_file: bool = True):
        self.output_dir = output_dir
        self.single_file = single_file
        
        # Create output directory if specified and doesn't exist
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def write_objects(self, objects: List[OracleObject], output_file: str, include_header: bool = True) -> None:
        """Write objects to output"""
        if self.single_file:
            self._write_single_file(objects, output_file, include_header)
        else:
            self._write_multiple_files(objects, include_header)
    
    def _write_single_file(self, objects: List[OracleObject], output_file: str, include_header: bool) -> None:
        """Write objects to a single file"""
        # Sort objects by dependencies
        sorted_objects = self._sort_objects_by_dependencies(objects)
        
        # Write to file
        file_path = os.path.join(self.output_dir, output_file) if self.output_dir else output_file
        with open(file_path, 'w', encoding='utf-8') as f:
            # Write header
            if include_header:
                self._write_header(f)
            
            # Write objects
            for obj in sorted_objects:
                f.write(f"-- {obj.object_type}: {obj.name}\n")
                f.write(obj.sql)
                f.write("\n\n")
            
            # Write footer
            if include_header:
                self._write_footer(f)
    
    def _write_multiple_files(self, objects: List[OracleObject], include_header: bool) -> None:
        """Write each object to a separate file"""
        # Group objects by type
        objects_by_type = {}
        for obj in objects:
            if obj.object_type not in objects_by_type:
                objects_by_type[obj.object_type] = []
            objects_by_type[obj.object_type].append(obj)
        
        # Write each type to a separate file
        for obj_type, objs in objects_by_type.items():
            # Sort objects by dependencies
            sorted_objects = self._sort_objects_by_dependencies(objs)
            
            # Create type directory if it doesn't exist
            type_dir = os.path.join(self.output_dir, obj_type.lower())
            if not os.path.exists(type_dir):
                os.makedirs(type_dir)
            
            # Write each object to a file
            for obj in sorted_objects:
                file_name = f"{obj.name}.sql"
                file_path = os.path.join(type_dir, file_name)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    # Write header
                    if include_header:
                        self._write_object_header(f, obj)
                    
                    # Write object
                    f.write(obj.sql)
                    
                    # Write footer
                    if include_header:
                        self._write_object_footer(f)
    
    def _sort_objects_by_dependencies(self, objects: List[OracleObject]) -> List[OracleObject]:
        """Sort objects by dependencies"""
        # Create dependency graph
        graph = {}
        for obj in objects:
            graph[obj.name] = set(obj.dependencies)
        
        # Perform topological sort
        result = []
        visited = set()
        temp_visited = set()
        
        def visit(node_name):
            # Skip if already visited
            if node_name in visited:
                return
            
            # Check for circular dependencies
            if node_name in temp_visited:
                # Circular dependency detected, but we'll continue anyway
                return
            
            # Mark as temporarily visited
            temp_visited.add(node_name)
            
            # Visit dependencies
            for dep in graph.get(node_name, set()):
                visit(dep)
            
            # Mark as visited
            temp_visited.remove(node_name)
            visited.add(node_name)
            
            # Add to result
            for obj in objects:
                if obj.name == node_name:
                    result.append(obj)
                    break
        
        # Visit all nodes
        for obj in objects:
            if obj.name not in visited:
                visit(obj.name)
        
        return result
    
    def _write_header(self, file) -> None:
        """Write header to file"""
        timestamp = datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")
        file.write(f"""-- Export dump file generated by modular_OraSchemaGen.py
-- Version: Oracle Database 19c Enterprise Edition Release 19.0.0.0.0
-- Export Timestamp: {timestamp}
-- Character Set: UTF-8 (Will be converted to Shift-JIS)

""")
    
    def _write_footer(self, file) -> None:
        """Write footer to file"""
        timestamp = datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")
        file.write(f"""
-- Export completed successfully
-- Export Timestamp: {timestamp}
""")
    
    def _write_object_header(self, file, obj: OracleObject) -> None:
        """Write object header to file"""
        timestamp = datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")
        file.write(f"""-- {obj.object_type}: {obj.name}
-- Generated: {timestamp}
-- Dependencies: {', '.join(obj.dependencies) if obj.dependencies else 'None'}

""")
    
    def _write_object_footer(self, file) -> None:
        """Write object footer to file"""
        file.write("""
-- End of object definition
""")
