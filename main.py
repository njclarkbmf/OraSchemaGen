#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
main.py - Main program that ties everything together for OraSchemaGen

This module serves as the entry point for the OraSchemaGen Oracle schema generator,
integrating all components and providing a command-line interface.

Author: John Clark Naldoza
"""

import os
import sys
import argparse
import logging
import datetime
from typing import List, Dict, Any, Optional

from core import OracleObjectFactory, OutputHandler, TableInfo, OracleObject

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OraSchemaGenApp:
    """Main application class for OraSchemaGen"""
    
    def __init__(self):
        self.output_dir = "generated_sql"
        self.tables: List[TableInfo] = []
        self.generators = []
        self.objects: List[OracleObject] = []
        
    def parse_arguments(self) -> argparse.Namespace:
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(
            description="OraSchemaGen - Oracle Schema Generator",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        
        parser.add_argument(
            "--schemas", 
            type=str, 
            default="TEST_SCHEMA",
            help="Comma-separated list of schema names to generate"
        )
        
        parser.add_argument(
            "--tables", 
            type=int, 
            default=5,
            help="Number of tables to generate per schema"
        )
        
        parser.add_argument(
            "--data-rows", 
            type=int, 
            default=100,
            help="Number of data rows to generate per table"
        )
        
        parser.add_argument(
            "--triggers", 
            type=int, 
            default=3,
            help="Number of triggers to generate per schema"
        )
        
        parser.add_argument(
            "--procedures", 
            type=int, 
            default=3,
            help="Number of procedures to generate per schema"
        )
        
        parser.add_argument(
            "--functions", 
            type=int, 
            default=3,
            help="Number of functions to generate per schema"
        )
        
        parser.add_argument(
            "--packages", 
            type=int, 
            default=1,
            help="Number of packages to generate per schema"
        )
        
        parser.add_argument(
            "--lobs", 
            type=int, 
            default=1,
            help="Number of LOB operations to generate per schema"
        )
        
        parser.add_argument(
            "--output-dir", 
            type=str, 
            default="generated_sql",
            help="Directory to save generated SQL"
        )
        
        parser.add_argument(
            "--single-file",
            action="store_true",
            help="Generate a single SQL file instead of multiple files"
        )
        
        parser.add_argument(
            "--shift-jis",
            action="store_true",
            help="Convert output files to Shift-JIS encoding"
        )
        
        parser.add_argument(
            "--verbose", 
            action="store_true",
            help="Enable verbose logging"
        )
        
        return parser.parse_args()
    
    def setup_environment(self, args: argparse.Namespace) -> None:
        """Setup the environment based on arguments"""
        # Set log level based on verbose flag
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            logger.debug("Verbose logging enabled")
        
        # Set output directory
        self.output_dir = args.output_dir
        logger.info(f"Output directory set to: {self.output_dir}")
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
        
        # Determine which object types to generate
        object_types = []
        if args.data_rows > 0:
            object_types.append('data')
        if args.triggers > 0:
            object_types.append('trigger')
        if args.procedures > 0:
            object_types.append('procedure')
        if args.functions > 0:
            object_types.append('function')
        if args.packages > 0:
            object_types.append('package')
        if args.lobs > 0:
            object_types.append('lob')
        
        # Create generators
        self.generators = OracleObjectFactory.create_generators(object_types)
        logger.info(f"Created generators for object types: tables, {', '.join(object_types)}")
    
    def generate_objects(self, args: argparse.Namespace) -> None:
        """Generate all database objects based on specified counts"""
        # Process each specified schema
        schemas = args.schemas.split(",")
        
        for schema in schemas:
            schema = schema.strip()
            logger.info(f"Generating objects for schema: {schema}")
            
            # Generate tables first using SchemaGenerator
            schema_generator = self.generators[0]  # SchemaGenerator is always first
            table_objects = schema_generator.generate(
                table_count=args.tables, 
                include_storage=True
            )
            
            self.objects.extend(table_objects)
            
            # Extract TableInfo objects
            self.tables = schema_generator.get_tables()
            
            # Generate other objects using their respective generators
            for generator in self.generators[1:]:  # Skip SchemaGenerator
                generator_name = generator.__class__.__name__
                
                # Call the appropriate generator with the right parameters
                if generator_name == 'DataGenerator' and args.data_rows > 0:
                    objects = generator.generate(
                        tables=self.tables,
                        rows_per_table=args.data_rows
                    )
                    self.objects.extend(objects)
                    
                elif generator_name == 'TriggerGenerator' and args.triggers > 0:
                    objects = generator.generate(
                        tables=self.tables,
                        num_triggers=args.triggers
                    )
                    self.objects.extend(objects)
                    
                elif generator_name == 'ProcedureGenerator' and args.procedures > 0:
                    objects = generator.generate(
                        tables=self.tables,
                        num_procedures=args.procedures
                    )
                    self.objects.extend(objects)
                    
                elif generator_name == 'FunctionGenerator' and args.functions > 0:
                    objects = generator.generate(
                        tables=self.tables,
                        num_functions=args.functions
                    )
                    self.objects.extend(objects)
                    
                elif generator_name == 'PackageGenerator' and args.packages > 0:
                    objects = generator.generate(
                        tables=self.tables,
                        num_packages=args.packages
                    )
                    self.objects.extend(objects)
                    
                elif generator_name == 'LobGenerator' and args.lobs > 0:
                    objects = generator.generate(
                        tables=self.tables,
                        num_lobs=args.lobs
                    )
                    self.objects.extend(objects)
    
    def save_generated_sql(self, args: argparse.Namespace) -> None:
        """Save all generated SQL to files"""
        logger.info("Saving generated SQL...")
        
        # Create output handler
        output_handler = OutputHandler(
            output_dir=self.output_dir,
            single_file=args.single_file
        )
        
        # Get timestamp for filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Write objects to file(s)
        if args.single_file:
            output_file = f"oraschemagen_{timestamp}.sql"
            output_handler.write_objects(self.objects, output_file)
            logger.info(f"All SQL saved to: {os.path.join(self.output_dir, output_file)}")
            
            # Convert to Shift-JIS if requested
            if args.shift_jis:
                from core import EncodingHandler
                input_file = os.path.join(self.output_dir, output_file)
                output_file_sjis = os.path.join(self.output_dir, f"oraschemagen_{timestamp}_sjis.sql")
                EncodingHandler.convert_to_shift_jis(input_file, output_file_sjis)
        else:
            output_handler.write_objects(self.objects, "")
            logger.info(f"SQL files saved to subdirectories in: {self.output_dir}")
            
            # Convert to Shift-JIS if requested
            if args.shift_jis:
                logger.info("Converting files to Shift-JIS is not supported for multiple file output")
    
    def run(self) -> int:
        """Run the application"""
        try:
            # Parse command line arguments
            args = self.parse_arguments()
            
            # Setup environment
            self.setup_environment(args)
            
            # Generate objects
            self.generate_objects(args)
            
            # Save generated SQL
            self.save_generated_sql(args)
            
            logger.info("OraSchemaGen completed successfully!")
            return 0
            
        except Exception as e:
            logger.error(f"Error: {str(e)}", exc_info=True)
            return 1


if __name__ == "__main__":
    app = OraSchemaGenApp()
    sys.exit(app.run())
