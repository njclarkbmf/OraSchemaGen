# Principles and Patterns

## Architecture Principles

### 1. Generate executable Oracle SQL, not approximate SQL

Every line of SQL emitted by OraSchemaGen must be valid, executable Oracle SQL. Generated scripts should run in SQL*Plus, SQLcl, or SQL Developer without manual edits. This means correct syntax, proper quoting, appropriate termination characters, and adherence to Oracle's SQL and PL/SQL grammar.

```python
# Good: Executable CREATE TABLE with proper syntax
create_stmt = f"CREATE TABLE {table_name} \n(\n  " + ",\n  ".join(column_definitions)
create_stmt += "\n);"

# Bad: Incomplete or placeholder SQL
create_stmt = f"CREATE TABLE {table_name} ( ... );"
```

### 2. Prefer explicit conversions over implicit NLS-dependent behavior

Never rely on implicit DATE or NUMBER conversions that depend on `NLS_DATE_FORMAT`, `NLS_NUMERIC_CHARACTERS`, or session-level locale settings. Always use `TO_DATE()` with an explicit format mask, `TO_NUMBER()` with explicit format, and `TO_CHAR()` with explicit format models.

```sql
-- Good: Explicit TO_DATE with format mask
TO_DATE('2025-03-15', 'YYYY-MM-DD')
TO_TIMESTAMP('2025-03-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS')

-- Bad: Relies on NLS_DATE_FORMAT
'15-MAR-25'
'2025-03-15'
```

This principle is implemented throughout `data_generator.py`, where all DATE and TIMESTAMP values use `TO_DATE()` and `TO_TIMESTAMP()` with explicit format masks rather than relying on session-level NLS settings.

### 3. Separate logical schema modeling from rendered SQL text

The `TableInfo` class in `core.py` represents the logical model of a table -- its columns, types, and constraints -- independently of any SQL text. Generators consume `TableInfo` objects and produce SQL strings. This separation allows the same model to drive table creation, DML generation, trigger generation, and more without duplicating schema knowledge.

```python
# Logical model -- no SQL here
class TableInfo:
    def __init__(self, name: str, columns: List[Dict[str, Any]]):
        self.name = name
        self.columns = columns

    def get_primary_key_columns(self) -> List[str]: ...
    def get_column_names(self) -> List[str]: ...
    def get_column_by_name(self, name: str) -> Optional[Dict[str, Any]]: ...
```

### 4. Preserve deterministic generation where possible

Given the same inputs (table count, rows per table, schema names), the generator should produce structurally identical output. Where randomness is used (e.g., Faker for test data), the randomness is scoped to data values, not to structure. Table names, column names, procedure signatures, and constraint names are deterministic.

```python
# Deterministic: fixed function signatures
function_defs = self._get_function_definitions(schema, num_functions, tables)
# The same schema + count always yields the same function names and parameters
```

### 5. Fail loudly on invalid generation states

When the generator encounters an inconsistent state -- such as a procedure referencing a table that was not created -- it should raise an error rather than silently producing incomplete or broken SQL. Validation checks (e.g., existence of referenced tables, manager IDs, job IDs) are embedded in generated procedures using `RAISE_APPLICATION_ERROR` with descriptive messages.

```sql
-- Generated procedures validate inputs and fail with meaningful errors
IF l_dept_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Department ID does not exist');
END IF;
```

### 6. Minimize Oracle-version-fragile clauses unless explicitly enabled

Storage clauses (`PCTFREE`, `INITRANS`, `MAXTRANS`, `BUFFER_POOL`, `FLASH_CACHE`) are environment-dependent and may not be appropriate for all Oracle versions or cloud deployments. The `include_storage` parameter controls whether these clauses are emitted, defaulting to `True` for backward compatibility but allowing callers to omit them.

```python
# Storage clauses are optional
def generate(self, table_count: int = 6, include_storage: bool = True, **kwargs):
```

COMPOUND TRIGGERS require Oracle 11g or later. System triggers (`AFTER DDL ON DATABASE`, `AFTER LOGON ON DATABASE`) require DBA privileges. These are generated only when the specific generator is invoked, not as part of the default table generation path.

### 7. Keep generated artifacts importable in standard Oracle tools

All generated SQL must execute successfully in:
- SQL*Plus
- SQLcl
- SQL Developer

This means using standard DDL/DML syntax, proper `/` termination for PL/SQL blocks, and avoiding tool-specific extensions. The `OutputHandler` class writes headers identifying the generation timestamp and character set, and footers confirming completion.

### 8. Make documentation match actual behavior

Comments in generated SQL, table/column comments via `COMMENT ON`, and external documentation must accurately reflect what the generator produces. The `_generate_comments` method in `SchemaGenerator` produces `COMMENT ON TABLE` and `COMMENT ON COLUMN` statements that describe actual columns, not placeholders.

```sql
-- Accurate: describes what the column actually holds
COMMENT ON COLUMN EMPLOYEES.FIRST_NAME_JP IS 'First name in Japanese';

-- Bad: generic or misleading
COMMENT ON COLUMN EMPLOYEES.FIRST_NAME_JP IS 'A column';
```

### 9. BOOLEAN is PL/SQL-only; use VARCHAR2('Y'/'N') for SQL-callable interfaces

Oracle SQL does not support the `BOOLEAN` data type. All SQL-callable procedure and function parameters that represent flags must use `VARCHAR2` with values `'Y'`/`'N'`. Inside PL/SQL blocks, these can be converted to local `BOOLEAN` variables for internal logic.

```sql
-- SQL-callable parameter: VARCHAR2 flag
CREATE OR REPLACE PROCEDURE PURGE_OLD_DATA(
  p_dry_run IN VARCHAR2 DEFAULT 'Y',  -- NOT BOOLEAN
  ...
)
IS
  l_is_dry_run BOOLEAN;
BEGIN
  -- Convert at the boundary
  l_is_dry_run := (UPPER(p_dry_run) IN ('Y', 'YES', 'TRUE', '1'));
  -- Internal logic uses BOOLEAN
  IF l_is_dry_run THEN
    RETURN;
  END IF;
```

This pattern appears consistently in `procedure_generator.py`, `lob_generator.py`, and `package_generator.py`.

### 10. All generated SQL must be verifiable without a live Oracle database

Generated SQL should be reviewable by reading it. A developer should be able to verify correctness of a generated script without connecting to a database. This means:
- No reliance on runtime-only constructs
- All object names are fully qualified or unambiguously resolvable
- Dependencies are declared and ordered correctly
- PL/SQL blocks are syntactically complete

---

## Design Patterns Used

### 1. Factory Pattern -- OracleObjectFactory

`OracleObjectFactory.create_generators()` accepts a list of object type strings and returns the appropriate set of generator instances. This centralizes the decision of which generators to instantiate and in what order.

```python
class OracleObjectFactory:
    @staticmethod
    def create_generators(object_types: List[str]) -> List[OracleObjectGenerator]:
        generators = []
        generators.append(SchemaGenerator())  # Always included
        if 'data' in object_types or 'all' in object_types:
            generators.append(DataGenerator())
        if 'trigger' in object_types or 'all' in object_types:
            generators.append(TriggerGenerator())
        # ... more generators
        return generators
```

**Location:** `core.py`, `OracleObjectFactory.create_generators()`

### 2. Strategy Pattern -- OracleObjectGenerator Interface

Each generator (`SchemaGenerator`, `DataGenerator`, `TriggerGenerator`, `ProcedureGenerator`, `FunctionGenerator`, `PackageGenerator`, `LobGenerator`) implements the `OracleObjectGenerator` abstract base class. They share a common interface (`generate(**kwargs) -> List[OracleObject]`) but differ entirely in their internal logic. The caller can swap generators without changing the calling code.

```python
class OracleObjectGenerator(ABC):
    @abstractmethod
    def generate(self, **kwargs) -> List[OracleObject]:
        pass
```

**Location:** `core.py`, `OracleObjectGenerator` (abstract base)

### 3. Template/Rendering Pattern -- SQL String Interpolation

SQL text is constructed using Python f-strings and string interpolation. Table names, column names, and values are interpolated into SQL templates. While simple, this approach requires careful handling of identifier quoting and value escaping to avoid injection risks in dynamic SQL.

```python
# Template pattern: SQL structure is fixed, names/values are interpolated
index_obj.sql = (
    f"CREATE UNIQUE INDEX {idx_name} ON {table_name}({col['name']})"
    f"{storage_clause};"
)
```

**Location:** All generator modules (`schema_generator.py`, `trigger_generator.py`, etc.)

### 4. Builder Pattern -- TableInfo Construction

`TableInfo` objects are built incrementally. `SchemaGenerator._generate_common_oracle_tables()` produces raw column dictionaries, which are then wrapped into `TableInfo` instances. Additional constraints, indexes, and foreign keys are layered on top via `_generate_indexes_and_constraints()`, `_generate_sequences()`, and `_generate_comments()`.

```python
# Step 1: Generate raw column structures
table_structures = self._generate_common_oracle_tables()

# Step 2: Wrap into TableInfo
for table_structure in tables_to_generate:
    table_name = table_structure[0]['table']
    self.tables.append(TableInfo(table_name, table_structure))

# Step 3: Layer on constraints, sequences, comments
self._generate_indexes_and_constraints(include_storage)
self._generate_sequences()
self._generate_comments()
```

**Location:** `core.py` (`TableInfo`), `schema_generator.py` (`SchemaGenerator`)

### 5. Dependency Injection -- Tables Passed to Generators

Generators that need table information receive `List[TableInfo]` as a parameter rather than looking up tables from a global or singleton. The `main.py` orchestrator extracts tables from the `SchemaGenerator` and passes them to subsequent generators.

```python
# main.py: Tables flow from SchemaGenerator to downstream generators
self.tables = schema_generator.get_tables()

# Downstream generators receive tables as a parameter
objects = generator.generate(tables=self.tables, rows_per_table=args.data_rows)
```

**Location:** `main.py` (`OraSchemaGenApp.generate_objects()`)

### 6. Configuration Object Pattern -- kwargs Propagation

The `generate(**kwargs)` signature on all generators allows optional configuration to flow through the generation chain without requiring every generator to define the same parameter list. Generators extract only the kwargs they care about.

```python
def generate(self, tables: List[TableInfo], num_procedures: int = 3, **kwargs):
    schemas = kwargs.get('schemas', ['HR', 'FINANCE'])
    output_dir = kwargs.get('output_dir')
```

**Location:** All generator `generate()` methods

### 7. Topological Sort -- OutputHandler Dependency Ordering

`OutputHandler._sort_objects_by_dependencies()` performs a topological sort on generated objects to ensure that tables are created before foreign keys, sequences before INSERT statements, and procedures before triggers that call them. The sort uses depth-first search with cycle detection.

```python
def _sort_objects_by_dependencies(self, objects: List[OracleObject]) -> List[OracleObject]:
    # Build dependency graph
    graph = {}
    for obj in objects:
        graph[obj.name] = set(obj.dependencies)

    # Topological sort via DFS
    def visit(node_name):
        if node_name in temp_visited:
            return  # Circular dependency -- skip gracefully
        temp_visited.add(node_name)
        for dep in graph.get(node_name, set()):
            visit(dep)
        visited.add(node_name)
        result.append(obj)
```

**Location:** `core.py`, `OutputHandler._sort_objects_by_dependencies()`

### 8. Conditional Generation -- FK/Constraints Only for Existing Tables

Foreign key constraints and check constraints are only generated for tables that actually exist in the current generation set. The `_generate_indexes_and_constraints` method checks `created_tables` before emitting each constraint.

```python
created_tables = {t.name for t in self.tables}

# FK is only emitted if BOTH tables exist
if 'EMPLOYEES' in created_tables and 'DEPARTMENTS' in created_tables:
    fk_statements.append(
        "ALTER TABLE EMPLOYEES ADD CONSTRAINT EMP_DEPT_FK\n"
        "  FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS (DEPARTMENT_ID)\n"
        "  ENABLE VALIDATE;"
    )
```

**Location:** `schema_generator.py`, `SchemaGenerator._generate_indexes_and_constraints()`

---

## Anti-Patterns to Avoid

### 1. String concatenation without validation (SQL injection risk in dynamic SQL)

When using `EXECUTE IMMEDIATE` with concatenated table or column names, validate inputs against known schemas. Use bind variables for values.

```sql
-- Dangerous: unvalidated concatenation in dynamic SQL
EXECUTE IMMEDIATE 'SELECT * FROM ' || p_table_name;

-- Safer: validate table name against a known list before use
```

The `LobGenerator.APPEND_TO_CLOB` procedure uses dynamic SQL with bind variables for values but concatenates table/column names. In a production system, these should be validated against a whitelist.

### 2. Hardcoded Oracle assumptions

Do not assume specific tablespace names (e.g., `USERS`), schema names, or initialization parameters. The `include_storage` parameter makes storage clauses optional precisely because the `USERS` tablespace may not exist in all environments.

```sql
-- Environment-dependent: assumes USERS tablespace exists
TABLESPACE USERS PCTFREE 10 PCTUSED 40 ...

-- Environment-independent: no tablespace assumption
-- (controlled by include_storage=False)
```

### 3. Hidden NLS dependence

Implicit DATE conversions that rely on `NLS_DATE_FORMAT` are fragile and non-portable. All date literals in generated SQL use `TO_DATE()` with explicit format masks.

```sql
-- Bad: NLS-dependent
INSERT INTO EMPLOYEES (HIRE_DATE) VALUES ('15-MAR-25');

-- Good: Explicit and portable
INSERT INTO EMPLOYEES (HIRE_DATE) VALUES (TO_DATE('2025-03-15', 'YYYY-MM-DD'));
```

### 4. Undocumented object dependency ordering

The output order must respect referential integrity. Tables before foreign keys, sequences before INSERT statements that use `SEQ.NEXTVAL`, packages before procedures that call them. The `OutputHandler` topological sort enforces this, but generators must correctly declare their dependencies via `add_dependency()`.

```python
# Declare what this object depends on
index_obj.add_dependency(table_name)
create_dept_proc.add_dependency("DEPARTMENTS")
create_dept_proc.add_dependency("LOCATIONS")
```

### 5. Examples that are not executable in Oracle

Every SQL snippet in documentation, comments, or generated output must be directly executable in Oracle. Avoid pseudo-SQL, truncated statements, or SQL that requires manual completion.

### 6. Package spec/body signature mismatches

A package body function must have the exact same signature as its declaration in the package spec. The `PackageGenerator` and `FunctionGenerator` avoid this by generating signatures once and reusing them for both spec and body.

```python
# FunctionGenerator: signature generated once, used in both spec and body
for func_name, params_decl, return_type, description, body_impl in function_defs:
    # Spec uses: params_decl, return_type
    # Body uses: params_decl, return_type, body_impl
    # Same params_decl in both places guarantees match
```

### 7. Random parameter generation causing spec/body divergence

The `PackageGenerator` uses `random.sample()` to select tables and `random.choice()` to pick schemas. While the randomness affects *which* tables are used, the function/procedure signatures for a given selection are generated deterministically and shared between spec and body. Be cautious that future randomization does not cause spec and body to diverge.

### 8. BOOLEAN parameters in SQL-callable procedures

Oracle SQL does not support `BOOLEAN` as a data type. Any procedure or function callable from SQL must use `VARCHAR2`, `NUMBER`, or another SQL-compatible type for flag parameters. See Architecture Principle #9.

### 9. Referencing columns that don't exist on target tables

All column references in generated SQL must correspond to actual columns on the target table. The `TableInfo.get_column_by_name()` and `TableInfo.get_column_names()` methods provide a way to validate column existence before generating SQL.

### 10. Foreign keys referencing tables that weren't created

The conditional generation pattern ensures that FK constraints are only emitted when both the referencing and referenced tables exist in the current generation set. See `SchemaGenerator._generate_indexes_and_constraints()`.

### 11. REGEXP_LIKE syntax errors

Missing parentheses or incorrect regex syntax in `REGEXP_LIKE` calls will cause compilation failures. The generated `is_valid_number_*` function in `FunctionGenerator` uses:

```sql
IF REGEXP_LIKE(p_value, '^[0-9]+(\.[0-9]+)?$') THEN
```

Ensure the parentheses balance and the regex is valid Oracle regex syntax (not Python regex).

---

## Code Generation Principles

### 1. Each generator is independent but receives shared TableInfo

Generators do not modify each other's state. The `SchemaGenerator` produces tables and constraints; the `TriggerGenerator` produces triggers; the `ProcedureGenerator` produces procedures. They all receive the same `List[TableInfo]` as input, but no generator modifies the `TableInfo` objects or the output of another generator.

### 2. Generated SQL should be reviewable by humans before execution

Generated SQL includes:
- Section headers (`-- TABLE: EMPLOYEES`, `-- PROCEDURE: HIRE_EMPLOYEE`)
- Descriptive comments for each block
- Generation timestamps in file headers
- Dependency declarations in multi-file output mode

### 3. No generated SQL should depend on environment-specific settings

Generated SQL must not depend on:
- `NLS_DATE_FORMAT` or other NLS settings
- Specific tablespace names (controlled by `include_storage`)
- Specific schema names (tables are created unqualified; callers can wrap in `ALTER SESSION SET CURRENT_SCHEMA`)
- System parameters like `DB_BLOCK_SIZE`

### 4. PL/SQL blocks must be terminated with `/` on a separate line

Every PL/SQL block (procedures, functions, packages, triggers) ends with `/\n` on its own line. This is the standard SQL*Plus/SQLcl convention for executing anonymous or stored PL/SQL blocks.

```sql
END HIRE_EMPLOYEE;
/
```

### 5. DDL and DML ordering must respect referential integrity

The generation pipeline in `main.py` enforces a strict ordering:

1. **Tables** -- SchemaGenerator creates all tables first
2. **Data** -- DataGenerator inserts rows (depends on tables and sequences)
3. **Triggers** -- TriggerGenerator creates triggers (depends on tables)
4. **Procedures** -- ProcedureGenerator creates procedures (depends on tables)
5. **Functions** -- FunctionGenerator creates functions (depends on tables)
6. **Packages** -- PackageGenerator creates packages (depends on tables)
7. **LOBs** -- LobGenerator creates LOB utilities (standalone utilities)

Within each category, the `OutputHandler` topological sort further refines ordering based on declared dependencies.

### 6. Comments and documentation in generated SQL are valuable

Generated SQL includes:
- File-level headers with generation timestamp and character set
- Object-level headers with object type, name, and dependencies
- Inline comments describing what each block does
- Table and column comments via `COMMENT ON` statements

These comments are not decorative -- they enable developers to review generated SQL and understand what it does without executing it.

---

## Oracle Compatibility Principles

### 1. Maximum identifier length is 30 characters (Oracle 12c and earlier)

Oracle 12c Release 1 and earlier limit identifiers (table names, column names, constraint names) to 30 bytes. Oracle 12c Release 2 increased this to 128 bytes, but OraSchemaGen targets the 30-character limit for maximum compatibility.

Constraint names follow the pattern `{TABLE}_{TYPE}_{N}` (e.g., `EMPLOYEES_PK`, `EMP_DEPT_FK`), which stays well under 30 characters for typical table names.

### 2. DATE literals must use TO_DATE() with explicit format mask

Oracle does not have a native SQL DATE literal. All date values must be constructed via `TO_DATE()` or `SYSDATE`.

```python
# data_generator.py: All DATE values use TO_DATE with explicit format
value = f"TO_DATE('{raw_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')"
```

### 3. TIMESTAMP literals must use TO_TIMESTAMP() with explicit format mask

Similarly, TIMESTAMP values require `TO_TIMESTAMP()`.

```python
# data_generator.py: TIMESTAMP values use TO_TIMESTAMP with explicit format
value = f"TO_TIMESTAMP('{raw_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
```

### 4. BOOLEAN cannot be used in SQL-facing procedure parameters

See Architecture Principle #9. `BOOLEAN` is valid only within PL/SQL blocks (procedure bodies, function bodies, trigger bodies). It cannot appear in:
- Procedure/function parameter lists (if called from SQL)
- Table column definitions
- Function return types (if called from SQL)

### 5. Storage clauses are optional and environment-dependent

Storage clauses (`PCTFREE`, `INITRANS`, `MAXTRANS`, `STORAGE(INITIAL ... NEXT ...)` etc.) are tied to specific Oracle storage configurations and may not apply to:
- Oracle Autonomous Database
- Oracle Cloud Infrastructure deployments
- Non-standard tablespace configurations

The `include_storage` parameter controls their inclusion.

### 6. System triggers require DBA privileges

Triggers on `AFTER DDL ON DATABASE` and `AFTER LOGON ON DATABASE` require the `ADMINISTER DATABASE TRIGGER` privilege (effectively DBA-level). These are generated by `TriggerGenerator._generate_system_event_triggers()` but should only be deployed in environments where the executing user has appropriate privileges.

```sql
-- Requires DBA privileges
CREATE OR REPLACE TRIGGER AUDIT_DDL_TRG
AFTER DDL ON DATABASE
```

### 7. COMPOUND TRIGGERS require Oracle 11g or later

COMPOUND TRIGGERS (with `BEFORE STATEMENT`, `BEFORE EACH ROW`, `AFTER EACH ROW`, `AFTER STATEMENT` sections) were introduced in Oracle 11g. The `EMPLOYEES_SALARY_CHK_TRG` in `trigger_generator.py` uses this syntax.

### 8. SYS.ODCIVARCHAR2LIST and SYSTEM.ODCIVARCHAR2LIST require appropriate schema access

The `LobGenerator.EXTRACT_KEYWORDS` function uses `SYS.ODCIVARCHAR2LIST` as a collection type. This type is available in the `SYS` schema by default but may require explicit grants in locked-down environments.

```sql
l_exclude_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
```

### 9. DBMS_OUTPUT.PUT_LINE output is invisible unless SERVEROUTPUT is ON

Many generated procedures use `DBMS_OUTPUT.PUT_LINE` for logging and status messages. This output is only visible when the client has `SET SERVEROUTPUT ON` (SQL*Plus/SQLcl) or the equivalent setting enabled. Developers executing generated scripts should be aware of this requirement.

```sql
-- In SQL*Plus or SQLcl:
SET SERVEROUTPUT ON SIZE UNLIMITED

-- Then execute the script
@generated_script.sql
```

---

## Summary of Key Files

| File | Role |
|------|------|
| `core.py` | Base classes (`OracleObject`, `OracleObjectGenerator`, `TableInfo`), `OracleObjectFactory`, `OutputHandler` with topological sort |
| `schema_generator.py` | Table DDL, constraints (PK, FK, unique, check), indexes, sequences, comments |
| `data_generator.py` | Sample DML with Faker-generated data, locale-aware (English/Japanese) |
| `trigger_generator.py` | Row-level, statement-level, and compound triggers; system event triggers |
| `procedure_generator.py` | CRUD and business logic procedures with input validation |
| `function_generator.py` | Utility functions in package form with matching spec/body signatures |
| `package_generator.py` | PL/SQL packages with functions and procedures, spec/body generation |
| `lob_generator.py` | CLOB/BLOB utilities, document storage, text analysis functions |
| `main.py` | CLI entry point, orchestration, progress reporting |
