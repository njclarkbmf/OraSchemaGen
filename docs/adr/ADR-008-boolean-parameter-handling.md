# ADR-008: BOOLEAN Parameter Handling in Generated SQL

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

Oracle has a long-standing quirk: the **BOOLEAN data type exists in PL/SQL but not in SQL**. This means:

- PL/SQL procedures and functions can declare BOOLEAN parameters internally
- BOOLEAN cannot be used in SQL statements (SELECT, INSERT, UPDATE, EXEC)
- Calling a PL/SQL function with a BOOLEAN parameter from SQL results in `ORA-00902: invalid datatype`
- This limitation has persisted through Oracle 19c and remains in 21c/23c

This creates a problem for generated procedures and functions: if they use BOOLEAN parameters, they cannot be called directly from SQL statements, only from other PL/SQL blocks.

## Decision

All procedure and function parameters that cross the SQL/PL/SQL boundary **must use `VARCHAR2('Y'/'N')` instead of BOOLEAN**.

Specific rules:
- Public-facing parameters (in procedure/function signatures) use `VARCHAR2` with `'Y'` for true and `'N'` for false
- Internal PL/SQL logic within procedure/function bodies **may use BOOLEAN** for clarity
- A conversion layer at the procedure entry point maps `VARCHAR2` to `BOOLEAN` if needed internally
- This ensures all generated procedures are **SQL-callable** (can be invoked from SELECT, EXEC, etc.)

Example:
```sql
-- SQL-callable signature
CREATE OR REPLACE PROCEDURE update_employee(
    p_employee_id IN NUMBER,
    p_is_active   IN VARCHAR2  -- 'Y' or 'N', not BOOLEAN
) IS
    v_is_active BOOLEAN;
BEGIN
    -- Convert to BOOLEAN for internal use
    v_is_active := (p_is_active = 'Y');

    -- Internal logic using BOOLEAN
    IF v_is_active THEN
        -- ...
    END IF;
END;
/
```

Not:
```sql
-- NOT SQL-callable
CREATE OR REPLACE PROCEDURE update_employee(
    p_employee_id IN NUMBER,
    p_is_active   IN BOOLEAN   -- Cannot be called from SQL
) IS
BEGIN
    -- ...
END;
/
```

## Consequences

### Positive
- **All generated procedures are SQL-callable**: can be invoked via `EXEC`, `CALL`, or in SQL expressions
- Compatible with JDBC, ODBC, and all database connectivity layers
- No `ORA-00902` errors when calling procedures from SQL context
- Clear convention: `'Y'` = true, `'N'` = false
- Internal PL/SQL logic retains BOOLEAN type safety
- Works across all Oracle versions (this is a fundamental Oracle limitation)

### Negative
- Slightly more verbose parameter declarations (`VARCHAR2` vs `BOOLEAN`)
- Requires conversion logic at procedure entry points
- `'Y'/'N'` convention may be less intuitive than `TRUE/FALSE` for non-Oracle developers
- NULL handling requires additional consideration (NULL is neither 'Y' nor 'N')

## Alternatives Considered

### Use NUMBER(1) with 0/1 for boolean values
**Rejected.** `'Y'/'N'` is more readable and self-documenting. `0/1` requires interpretation and is prone to errors (is 0 true or false?). `'Y'/'N'` is the conventional Oracle pattern.

### Use BOOLEAN and document the PL/SQL-only limitation
**Rejected.** This would make generated procedures unusable from SQL, which is a common invocation method. The tool should produce SQL-callable code by default.

### Generate overloaded versions (one with BOOLEAN for PL/SQL, one with VARCHAR2 for SQL)
**Rejected.** While technically possible, this doubles the generated code and creates maintenance complexity. The VARCHAR2 approach serves both use cases adequately.
