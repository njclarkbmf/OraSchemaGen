# ADR-005: Identifier Naming Strategy

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

Oracle identifiers (table names, column names, constraint names, sequence names, etc.) have specific rules and limitations. Oracle 12c and earlier enforce a **30-character maximum** for identifiers (increased to 128 in 12.2 with quoted identifiers, but 30 remains the safe portable limit). Identifier naming conventions directly impact readability, maintainability, and tooling compatibility.

Inconsistent naming leads to confusion, makes generated scripts harder to review, and can cause conflicts with existing database objects.

## Decision

All generated identifiers follow **UPPERCASE with underscores** (e.g., `EMPLOYEE_ID`, `EMP_DEPT_FK`). Specific naming conventions:

- **Tables**: Plural noun, uppercase (e.g., `EMPLOYEES`, `DEPARTMENTS`)
- **Columns**: `TABLE_COLUMN` pattern (e.g., `EMPLOYEE_ID`, `DEPARTMENT_NAME`)
- **Primary key constraints**: `TABLENAME_PK` (e.g., `EMPLOYEES_PK`)
- **Foreign key constraints**: `SRC_REF_FK` (e.g., `EMP_DEPT_FK` for employees.department_id -> departments.department_id)
- **Unique constraints**: `TABLENAME_UK` or `TABLE_COLUMN_UK` (e.g., `EMPLOYEES_EMAIL_UK`)
- **Check constraints**: `TABLE_COLUMN_CHK` (e.g., `EMPLOYEES_SALARY_CHK`)
- **Indexes**: `TABLE_COLUMN_IDX` (e.g., `EMPLOYEES_LAST_NAME_IDX`)
- **Sequences**: `TABLENAME_SEQ` (e.g., `EMPLOYEES_SEQ`)
- **Triggers**: `TABLENAME_TRG` or `TABLE_EVENT_TRG` (e.g., `EMPLOYEES_BI_TRG`)
- **Procedures/Functions**: Descriptive verb-noun (e.g., `CALCULATE_SALARY`, `GET_EMPLOYEE`)

All identifiers are limited to **30 characters maximum** for Oracle 12c compatibility.

## Consequences

### Positive
- Consistent, predictable naming across all generated objects
- Easy to identify object type from its name alone
- 30-character limit ensures compatibility with all supported Oracle versions
- Standard pattern familiar to Oracle DBAs and developers
- Constraints are self-documenting (name reveals the table and type)
- Uppercase matches Oracle's default storage format for identifiers

### Negative
- 30-character limit requires abbreviation for long table/column names
- Some FK constraint names may need aggressive shortening for long table names
- Uppercase convention may conflict with existing lowercase schema conventions
- Abbreviation can reduce readability (e.g., `EMPLOYEE_ADDRESS_LINE1` becomes `EMP_ADDR_L1`)

## Alternatives Considered

### Lowercase identifiers
**Rejected.** Oracle stores unquoted identifiers as uppercase. Using lowercase would require quoted identifiers everywhere, which is error-prone and non-standard in the Oracle ecosystem.

### CamelCase identifiers
**Rejected.** Not idiomatic for Oracle databases. The Oracle community standard is uppercase with underscores. CamelCase also has case-sensitivity issues with quoted identifiers.

### Use Oracle's longer identifier limit (128 chars, 12.2+)
**Rejected.** While Oracle 12.2+ supports longer identifiers with quoted identifiers, this breaks compatibility with Oracle 12cR1 and earlier. The 30-character limit is the safe, portable choice.

### System-generated constraint names (SYS_Cnnnnnnn)
**Rejected.** System-generated names are opaque and make debugging difficult. Named constraints are essential for maintenance, error interpretation, and schema comparison.
