#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
trigger_generator.py - Trigger generation for OraSchemaGen

This module provides the TriggerGenerator class which creates Oracle database
triggers of various types including row-level triggers, statement-level triggers,
and compound triggers.

Author: John Clark Naldoza
"""

import random
from typing import List, Dict, Any
from core import OracleObjectGenerator, OracleObject, TableInfo

class TriggerGenerator(OracleObjectGenerator):
    """
    Generates Oracle trigger objects
    """
    def __init__(self):
        super().__init__()
        
    def generate(self, tables: List[TableInfo], **kwargs) -> List[OracleObject]:
        """Generate Oracle triggers for tables"""
        # Generate various types of triggers for the tables
        for table_info in tables:
            table_name = table_info.name
            
            # Skip ORDER_ITEMS table for triggers as it's a child table
            if table_name == 'ORDER_ITEMS':
                continue
                
            # Generate different types of triggers based on table
            if table_name == 'EMPLOYEES':
                self._generate_employee_triggers(table_info)
            elif table_name == 'DEPARTMENTS':
                self._generate_department_triggers(table_info)
            elif table_name == 'ORDERS':
                self._generate_order_triggers(table_info)
            elif table_name == 'PRODUCTS':
                self._generate_product_triggers(table_info)
            elif table_name == 'CUSTOMERS':
                self._generate_customer_triggers(table_info)
        
        # Generate system event triggers
        self._generate_system_event_triggers()
        
        return self.objects
        
    def _generate_employee_triggers(self, table_info: TableInfo) -> None:
        """Generate triggers for the EMPLOYEES table"""
        # Compound trigger for salary validation and audit
        compound_trigger = OracleObject("EMPLOYEES_SALARY_CHK_TRG", "TRIGGER")
        compound_trigger.sql = f"""-- Compound trigger to validate salary changes and maintain audit history
CREATE OR REPLACE TRIGGER EMPLOYEES_SALARY_CHK_TRG
FOR UPDATE OF SALARY ON EMPLOYEES
COMPOUND TRIGGER
  -- Declaration section
  TYPE salary_change_rec IS RECORD (
    employee_id EMPLOYEES.EMPLOYEE_ID%TYPE,
    old_salary EMPLOYEES.SALARY%TYPE,
    new_salary EMPLOYEES.SALARY%TYPE,
    change_pct NUMBER,
    changed_by VARCHAR2(30)
  );
  
  -- Collection to hold changes
  TYPE salary_changes_tab IS TABLE OF salary_change_rec INDEX BY PLS_INTEGER;
  l_changes salary_changes_tab;
  l_idx PLS_INTEGER := 0;
  
  -- Constants
  MAX_SALARY_INCREASE CONSTANT NUMBER := 20; -- Maximum 20% increase allowed
  
  -- Before statement section
  BEFORE STATEMENT IS
  BEGIN
    -- Initialize collection
    l_changes.DELETE;
    l_idx := 0;
  END BEFORE STATEMENT;

  -- Before each row section
  BEFORE EACH ROW IS
  BEGIN
    -- Check if salary has increased too much
    IF :NEW.SALARY > :OLD.SALARY THEN
      -- Calculate percentage increase
      IF :OLD.SALARY > 0 THEN
        IF ((:NEW.SALARY - :OLD.SALARY) / :OLD.SALARY) * 100 > MAX_SALARY_INCREASE THEN
          RAISE_APPLICATION_ERROR(-20001, 
            'Salary increase exceeds maximum allowed percentage of ' || 
            MAX_SALARY_INCREASE || '%. Contact HR for approval.');
        END IF;
      END IF;
      
      -- Store change info in collection
      l_idx := l_idx + 1;
      l_changes(l_idx).employee_id := :OLD.EMPLOYEE_ID;
      l_changes(l_idx).old_salary := :OLD.SALARY;
      l_changes(l_idx).new_salary := :NEW.SALARY;
      l_changes(l_idx).change_pct := CASE 
                                       WHEN :OLD.SALARY > 0 
                                       THEN ROUND(((:NEW.SALARY - :OLD.SALARY) / :OLD.SALARY) * 100, 2)
                                       ELSE NULL 
                                     END;
      l_changes(l_idx).changed_by := SYS_CONTEXT('USERENV', 'SESSION_USER');
    END IF;
  END BEFORE EACH ROW;

  -- After statement section
  AFTER STATEMENT IS
  BEGIN
    -- Log salary changes to audit table
    FOR i IN 1..l_changes.COUNT LOOP
      -- For simplicity, we're just printing what would be inserted
      -- In real implementation, this would insert to an audit table
      DBMS_OUTPUT.PUT_LINE(
        'AUDIT: Employee ' || l_changes(i).employee_id || 
        ' salary changed from ' || l_changes(i).old_salary || 
        ' to ' || l_changes(i).new_salary || 
        ' (' || l_changes(i).change_pct || '%)' ||
        ' by ' || l_changes(i).changed_by
      );
    END LOOP;
  END AFTER STATEMENT;
END EMPLOYEES_SALARY_CHK_TRG;
/"""
        compound_trigger.add_dependency("EMPLOYEES")
        self.objects.append(compound_trigger)
        
        # Row-level trigger for employee email standardization
        email_trigger = OracleObject("EMPLOYEES_EMAIL_TRG", "TRIGGER")
        email_trigger.sql = f"""-- Row-level trigger to standardize email format
CREATE OR REPLACE TRIGGER EMPLOYEES_EMAIL_TRG
BEFORE INSERT OR UPDATE OF EMAIL ON EMPLOYEES
FOR EACH ROW
BEGIN
  -- Standardize email format - lowercase and trim
  IF :NEW.EMAIL IS NOT NULL THEN
    :NEW.EMAIL := LOWER(TRIM(:NEW.EMAIL));
    
    -- Ensure email has company domain if missing
    IF INSTR(:NEW.EMAIL, '@') = 0 THEN
      :NEW.EMAIL := :NEW.EMAIL || '@example.com';
    END IF;
  END IF;
END;
/"""
        email_trigger.add_dependency("EMPLOYEES")
        self.objects.append(email_trigger)
        
    def _generate_department_triggers(self, table_info: TableInfo) -> None:
        """Generate triggers for the DEPARTMENTS table"""
        # Before insert/update trigger for departments
        dept_trigger = OracleObject("DEPARTMENTS_BIU_TRG", "TRIGGER")
        dept_trigger.sql = f"""-- Trigger to enforce department name uniqueness and standardization
CREATE OR REPLACE TRIGGER DEPARTMENTS_BIU_TRG
BEFORE INSERT OR UPDATE ON DEPARTMENTS
FOR EACH ROW
DECLARE
  l_dept_count NUMBER;
  l_standard_name VARCHAR2(30);
BEGIN
  -- Standardize department name
  l_standard_name := INITCAP(TRIM(:NEW.DEPARTMENT_NAME));
  :NEW.DEPARTMENT_NAME := l_standard_name;
  
  -- Check uniqueness of department name (case insensitive)
  SELECT COUNT(*)
  INTO l_dept_count
  FROM DEPARTMENTS
  WHERE UPPER(DEPARTMENT_NAME) = UPPER(l_standard_name)
  AND DEPARTMENT_ID != NVL(:NEW.DEPARTMENT_ID, -999);
  
  IF l_dept_count > 0 THEN
    RAISE_APPLICATION_ERROR(-20002, 
      'Department name "' || l_standard_name || '" already exists. Department names must be unique.');
  END IF;
  
  -- Auto-generate Japanese department name if not provided
  IF :NEW.DEPARTMENT_NAME_JP IS NULL AND :NEW.DEPARTMENT_NAME IS NOT NULL THEN
    -- In a real system, this would use a translation service or lookup table
    -- For this example, we'll just append "部門" to the English name
    :NEW.DEPARTMENT_NAME_JP := :NEW.DEPARTMENT_NAME || ' 部門';
  END IF;
  
  -- Record last modification timestamp
  :NEW.MODIFIED_DATE := SYSDATE;
END;
/"""
        dept_trigger.add_dependency("DEPARTMENTS")
        self.objects.append(dept_trigger)
        
        # Statement-level audit trigger
        audit_trigger = OracleObject("DEPARTMENTS_AUDIT_TRG", "TRIGGER")
        audit_trigger.sql = f"""-- Statement-level trigger for auditing department changes
CREATE OR REPLACE TRIGGER DEPARTMENTS_AUDIT_TRG
AFTER INSERT OR UPDATE OR DELETE ON DEPARTMENTS
DECLARE
  l_operation VARCHAR2(10);
  l_user VARCHAR2(30);
  l_time TIMESTAMP;
BEGIN
  -- Determine operation type
  CASE
    WHEN INSERTING THEN l_operation := 'INSERT';
    WHEN UPDATING THEN l_operation := 'UPDATE';
    WHEN DELETING THEN l_operation := 'DELETE';
  END CASE;
  
  -- Get current user and time
  l_user := SYS_CONTEXT('USERENV', 'SESSION_USER');
  l_time := SYSTIMESTAMP;
  
  -- Log audit information (to output for this example)
  DBMS_OUTPUT.PUT_LINE(
    'AUDIT: ' || l_operation || ' operation on DEPARTMENTS at ' || 
    TO_CHAR(l_time, 'YYYY-MM-DD HH24:MI:SS.FF') || 
    ' by user ' || l_user
  );
  
  -- In a real system, this would insert into an audit table:
  -- INSERT INTO AUDIT_LOG (TABLE_NAME, OPERATION, USER_NAME, LOG_TIME)
  -- VALUES ('DEPARTMENTS', l_operation, l_user, l_time);
END;
/"""
        audit_trigger.add_dependency("DEPARTMENTS")
        self.objects.append(audit_trigger)
        
    def _generate_order_triggers(self, table_info: TableInfo) -> None:
        """Generate triggers for the ORDERS table"""
        # Trigger to calculate order total from items
        order_total_trigger = OracleObject("ORDERS_UPD_TOTAL_TRG", "TRIGGER")
        order_total_trigger.sql = f"""-- Trigger to update order total when items change
CREATE OR REPLACE TRIGGER ORDERS_UPD_TOTAL_TRG
AFTER INSERT OR UPDATE OR DELETE ON ORDER_ITEMS
FOR EACH ROW
DECLARE
  l_order_id ORDER_ITEMS.ORDER_ID%TYPE;
BEGIN
  -- Determine the affected order
  IF INSERTING OR UPDATING THEN
    l_order_id := :NEW.ORDER_ID;
  ELSE -- DELETING
    l_order_id := :OLD.ORDER_ID;
  END IF;
  
  -- Recalculate order total
  UPDATE ORDERS
  SET ORDER_TOTAL = (
    SELECT NVL(SUM(UNIT_PRICE * QUANTITY * (1 - NVL(DISCOUNT_PERCENT, 0)/100)), 0)
    FROM ORDER_ITEMS
    WHERE ORDER_ID = l_order_id
  )
  WHERE ORDER_ID = l_order_id;
END;
/"""
        order_total_trigger.add_dependency("ORDERS")
        order_total_trigger.add_dependency("ORDER_ITEMS")
        self.objects.append(order_total_trigger)
        
        # Status change validation trigger
        status_trigger = OracleObject("ORDERS_STATUS_CHK_TRG", "TRIGGER")
        status_trigger.sql = f"""-- Trigger to validate order status changes
CREATE OR REPLACE TRIGGER ORDERS_STATUS_CHK_TRG
BEFORE UPDATE OF STATUS ON ORDERS
FOR EACH ROW
DECLARE
  TYPE status_transition_t IS TABLE OF VARCHAR2(20) INDEX BY VARCHAR2(20);
  l_valid_transitions status_transition_t;
BEGIN
  -- Define valid status transitions
  l_valid_transitions('PENDING') := 'PROCESSING,CANCELLED';
  l_valid_transitions('PROCESSING') := 'SHIPPED,CANCELLED';
  l_valid_transitions('SHIPPED') := 'DELIVERED,RETURNED';
  l_valid_transitions('DELIVERED') := 'RETURNED,COMPLETED';
  l_valid_transitions('RETURNED') := 'REFUNDED,RESTOCKED';
  l_valid_transitions('CANCELLED') := '';
  l_valid_transitions('COMPLETED') := '';
  l_valid_transitions('REFUNDED') := '';
  l_valid_transitions('RESTOCKED') := '';
  
  -- Check if new status is different
  IF :NEW.STATUS != :OLD.STATUS THEN
    -- Verify the transition is valid
    IF :OLD.STATUS IS NOT NULL AND 
       l_valid_transitions.EXISTS(:OLD.STATUS) AND
       INSTR(',' || l_valid_transitions(:OLD.STATUS) || ',', ',' || :NEW.STATUS || ',') = 0 
    THEN
      RAISE_APPLICATION_ERROR(-20003, 
        'Invalid status transition from "' || :OLD.STATUS || '" to "' || :NEW.STATUS || '". ' ||
        'Valid next statuses are: ' || NVL(l_valid_transitions(:OLD.STATUS), 'NONE'));
    END IF;
    
    -- Auto-update shipping date when status changes to SHIPPED
    IF :NEW.STATUS = 'SHIPPED' AND :OLD.STATUS != 'SHIPPED' THEN
      :NEW.SHIPPING_DATE := SYSDATE;
    END IF;
  END IF;
END;
/"""
        status_trigger.add_dependency("ORDERS")
        self.objects.append(status_trigger)
        
    def _generate_product_triggers(self, table_info: TableInfo) -> None:
        """Generate triggers for the PRODUCTS table"""
        # Price change notification trigger
        price_trigger = OracleObject("PRODUCTS_PRICE_TRG", "TRIGGER")
        price_trigger.sql = f"""-- Trigger to track product price changes and notify key customers
CREATE OR REPLACE TRIGGER PRODUCTS_PRICE_TRG
AFTER UPDATE OF LIST_PRICE ON PRODUCTS
FOR EACH ROW
WHEN (OLD.LIST_PRICE != NEW.LIST_PRICE)
DECLARE
  l_price_diff NUMBER;
  l_change_pct NUMBER;
  l_change_type VARCHAR2(10);
BEGIN
  -- Calculate price difference and percentage
  l_price_diff := :NEW.LIST_PRICE - :OLD.LIST_PRICE;
  
  -- Avoid division by zero
  IF :OLD.LIST_PRICE > 0 THEN
    l_change_pct := ABS(ROUND((l_price_diff / :OLD.LIST_PRICE) * 100, 2));
  ELSE
    l_change_pct := 100; -- Consider as 100% if old price was 0
  END IF;
  
  -- Determine change type
  IF l_price_diff > 0 THEN
    l_change_type := 'increase';
  ELSE
    l_change_type := 'decrease';
  END IF;
  
  -- Log the price change
  DBMS_OUTPUT.PUT_LINE(
    'PRICE CHANGE: Product ' || :NEW.PRODUCT_NAME || ' (ID: ' || :NEW.PRODUCT_ID || ') ' ||
    'price ' || l_change_type || ' from ' || :OLD.LIST_PRICE || ' to ' || :NEW.LIST_PRICE || ' (' || 
    l_change_pct || '% ' || l_change_type || ')'
  );
  
  -- For significant price changes (over 10%), notify key customers
  -- In a real system, this would call a procedure to send notifications
  IF l_change_pct > 10 THEN
    DBMS_OUTPUT.PUT_LINE(
      'NOTIFICATION: Sending alerts to key customers about ' || l_change_pct || '% price ' || 
      l_change_type || ' for product ' || :NEW.PRODUCT_NAME
    );
    
    -- Example of what would happen in a real system:
    -- NOTIFY_KEY_CUSTOMERS(
    --   p_product_id => :NEW.PRODUCT_ID,
    --   p_old_price => :OLD.LIST_PRICE,
    --   p_new_price => :NEW.LIST_PRICE,
    --   p_change_pct => l_change_pct,
    --   p_change_type => l_change_type
    -- );
  END IF;
  
  -- Update the modified date
  :NEW.MODIFIED_DATE := SYSDATE;
END;
/"""
        price_trigger.add_dependency("PRODUCTS")
        self.objects.append(price_trigger)
        
    def _generate_customer_triggers(self, table_info: TableInfo) -> None:
        """Generate triggers for the CUSTOMERS table"""
        # Customer data normalization trigger
        customer_trigger = OracleObject("CUSTOMERS_NORM_TRG", "TRIGGER")
        customer_trigger.sql = f"""-- Trigger to normalize customer data
CREATE OR REPLACE TRIGGER CUSTOMERS_NORM_TRG
BEFORE INSERT OR UPDATE ON CUSTOMERS
FOR EACH ROW
BEGIN
  -- Normalize email address (lowercase)
  IF :NEW.EMAIL IS NOT NULL THEN
    :NEW.EMAIL := LOWER(TRIM(:NEW.EMAIL));
  END IF;
  
  -- Normalize phone number format
  IF :NEW.PHONE IS NOT NULL THEN
    -- Remove any non-digit characters except +
    :NEW.PHONE := REGEXP_REPLACE(:NEW.PHONE, '[^0-9+]', '');
    
    -- Ensure proper format for Japanese numbers if they start with +81
    IF SUBSTR(:NEW.PHONE, 1, 3) = '+81' THEN
      -- Format as +81-XX-XXXX-XXXX (standard Japanese format)
      :NEW.PHONE := REGEXP_REPLACE(:NEW.PHONE, '\\+81([0-9]{2})([0-9]{4})([0-9]{4})', '+81-\\1-\\2-\\3');
    END IF;
  END IF;
  
  -- Normalize postal code format
  IF :NEW.POSTAL_CODE IS NOT NULL THEN
    -- For Japanese postal codes (NNN-NNNN format)
    IF :NEW.COUNTRY = 'Japan' OR :NEW.COUNTRY_JP = '日本' THEN
      -- Remove any non-digit characters first
      :NEW.POSTAL_CODE := REGEXP_REPLACE(:NEW.POSTAL_CODE, '[^0-9]', '');
      
      -- Format as NNN-NNNN if 7 digits
      IF LENGTH(:NEW.POSTAL_CODE) = 7 THEN
        :NEW.POSTAL_CODE := SUBSTR(:NEW.POSTAL_CODE, 1, 3) || '-' || SUBSTR(:NEW.POSTAL_CODE, 4);
      END IF;
    END IF;
  END IF;
  
  -- Set registration date for new customers
  IF INSERTING AND :NEW.REGISTRATION_DATE IS NULL THEN
    :NEW.REGISTRATION_DATE := SYSDATE;
  END IF;
END;
/"""
        customer_trigger.add_dependency("CUSTOMERS")
        self.objects.append(customer_trigger)
        
    def _generate_system_event_triggers(self) -> None:
        """Generate system event triggers"""
        # DDL audit trigger
        ddl_trigger = OracleObject("AUDIT_DDL_TRG", "TRIGGER")
        ddl_trigger.sql = f"""-- System trigger to audit DDL operations
CREATE OR REPLACE TRIGGER AUDIT_DDL_TRG
AFTER DDL ON DATABASE
DECLARE
  l_event_type VARCHAR2(30) := SYS.SYSEVENT;
  l_object_type VARCHAR2(30) := SYS.DICTIONARY_OBJ_TYPE;
  l_object_name VARCHAR2(30) := SYS.DICTIONARY_OBJ_NAME;
  l_object_owner VARCHAR2(30) := SYS.DICTIONARY_OBJ_OWNER;
  l_user VARCHAR2(30) := SYS.LOGIN_USER;
  l_timestamp TIMESTAMP := SYSTIMESTAMP;
BEGIN
  -- Log DDL event (to output for this example)
  DBMS_OUTPUT.PUT_LINE(
    'DDL AUDIT: ' || l_event_type || ' operation on ' || 
    l_object_type || ' ' || l_object_owner || '.' || l_object_name || 
    ' at ' || TO_CHAR(l_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF') || 
    ' by user ' || l_user
  );
  
  -- In a real system, this would insert into an audit table:
  -- INSERT INTO DDL_AUDIT_LOG (
  --   EVENT_TYPE, OBJECT_TYPE, OBJECT_NAME, OBJECT_OWNER, 
  --   EVENT_TIME, USERNAME, SQL_TEXT
  -- ) VALUES (
  --   l_event_type, l_object_type, l_object_name, l_object_owner,
  --   l_timestamp, l_user, SYS.SQL_TEXT
  -- );
END AUDIT_DDL_TRG;
/"""
        self.objects.append(ddl_trigger)
        
        # Logon trigger for security monitoring
        logon_trigger = OracleObject("MONITOR_LOGON_TRG", "TRIGGER")
        logon_trigger.sql = f"""-- System trigger to monitor database logon events
CREATE OR REPLACE TRIGGER MONITOR_LOGON_TRG
AFTER LOGON ON DATABASE
DECLARE
  l_user VARCHAR2(30) := SYS_CONTEXT('USERENV', 'SESSION_USER');
  l_ip VARCHAR2(15) := SYS_CONTEXT('USERENV', 'IP_ADDRESS');
  l_timestamp TIMESTAMP := SYSTIMESTAMP;
  l_os_user VARCHAR2(30) := SYS_CONTEXT('USERENV', 'OS_USER');
  l_machine VARCHAR2(64) := SYS_CONTEXT('USERENV', 'HOST');
  l_program VARCHAR2(64) := SYS_CONTEXT('USERENV', 'MODULE');
BEGIN
  -- Skip monitoring for system accounts
  IF l_user IN ('SYS', 'SYSTEM', 'DBSNMP') THEN
    RETURN;
  END IF;
  
  -- Log connection information (to output for this example)
  DBMS_OUTPUT.PUT_LINE(
    'LOGON: User ' || l_user || ' connected from ' || l_ip || 
    ' (' || l_machine || ') at ' || 
    TO_CHAR(l_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF') || 
    ' using ' || l_program
  );
  
  -- In a real system, this would insert into a security log table:
  -- INSERT INTO SECURITY_LOGON_LOG (
  --   DB_USER, IP_ADDRESS, OS_USER, MACHINE, PROGRAM, LOGON_TIME
  -- ) VALUES (
  --   l_user, l_ip, l_os_user, l_machine, l_program, l_timestamp
  -- );
END MONITOR_LOGON_TRG;
/"""
        self.objects.append(logon_trigger)
