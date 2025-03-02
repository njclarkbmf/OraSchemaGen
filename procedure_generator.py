#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
procedure_generator.py - Procedure generation for OraSchemaGen

This module provides the ProcedureGenerator class which creates Oracle database
stored procedures for common database operations and business logic.

Author: John Clark Naldoza
"""

from typing import List, Dict, Any
from core import OracleObjectGenerator, OracleObject, TableInfo

class ProcedureGenerator(OracleObjectGenerator):
    """
    Generates Oracle stored procedure objects
    """
    def __init__(self):
        super().__init__()
        
    def generate(self, tables: List[TableInfo], num_procedures: int = 3, **kwargs) -> List[OracleObject]:
        """Generate Oracle procedures for tables and common operations"""
        # Generate table-specific procedures
        for table_info in tables:
            table_name = table_info.name
            
            # Generate appropriate procedures based on table
            if table_name == 'EMPLOYEES':
                self._generate_employee_procedures(table_info)
            elif table_name == 'DEPARTMENTS':
                self._generate_department_procedures(table_info)
            elif table_name == 'ORDERS':
                self._generate_order_procedures(table_info)
        
        # Generate utility procedures
        self._generate_utility_procedures()
        
        # Generate data validation procedures
        self._generate_validation_procedures()
        
        return self.objects
        
    def _generate_department_procedures(self, table_info: TableInfo) -> None:
        """Generate procedures for the DEPARTMENTS table"""
        # Procedure to create a new department
        create_dept_proc = OracleObject("CREATE_DEPARTMENT", "PROCEDURE")
        create_dept_proc.sql = """-- Procedure to create a new department
CREATE OR REPLACE PROCEDURE CREATE_DEPARTMENT(
  p_department_name IN VARCHAR2,
  p_department_name_jp IN VARCHAR2 DEFAULT NULL,
  p_manager_id IN NUMBER DEFAULT NULL,
  p_location_id IN NUMBER,
  p_department_id OUT NUMBER
)
IS
  l_dept_exists NUMBER;
  l_manager_exists NUMBER := 1;  -- Default if no manager specified
  l_location_exists NUMBER;
BEGIN
  -- Input validation
  -- Check if department name already exists
  SELECT COUNT(*) INTO l_dept_exists
  FROM DEPARTMENTS
  WHERE UPPER(DEPARTMENT_NAME) = UPPER(p_department_name);
  
  IF l_dept_exists > 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Department name already exists');
  END IF;
  
  -- Check if location exists
  SELECT COUNT(*) INTO l_location_exists
  FROM LOCATIONS
  WHERE LOCATION_ID = p_location_id;
  
  IF l_location_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20002, 'Location ID does not exist');
  END IF;
  
  -- Check if manager exists (if provided)
  IF p_manager_id IS NOT NULL THEN
    SELECT COUNT(*) INTO l_manager_exists
    FROM EMPLOYEES
    WHERE EMPLOYEE_ID = p_manager_id;
    
    IF l_manager_exists = 0 THEN
      RAISE_APPLICATION_ERROR(-20003, 'Manager ID does not exist');
    END IF;
  END IF;
  
  -- Insert the new department
  INSERT INTO DEPARTMENTS (
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    DEPARTMENT_NAME_JP,
    MANAGER_ID,
    LOCATION_ID
  ) VALUES (
    DEPARTMENTS_SEQ.NEXTVAL,
    p_department_name,
    p_department_name_jp,
    p_manager_id,
    p_location_id
  ) RETURNING DEPARTMENT_ID INTO p_department_id;
  
  COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('New department created: ' || p_department_name || 
                      ' (ID: ' || p_department_id || ')');
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back any changes
    ROLLBACK;
    
    -- Re-raise the error
    RAISE;
END CREATE_DEPARTMENT;
/"""
        create_dept_proc.add_dependency("DEPARTMENTS")
        create_dept_proc.add_dependency("LOCATIONS")
        create_dept_proc.add_dependency("EMPLOYEES")
        self.objects.append(create_dept_proc)
        
        # Procedure to relocate a department
        relocate_proc = OracleObject("RELOCATE_DEPARTMENT", "PROCEDURE")
        relocate_proc.sql = """-- Procedure to relocate a department
CREATE OR REPLACE PROCEDURE RELOCATE_DEPARTMENT(
  p_department_id IN NUMBER,
  p_new_location_id IN NUMBER,
  p_effective_date IN DATE DEFAULT SYSDATE,
  p_notify_employees IN BOOLEAN DEFAULT TRUE
)
IS
  l_dept_exists NUMBER;
  l_location_exists NUMBER;
  l_dept_name VARCHAR2(30);
  l_old_location_id NUMBER;
  l_old_location_city VARCHAR2(30);
  l_new_location_city VARCHAR2(30);
  l_employee_count NUMBER;
BEGIN
  -- Input validation
  -- Check if department exists
  SELECT COUNT(*), DEPARTMENT_NAME, LOCATION_ID
  INTO l_dept_exists, l_dept_name, l_old_location_id
  FROM DEPARTMENTS
  WHERE DEPARTMENT_ID = p_department_id;
  
  IF l_dept_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Department ID does not exist');
  END IF;
  
  -- No change needed if same location
  IF l_old_location_id = p_new_location_id THEN
    RAISE_APPLICATION_ERROR(-20002, 'Department is already at this location');
  END IF;
  
  -- Check if new location exists
  SELECT COUNT(*), CITY
  INTO l_location_exists, l_new_location_city
  FROM LOCATIONS
  WHERE LOCATION_ID = p_new_location_id;
  
  IF l_location_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20003, 'Location ID does not exist');
  END IF;
  
  -- Get old location city
  SELECT CITY INTO l_old_location_city
  FROM LOCATIONS
  WHERE LOCATION_ID = l_old_location_id;
  
  -- Update the department
  UPDATE DEPARTMENTS
  SET LOCATION_ID = p_new_location_id
  WHERE DEPARTMENT_ID = p_department_id;
  
  -- Log the relocation
  DBMS_OUTPUT.PUT_LINE('Department ' || l_dept_name || ' (ID: ' || p_department_id || 
                      ') relocated from ' || l_old_location_city || 
                      ' to ' || l_new_location_city ||
                      ' effective ' || TO_CHAR(p_effective_date, 'YYYY-MM-DD'));
  
  -- Notify employees if requested
  IF p_notify_employees THEN
    -- Count affected employees
    SELECT COUNT(*) INTO l_employee_count
    FROM EMPLOYEES
    WHERE DEPARTMENT_ID = p_department_id;
    
    DBMS_OUTPUT.PUT_LINE('Notification: ' || l_employee_count || 
                         ' employees will be notified about the relocation of department ' ||
                         l_dept_name || ' from ' || l_old_location_city || 
                         ' to ' || l_new_location_city);
    
    -- In a real system, this would call a notification procedure
    -- NOTIFY_DEPARTMENT_EMPLOYEES(
    --   p_department_id => p_department_id,
    --   p_message => 'Your department is relocating to ' || l_new_location_city || 
    --               ' effective ' || TO_CHAR(p_effective_date, 'YYYY-MM-DD')
    -- );
  END IF;
  
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back any changes
    ROLLBACK;
    
    -- Re-raise the error
    RAISE;
END RELOCATE_DEPARTMENT;
/"""
        relocate_proc.add_dependency("DEPARTMENTS")
        relocate_proc.add_dependency("LOCATIONS")
        relocate_proc.add_dependency("EMPLOYEES")
        self.objects.append(relocate_proc)
    
    def _generate_employee_procedures(self, table_info: TableInfo) -> None:
        """Generate procedures for the EMPLOYEES table"""
        # Procedure to hire a new employee
        hire_proc = OracleObject("HIRE_EMPLOYEE", "PROCEDURE")
        hire_proc.sql = """-- Procedure to hire a new employee
CREATE OR REPLACE PROCEDURE HIRE_EMPLOYEE(
  p_first_name IN VARCHAR2,
  p_last_name IN VARCHAR2,
  p_first_name_jp IN VARCHAR2 DEFAULT NULL,
  p_last_name_jp IN VARCHAR2 DEFAULT NULL,
  p_email IN VARCHAR2,
  p_phone IN VARCHAR2,
  p_hire_date IN DATE DEFAULT SYSDATE,
  p_job_id IN VARCHAR2,
  p_salary IN NUMBER,
  p_commission_pct IN NUMBER DEFAULT NULL,
  p_manager_id IN NUMBER DEFAULT NULL,
  p_department_id IN NUMBER,
  p_employee_id OUT NUMBER
)
IS
  l_email_count NUMBER;
  l_dept_exists NUMBER;
  l_job_exists NUMBER;
  l_manager_exists NUMBER := 0;
  l_min_salary NUMBER;
  l_max_salary NUMBER;
BEGIN
  -- Input validation
  -- Check if email already exists
  SELECT COUNT(*) INTO l_email_count
  FROM EMPLOYEES
  WHERE UPPER(EMAIL) = UPPER(p_email);
  
  IF l_email_count > 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Email address already exists');
  END IF;
  
  -- Check if department exists
  SELECT COUNT(*) INTO l_dept_exists
  FROM DEPARTMENTS
  WHERE DEPARTMENT_ID = p_department_id;
  
  IF l_dept_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20002, 'Department ID does not exist');
  END IF;
  
  -- Check if job exists and salary is within range
  SELECT COUNT(*), MIN_SALARY, MAX_SALARY 
  INTO l_job_exists, l_min_salary, l_max_salary
  FROM JOBS
  WHERE JOB_ID = p_job_id;
  
  IF l_job_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20003, 'Job ID does not exist');
  END IF;
  
  IF p_salary < l_min_salary OR p_salary > l_max_salary THEN
    RAISE_APPLICATION_ERROR(-20004, 
      'Salary ' || p_salary || ' is outside the valid range for this job (' || 
      l_min_salary || ' - ' || l_max_salary || ')');
  END IF;
  
  -- Check if manager exists (if provided)
  IF p_manager_id IS NOT NULL THEN
    SELECT COUNT(*) INTO l_manager_exists
    FROM EMPLOYEES
    WHERE EMPLOYEE_ID = p_manager_id;
    
    IF l_manager_exists = 0 THEN
      RAISE_APPLICATION_ERROR(-20005, 'Manager ID does not exist');
    END IF;
  END IF;
  
  -- Insert the new employee
  INSERT INTO EMPLOYEES (
    EMPLOYEE_ID,
    FIRST_NAME,
    LAST_NAME,
    FIRST_NAME_JP,
    LAST_NAME_JP,
    EMAIL,
    PHONE_NUMBER,
    HIRE_DATE,
    JOB_ID,
    SALARY,
    COMMISSION_PCT,
    MANAGER_ID,
    DEPARTMENT_ID
  ) VALUES (
    EMPLOYEES_SEQ.NEXTVAL,
    p_first_name,
    p_last_name,
    p_first_name_jp,
    p_last_name_jp,
    UPPER(p_email),
    p_phone,
    p_hire_date,
    p_job_id,
    p_salary,
    p_commission_pct,
    p_manager_id,
    p_department_id
  ) RETURNING EMPLOYEE_ID INTO p_employee_id;
  
  COMMIT;
  
  -- Log the hire
  DBMS_OUTPUT.PUT_LINE('New employee hired: ' || p_first_name || ' ' || p_last_name || 
                      ' (ID: ' || p_employee_id || ') in department ' || p_department_id);
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back any changes
    ROLLBACK;
    
    -- Re-raise the error
    RAISE;
END HIRE_EMPLOYEE;
/"""
        hire_proc.add_dependency("EMPLOYEES")
        hire_proc.add_dependency("DEPARTMENTS")
        hire_proc.add_dependency("JOBS")
        self.objects.append(hire_proc)
        
        # Procedure to transfer an employee
        transfer_proc = OracleObject("TRANSFER_EMPLOYEE", "PROCEDURE")
        transfer_proc.sql = """-- Procedure to transfer an employee to a different department
CREATE OR REPLACE PROCEDURE TRANSFER_EMPLOYEE(
  p_employee_id IN NUMBER,
  p_new_department_id IN NUMBER,
  p_new_job_id IN VARCHAR2 DEFAULT NULL,
  p_new_salary IN NUMBER DEFAULT NULL,
  p_new_manager_id IN NUMBER DEFAULT NULL,
  p_effective_date IN DATE DEFAULT SYSDATE,
  p_reason IN VARCHAR2 DEFAULT NULL
)
IS
  l_emp_exists NUMBER;
  l_dept_exists NUMBER;
  l_job_exists NUMBER := 1;  -- Default if no job change
  l_manager_exists NUMBER := 1;  -- Default if no manager change
  l_old_dept_id NUMBER;
  l_old_job_id VARCHAR2(10);
  l_old_salary NUMBER;
  l_old_manager_id NUMBER;
  l_min_salary NUMBER;
  l_max_salary NUMBER;
BEGIN
  -- Input validation
  -- Check if employee exists
  SELECT COUNT(*), DEPARTMENT_ID, JOB_ID, SALARY, MANAGER_ID
  INTO l_emp_exists, l_old_dept_id, l_old_job_id, l_old_salary, l_old_manager_id
  FROM EMPLOYEES
  WHERE EMPLOYEE_ID = p_employee_id;
  
  IF l_emp_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Employee ID ' || p_employee_id || ' does not exist');
  END IF;
  
  -- If trying to transfer to the same department with no other changes
  IF p_new_department_id = l_old_dept_id 
     AND p_new_job_id IS NULL 
     AND p_new_salary IS NULL 
     AND p_new_manager_id IS NULL THEN
    RAISE_APPLICATION_ERROR(-20002, 'No changes specified for transfer');
  END IF;
  
  -- Check if department exists
  SELECT COUNT(*) INTO l_dept_exists
  FROM DEPARTMENTS
  WHERE DEPARTMENT_ID = p_new_department_id;
  
  IF l_dept_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20003, 'Department ID ' || p_new_department_id || ' does not exist');
  END IF;
  
  -- Check if job exists and salary is within range (if job is changing)
  IF p_new_job_id IS NOT NULL THEN
    SELECT COUNT(*), MIN_SALARY, MAX_SALARY 
    INTO l_job_exists, l_min_salary, l_max_salary
    FROM JOBS
    WHERE JOB_ID = p_new_job_id;
    
    IF l_job_exists = 0 THEN
      RAISE_APPLICATION_ERROR(-20004, 'Job ID ' || p_new_job_id || ' does not exist');
    END IF;
    
    -- Validate new salary if provided, otherwise use existing salary
    IF p_new_salary IS NOT NULL THEN
      IF p_new_salary < l_min_salary OR p_new_salary > l_max_salary THEN
        RAISE_APPLICATION_ERROR(-20005, 
          'Salary ' || p_new_salary || ' is outside the valid range for this job (' || 
          l_min_salary || ' - ' || l_max_salary || ')');
      END IF;
    END IF;
  END IF;
  
  -- Check if manager exists (if provided)
  IF p_new_manager_id IS NOT NULL THEN
    -- Prevent circular management (employee can't be their own manager)
    IF p_new_manager_id = p_employee_id THEN
      RAISE_APPLICATION_ERROR(-20006, 'An employee cannot be their own manager');
    END IF;
    
    SELECT COUNT(*) INTO l_manager_exists
    FROM EMPLOYEES
    WHERE EMPLOYEE_ID = p_new_manager_id;
    
    IF l_manager_exists = 0 THEN
      RAISE_APPLICATION_ERROR(-20007, 'Manager ID ' || p_new_manager_id || ' does not exist');
    END IF;
  END IF;
  
  -- Create a record of the transfer (in a real system, this would go to a history table)
  DBMS_OUTPUT.PUT_LINE('TRANSFER RECORD: Employee ' || p_employee_id || 
                       ' transferred from department ' || l_old_dept_id || 
                       ' to department ' || p_new_department_id ||
                       ' effective ' || TO_CHAR(p_effective_date, 'YYYY-MM-DD') ||
                       ' Reason: ' || NVL(p_reason, 'Not specified'));
  
  -- Update the employee record
  UPDATE EMPLOYEES
  SET DEPARTMENT_ID = p_new_department_id,
      JOB_ID = NVL(p_new_job_id, JOB_ID),
      SALARY = NVL(p_new_salary, SALARY),
      MANAGER_ID = NVL(p_new_manager_id, MANAGER_ID)
  WHERE EMPLOYEE_ID = p_employee_id;
  
  COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('Employee ' || p_employee_id || ' successfully transferred');
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back any changes
    ROLLBACK;
    
    -- Re-raise the error
    RAISE;
END TRANSFER_EMPLOYEE;
/"""
        transfer_proc.add_dependency("EMPLOYEES")
        transfer_proc.add_dependency("DEPARTMENTS")
        transfer_proc.add_dependency("JOBS")
        self.objects.append(transfer_proc)
        
    def _generate_order_procedures(self, table_info: TableInfo) -> None:
        """Generate procedures for the ORDERS table"""
        # Procedure to create a new order
        create_order_proc = OracleObject("CREATE_ORDER", "PROCEDURE")
        create_order_proc.sql = """-- Procedure to create a new order
CREATE OR REPLACE PROCEDURE CREATE_ORDER(
  p_customer_id IN NUMBER,
  p_salesperson_id IN NUMBER DEFAULT NULL,
  p_shipping_address IN VARCHAR2,
  p_shipping_address_jp IN VARCHAR2 DEFAULT NULL,
  p_shipping_city IN VARCHAR2,
  p_shipping_city_jp IN VARCHAR2 DEFAULT NULL,
  p_shipping_state IN VARCHAR2,
  p_shipping_zip IN VARCHAR2,
  p_shipping_country IN VARCHAR2 DEFAULT 'USA',
  p_payment_method IN VARCHAR2 DEFAULT 'CREDIT',
  p_notes IN CLOB DEFAULT NULL,
  p_notes_jp IN CLOB DEFAULT NULL,
  p_order_id OUT NUMBER
)
IS
  l_customer_exists NUMBER;
  l_salesperson_exists NUMBER := 1;  -- Default if no salesperson
BEGIN
  -- Input validation
  -- Check if customer exists
  SELECT COUNT(*) INTO l_customer_exists
  FROM CUSTOMERS
  WHERE CUSTOMER_ID = p_customer_id;
  
  IF l_customer_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Customer ID does not exist');
  END IF;
  
  -- Check if salesperson exists (if provided)
  IF p_salesperson_id IS NOT NULL THEN
    SELECT COUNT(*) INTO l_salesperson_exists
    FROM EMPLOYEES
    WHERE EMPLOYEE_ID = p_salesperson_id
    AND JOB_ID = 'SA_REP';  -- Sales Representative
    
    IF l_salesperson_exists = 0 THEN
      RAISE_APPLICATION_ERROR(-20002, 'Salesperson ID does not exist or is not a sales representative');
    END IF;
  END IF;
  
  -- Create the order
  INSERT INTO ORDERS (
    ORDER_ID,
    CUSTOMER_ID,
    STATUS,
    SALESPERSON_ID,
    ORDER_DATE,
    SHIPPING_ADDRESS,
    SHIPPING_ADDRESS_JP,
    SHIPPING_CITY,
    SHIPPING_CITY_JP,
    SHIPPING_STATE,
    SHIPPING_ZIP,
    SHIPPING_COUNTRY,
    PAYMENT_METHOD,
    ORDER_TOTAL,
    NOTES,
    NOTES_JP
  ) VALUES (
    ORDERS_SEQ.NEXTVAL,
    p_customer_id,
    'PENDING',  -- Initial status
    p_salesperson_id,
    SYSDATE,
    p_shipping_address,
    p_shipping_address_jp,
    p_shipping_city,
    p_shipping_city_jp,
    p_shipping_state,
    p_shipping_zip,
    p_shipping_country,
    p_payment_method,
    0,  -- Initial total, will be updated when items are added
    p_notes,
    p_notes_jp
  ) RETURNING ORDER_ID INTO p_order_id;
  
  COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('New order created: Order ID ' || p_order_id || 
                       ' for Customer ' || p_customer_id);
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back any changes
    ROLLBACK;
    
    -- Re-raise the error
    RAISE;
END CREATE_ORDER;
/"""
        create_order_proc.add_dependency("ORDERS")
        create_order_proc.add_dependency("CUSTOMERS")
        create_order_proc.add_dependency("EMPLOYEES")
        self.objects.append(create_order_proc)
        
        # Procedure to add items to an order
        add_item_proc = OracleObject("ADD_ORDER_ITEM", "PROCEDURE")
        add_item_proc.sql = """-- Procedure to add an item to an order
CREATE OR REPLACE PROCEDURE ADD_ORDER_ITEM(
  p_order_id IN NUMBER,
  p_product_id IN NUMBER,
  p_quantity IN NUMBER,
  p_unit_price IN NUMBER DEFAULT NULL,  -- If NULL, use product's list price
  p_discount_percent IN NUMBER DEFAULT 0,
  p_notes IN VARCHAR2 DEFAULT NULL,
  p_notes_jp IN VARCHAR2 DEFAULT NULL
)
IS
  l_order_exists NUMBER;
  l_order_status VARCHAR2(20);
  l_product_exists NUMBER;
  l_product_price NUMBER;
  l_actual_price NUMBER;
  l_line_total NUMBER;
BEGIN
  -- Input validation
  -- Check if order exists and is in appropriate status
  SELECT COUNT(*), STATUS
  INTO l_order_exists, l_order_status
  FROM ORDERS
  WHERE ORDER_ID = p_order_id;
  
  IF l_order_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Order ID does not exist');
  END IF;
  
  -- Only allow adding items to orders in PENDING or PROCESSING status
  IF l_order_status NOT IN ('PENDING', 'PROCESSING') THEN
    RAISE_APPLICATION_ERROR(-20002, 
      'Cannot add items to order in ' || l_order_status || ' status');
  END IF;
  
  -- Check if product exists and get its price
  SELECT COUNT(*), LIST_PRICE
  INTO l_product_exists, l_product_price
  FROM PRODUCTS
  WHERE PRODUCT_ID = p_product_id;
  
  IF l_product_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20003, 'Product ID does not exist');
  END IF;
  
  -- Validate quantity
  IF p_quantity <= 0 THEN
    RAISE_APPLICATION_ERROR(-20004, 'Quantity must be greater than zero');
  END IF;
  
  -- Determine the unit price to use
  l_actual_price := NVL(p_unit_price, l_product_price);
  
  -- Calculate line total
  l_line_total := l_actual_price * p_quantity * (1 - NVL(p_discount_percent, 0)/100);
  
  -- Check if this product is already in the order
  DECLARE
    l_item_exists NUMBER;
  BEGIN
    SELECT COUNT(*)
    INTO l_item_exists
    FROM ORDER_ITEMS
    WHERE ORDER_ID = p_order_id
    AND PRODUCT_ID = p_product_id;
    
    IF l_item_exists > 0 THEN
      -- Update existing item
      UPDATE ORDER_ITEMS
      SET QUANTITY = QUANTITY + p_quantity,
          UNIT_PRICE = l_actual_price,
          DISCOUNT_PERCENT = p_discount_percent,
          LINE_TOTAL = LINE_TOTAL + l_line_total,
          NOTES = NVL(p_notes, NOTES),
          NOTES_JP = NVL(p_notes_jp, NOTES_JP)
      WHERE ORDER_ID = p_order_id
      AND PRODUCT_ID = p_product_id;
      
      DBMS_OUTPUT.PUT_LINE('Updated quantity of product ' || p_product_id || 
                         ' in order ' || p_order_id);
    ELSE
      -- Add new item
      INSERT INTO ORDER_ITEMS (
        ORDER_ID,
        PRODUCT_ID,
        UNIT_PRICE,
        QUANTITY,
        DISCOUNT_PERCENT,
        LINE_TOTAL,
        NOTES,
        NOTES_JP
      ) VALUES (
        p_order_id,
        p_product_id,
        l_actual_price,
        p_quantity,
        p_discount_percent,
        l_line_total,
        p_notes,
        p_notes_jp
      );
      
      DBMS_OUTPUT.PUT_LINE('Added product ' || p_product_id || 
                         ' to order ' || p_order_id);
    END IF;
  END;
  
  -- The order total will be automatically updated by a trigger
  
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back any changes
    ROLLBACK;
    
    -- Re-raise the error
    RAISE;
END ADD_ORDER_ITEM;
/"""
        add_item_proc.add_dependency("ORDERS")
        add_item_proc.add_dependency("ORDER_ITEMS")
        add_item_proc.add_dependency("PRODUCTS")
        self.objects.append(add_item_proc)
        
    def _generate_utility_procedures(self) -> None:
        """Generate utility procedures"""
        # Procedure to purge old data
        purge_proc = OracleObject("PURGE_OLD_DATA", "PROCEDURE")
        purge_proc.sql = """-- Procedure to purge old data
CREATE OR REPLACE PROCEDURE PURGE_OLD_DATA(
  p_months_old IN NUMBER DEFAULT 36,
  p_batch_size IN NUMBER DEFAULT 1000,
  p_dry_run IN BOOLEAN DEFAULT TRUE,
  p_rows_deleted OUT NUMBER
)
IS
  l_cutoff_date DATE := ADD_MONTHS(SYSDATE, -p_months_old);
  l_orders_deleted NUMBER := 0;
  l_items_deleted NUMBER := 0;
  l_continue BOOLEAN := TRUE;
  l_batch_count NUMBER;
BEGIN
  p_rows_deleted := 0;
  
  DBMS_OUTPUT.PUT_LINE('Purge operation started at ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
  DBMS_OUTPUT.PUT_LINE('Cutoff date: ' || TO_CHAR(l_cutoff_date, 'YYYY-MM-DD'));
  
  IF p_dry_run THEN
    DBMS_OUTPUT.PUT_LINE('DRY RUN MODE - No data will be deleted');
  END IF;
  
  -- Count and report rows that would be deleted
  SELECT COUNT(*)
  INTO l_batch_count
  FROM ORDERS
  WHERE ORDER_DATE < l_cutoff_date
  AND STATUS IN ('COMPLETED', 'CANCELLED', 'RETURNED', 'REFUNDED');
  
  DBMS_OUTPUT.PUT_LINE('Orders eligible for deletion: ' || l_batch_count);
  
  SELECT COUNT(*)
  INTO l_batch_count
  FROM ORDER_ITEMS
  WHERE ORDER_ID IN (
    SELECT ORDER_ID
    FROM ORDERS
    WHERE ORDER_DATE < l_cutoff_date
    AND STATUS IN ('COMPLETED', 'CANCELLED', 'RETURNED', 'REFUNDED')
  );
  
  DBMS_OUTPUT.PUT_LINE('Order items eligible for deletion: ' || l_batch_count);
  
  -- Exit if dry run
  IF p_dry_run THEN
    RETURN;
  END IF;
  
  -- Process in batches to avoid excessive locking
  WHILE l_continue LOOP
    -- Delete order items first (child records)
    DELETE FROM ORDER_ITEMS
    WHERE ORDER_ID IN (
      SELECT ORDER_ID
      FROM ORDERS
      WHERE ORDER_DATE < l_cutoff_date
      AND STATUS IN ('COMPLETED', 'CANCELLED', 'RETURNED', 'REFUNDED')
      AND ROWNUM <= p_batch_size
    );
    
    l_batch_count := SQL%ROWCOUNT;
    l_items_deleted := l_items_deleted + l_batch_count;
    
    -- Exit if no more items to delete
    IF l_batch_count = 0 THEN
      l_continue := FALSE;
    ELSE
      -- Delete the orders
      DELETE FROM ORDERS
      WHERE ORDER_ID IN (
        SELECT ORDER_ID
        FROM ORDERS
        WHERE ORDER_DATE < l_cutoff_date
        AND STATUS IN ('COMPLETED', 'CANCELLED', 'RETURNED', 'REFUNDED')
        AND ROWNUM <= p_batch_size
      );
      
      l_batch_count := SQL%ROWCOUNT;
      l_orders_deleted := l_orders_deleted + l_batch_count;
      
      -- Commit after each batch
      COMMIT;
      
      DBMS_OUTPUT.PUT_LINE('Deleted batch: ' || l_batch_count || ' orders');
    END IF;
  END LOOP;
  
  p_rows_deleted := l_orders_deleted + l_items_deleted;
  
  DBMS_OUTPUT.PUT_LINE('Purge operation completed at ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
  DBMS_OUTPUT.PUT_LINE('Total deleted: ' || l_orders_deleted || ' orders and ' || 
                       l_items_deleted || ' order items');
  DBMS_OUTPUT.PUT_LINE('Total rows affected: ' || p_rows_deleted);
  
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back any changes
    ROLLBACK;
    
    -- Re-raise the error
    RAISE;
END PURGE_OLD_DATA;
/"""
        purge_proc.add_dependency("ORDERS")
        purge_proc.add_dependency("ORDER_ITEMS")
        self.objects.append(purge_proc)
        
        # Generate test data procedure
        test_data_proc = OracleObject("GENERATE_TEST_DATA", "PROCEDURE")
        test_data_proc.sql = """-- Procedure to generate test data
CREATE OR REPLACE PROCEDURE GENERATE_TEST_DATA(
  p_employees IN NUMBER DEFAULT 10,
  p_customers IN NUMBER DEFAULT 50,
  p_orders_per_customer IN NUMBER DEFAULT 3,
  p_max_items_per_order IN NUMBER DEFAULT 5
)
IS
  l_order_id NUMBER;
  l_max_product_id NUMBER;
  l_product_count NUMBER;
  l_items_per_order NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('Starting test data generation at ' || 
                       TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
  
  -- Get product count for random selection
  SELECT MAX(PRODUCT_ID), COUNT(*)
  INTO l_max_product_id, l_product_count
  FROM PRODUCTS;
  
  IF l_product_count = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'No products available for test data generation');
  END IF;
  
  -- Generate employees if needed
  DECLARE
    l_employee_count NUMBER;
    l_first_names VARCHAR2(500) := 'John,Jane,Michael,Emily,David,Sarah,Robert,Lisa,William,Mary,James,Patricia,Thomas,Jennifer,Charles,Linda';
    l_last_names VARCHAR2(500) := 'Smith,Johnson,Williams,Jones,Brown,Davis,Miller,Wilson,Moore,Taylor,Anderson,Thomas,Jackson,White,Harris,Martin';
    l_domains VARCHAR2(200) := 'example.com,testmail.org,fakecorp.net,mailtest.co';
    l_job_ids VARCHAR2(100) := 'IT_PROG,SA_REP,ST_CLERK,AD_ASST,MK_REP,HR_REP,PR_REP,AC_MGR';
    
    TYPE name_array IS TABLE OF VARCHAR2(50);
    l_first_name_array name_array;
    l_last_name_array name_array;
    l_domains_array name_array;
    l_job_array name_array;
    
    l_first_name VARCHAR2(50);
    l_last_name VARCHAR2(50);
    l_email VARCHAR2(50);
    l_job_id VARCHAR2(10);
    l_dept_id NUMBER;
    l_salary NUMBER;
    l_hire_date DATE;
  BEGIN
    -- Check how many employees exist
    SELECT COUNT(*) INTO l_employee_count FROM EMPLOYEES;
    
    -- Split the CSV strings into arrays
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_first_name_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_first_names, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_first_names, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_last_name_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_last_names, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_last_names, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_domains_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_domains, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_domains, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_job_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_job_ids, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_job_ids, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    -- Add employees if needed
    IF l_employee_count < p_employees THEN
      DBMS_OUTPUT.PUT_LINE('Generating ' || (p_employees - l_employee_count) || ' employees...');
      
      FOR i IN 1..(p_employees - l_employee_count) LOOP
        -- Generate random data
        l_first_name := l_first_name_array(TRUNC(DBMS_RANDOM.VALUE(1, l_first_name_array.COUNT + 1)));
        l_last_name := l_last_name_array(TRUNC(DBMS_RANDOM.VALUE(1, l_last_name_array.COUNT + 1)));
        l_email := UPPER(SUBSTR(l_first_name, 1, 1) || l_last_name) || '@' ||
                   l_domains_array(TRUNC(DBMS_RANDOM.VALUE(1, l_domains_array.COUNT + 1)));
        l_job_id := l_job_array(TRUNC(DBMS_RANDOM.VALUE(1, l_job_array.COUNT + 1)));
        l_dept_id := TRUNC(DBMS_RANDOM.VALUE(10, 110) / 10) * 10; -- 10, 20, ..., 100
        l_salary := TRUNC(DBMS_RANDOM.VALUE(3000, 15000));
        l_hire_date := TO_DATE('2015-01-01', 'YYYY-MM-DD') + 
                       TRUNC(DBMS_RANDOM.VALUE(0, (SYSDATE - TO_DATE('2015-01-01', 'YYYY-MM-DD'))));
        
        -- Insert employee
        INSERT INTO EMPLOYEES (
          EMPLOYEE_ID,
          FIRST_NAME,
          LAST_NAME,
          EMAIL,
          PHONE_NUMBER,
          HIRE_DATE,
          JOB_ID,
          SALARY,
          DEPARTMENT_ID
        ) VALUES (
          EMPLOYEES_SEQ.NEXTVAL,
          l_first_name,
          l_last_name,
          l_email,
          '555-' || TO_CHAR(TRUNC(DBMS_RANDOM.VALUE(100, 1000)), 'FM000') || '-' || 
          TO_CHAR(TRUNC(DBMS_RANDOM.VALUE(1000, 10000)), 'FM0000'),
          l_hire_date,
          l_job_id,
          l_salary,
          l_dept_id
        );
        
        IF MOD(i, 10) = 0 THEN
          COMMIT;
        END IF;
      END LOOP;
      
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Generated employees: ' || (p_employees - l_employee_count));
    END IF;
  END;
  
  -- Generate customers if needed
  DECLARE
    l_customer_count NUMBER;
    l_first_names VARCHAR2(500) := 'James,Mary,John,Patricia,Robert,Jennifer,Michael,Linda,William,Elizabeth,David,Barbara,Richard,Susan,Joseph,Jessica';
    l_last_names VARCHAR2(500) := 'Smith,Johnson,Williams,Brown,Jones,Garcia,Miller,Davis,Rodriguez,Martinez,Hernandez,Lopez,Gonzalez,Wilson,Anderson,Thomas';
    l_domains VARCHAR2(200) := 'gmail.com,yahoo.com,hotmail.com,aol.com,outlook.com,icloud.com,protonmail.com';
    l_cities VARCHAR2(300) := 'New York,Los Angeles,Chicago,Houston,Phoenix,Philadelphia,San Antonio,San Diego,Dallas,San Jose,Austin,Jacksonville,Fort Worth,Columbus,Charlotte';
    l_states VARCHAR2(200) := 'NY,CA,IL,TX,AZ,PA,TX,CA,TX,CA,TX,FL,TX,OH,NC';
    
    TYPE name_array IS TABLE OF VARCHAR2(50);
    l_first_name_array name_array;
    l_last_name_array name_array;
    l_domains_array name_array;
    l_cities_array name_array;
    l_states_array name_array;
    
    l_first_name VARCHAR2(50);
    l_last_name VARCHAR2(50);
    l_email VARCHAR2(50);
    l_city VARCHAR2(30);
    l_state VARCHAR2(2);
    l_credit_limit NUMBER;
  BEGIN
    -- Check how many customers exist
    SELECT COUNT(*) INTO l_customer_count FROM CUSTOMERS;
    
    -- Split the CSV strings into arrays
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_first_name_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_first_names, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_first_names, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_last_name_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_last_names, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_last_names, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_domains_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_domains, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_domains, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_cities_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_cities, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_cities, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_states_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_states, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_states, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    -- Add customers if needed
    IF l_customer_count < p_customers THEN
      DBMS_OUTPUT.PUT_LINE('Generating ' || (p_customers - l_customer_count) || ' customers...');
      
      FOR i IN 1..(p_customers - l_customer_count) LOOP
        -- Generate random data
        l_first_name := l_first_name_array(TRUNC(DBMS_RANDOM.VALUE(1, l_first_name_array.COUNT + 1)));
        l_last_name := l_last_name_array(TRUNC(DBMS_RANDOM.VALUE(1, l_last_name_array.COUNT + 1)));
        l_email := LOWER(l_first_name || '.' || l_last_name || TRUNC(DBMS_RANDOM.VALUE(1, 1000))) || '@' ||
                   l_domains_array(TRUNC(DBMS_RANDOM.VALUE(1, l_domains_array.COUNT + 1)));
        l_city := l_cities_array(TRUNC(DBMS_RANDOM.VALUE(1, l_cities_array.COUNT + 1)));
        l_state := l_states_array(TRUNC(DBMS_RANDOM.VALUE(1, l_states_array.COUNT + 1)));
        l_credit_limit := TRUNC(DBMS_RANDOM.VALUE(1000, 10000) / 100) * 100; -- Round to nearest hundred
        
        -- Insert customer
        INSERT INTO CUSTOMERS (
          CUSTOMER_ID,
          FIRST_NAME,
          LAST_NAME,
          EMAIL,
          PHONE,
          ADDRESS,
          CITY,
          STATE,
          POSTAL_CODE,
          COUNTRY,
          CREDIT_LIMIT,
          REGISTRATION_DATE
        ) VALUES (
          CUSTOMERS_SEQ.NEXTVAL,
          l_first_name,
          l_last_name,
          l_email,
          '555-' || TO_CHAR(TRUNC(DBMS_RANDOM.VALUE(100, 1000)), 'FM000') || '-' || 
          TO_CHAR(TRUNC(DBMS_RANDOM.VALUE(1000, 10000)), 'FM0000'),
          TRUNC(DBMS_RANDOM.VALUE(100, 10000)) || ' ' || 
          CASE TRUNC(DBMS_RANDOM.VALUE(1, 5))
            WHEN 1 THEN 'Main'
            WHEN 2 THEN 'Oak'
            WHEN 3 THEN 'Pine'
            WHEN 4 THEN 'Maple'
            ELSE 'Cedar'
          END || ' ' ||
          CASE TRUNC(DBMS_RANDOM.VALUE(1, 4))
            WHEN 1 THEN 'St'
            WHEN 2 THEN 'Ave'
            WHEN 3 THEN 'Rd'
            ELSE 'Blvd'
          END,
          l_city,
          l_state,
          TO_CHAR(TRUNC(DBMS_RANDOM.VALUE(10000, 100000)), 'FM00000'),
          'USA',
          l_credit_limit,
          TO_DATE('2018-01-01', 'YYYY-MM-DD') + 
          TRUNC(DBMS_RANDOM.VALUE(0, (SYSDATE - TO_DATE('2018-01-01', 'YYYY-MM-DD'))))
        );
        
        IF MOD(i, 10) = 0 THEN
          COMMIT;
        END IF;
      END LOOP;
      
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Generated customers: ' || (p_customers - l_customer_count));
    END IF;
  END;
  
  -- Generate orders
  DECLARE
    CURSOR customer_cur IS
      SELECT CUSTOMER_ID FROM CUSTOMERS;
    
    CURSOR employee_cur IS
      SELECT EMPLOYEE_ID FROM EMPLOYEES
      WHERE JOB_ID = 'SA_REP'
      OR JOB_ID LIKE 'SA%';
    
    TYPE employee_array IS TABLE OF EMPLOYEES.EMPLOYEE_ID%TYPE;
    l_employees employee_array;
    
    l_salesperson NUMBER;
    l_order_date DATE;
    l_status VARCHAR2(20);
    l_payment_methods VARCHAR2(100) := 'CREDIT,DEBIT,BANK_TRANSFER,PAYPAL,CASH';
    l_statuses VARCHAR2(200) := 'PENDING,PROCESSING,SHIPPED,DELIVERED,COMPLETED,CANCELLED,RETURNED,REFUNDED';
    
    TYPE string_array IS TABLE OF VARCHAR2(50);
    l_payment_array string_array;
    l_status_array string_array;
  BEGIN
    -- Load employees into array
    SELECT EMPLOYEE_ID BULK COLLECT INTO l_employees
    FROM EMPLOYEES
    WHERE JOB_ID = 'SA_REP'
    OR JOB_ID LIKE 'SA%';
    
    -- Default if no sales reps
    IF l_employees.COUNT = 0 THEN
      SELECT EMPLOYEE_ID BULK COLLECT INTO l_employees
      FROM EMPLOYEES
      WHERE ROWNUM <= 5;
    END IF;
    
    -- Split the CSV strings into arrays
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_payment_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_payment_methods, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_payment_methods, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    SELECT TRIM(COLUMN_VALUE) BULK COLLECT INTO l_status_array
    FROM TABLE(CAST(MULTISET(
      SELECT REGEXP_SUBSTR(l_statuses, '[^,]+', 1, LEVEL)
      FROM DUAL
      CONNECT BY REGEXP_SUBSTR(l_statuses, '[^,]+', 1, LEVEL) IS NOT NULL
    ) AS SYSTEM.ODCIVARCHAR2LIST));
    
    -- Generate orders for customers
    DBMS_OUTPUT.PUT_LINE('Generating orders...');
    
    FOR cust_rec IN customer_cur LOOP
      -- Generate random number of orders for this customer
      FOR i IN 1..TRUNC(DBMS_RANDOM.VALUE(1, p_orders_per_customer + 1)) LOOP
        -- Pick random salesperson
        l_salesperson := l_employees(TRUNC(DBMS_RANDOM.VALUE(1, l_employees.COUNT + 1)));
        
        -- Generate order date within last 2 years
        l_order_date := TO_DATE(SYSDATE - TRUNC(DBMS_RANDOM.VALUE(0, 730)));
        
        -- Generate random status - weight toward COMPLETED for older orders
        IF l_order_date < SYSDATE - 60 THEN
          -- Older orders more likely to be completed
          l_status := CASE TRUNC(DBMS_RANDOM.VALUE(1, 10))
                        WHEN 1 THEN 'CANCELLED'
                        WHEN 2 THEN 'RETURNED'
                        ELSE 'COMPLETED'
                      END;
        ELSE
          -- Newer orders - any status
          l_status := l_status_array(TRUNC(DBMS_RANDOM.VALUE(1, l_status_array.COUNT + 1)));
        END IF;
        
        -- Create the order
        INSERT INTO ORDERS (
          ORDER_ID,
          CUSTOMER_ID,
          STATUS,
          SALESPERSON_ID,
          ORDER_DATE,
          SHIPPING_DATE,
          PAYMENT_METHOD,
          ORDER_TOTAL
        ) VALUES (
          ORDERS_SEQ.NEXTVAL,
          cust_rec.CUSTOMER_ID,
          l_status,
          l_salesperson,
          l_order_date,
          CASE 
            WHEN l_status IN ('SHIPPED', 'DELIVERED', 'COMPLETED', 'RETURNED') 
            THEN l_order_date + TRUNC(DBMS_RANDOM.VALUE(1, 8))
            ELSE NULL
          END,
          l_payment_array(TRUNC(DBMS_RANDOM.VALUE(1, l_payment_array.COUNT + 1))),
          0  -- Will be updated when items are added
        ) RETURNING ORDER_ID INTO l_order_id;
        
        -- Generate order items
        l_items_per_order := TRUNC(DBMS_RANDOM.VALUE(1, p_max_items_per_order + 1));
        
        -- Add items
        FOR j IN 1..l_items_per_order LOOP
          -- Add a random product
          INSERT INTO ORDER_ITEMS (
            ORDER_ID,
            PRODUCT_ID,
            UNIT_PRICE,
            QUANTITY,
            DISCOUNT_PERCENT
          ) VALUES (
            l_order_id,
            TRUNC(DBMS_RANDOM.VALUE(1, l_max_product_id + 1)),
            ROUND(DBMS_RANDOM.VALUE(10, 500), 2),
            TRUNC(DBMS_RANDOM.VALUE(1, 11)),
            CASE
              WHEN DBMS_RANDOM.VALUE(0, 1) < 0.8 THEN 0  -- 80% no discount
              ELSE TRUNC(DBMS_RANDOM.VALUE(5, 31) / 5) * 5  -- 5%, 10%, 15%, etc.
            END
          );
        END LOOP;
        
        -- Update order total (trigger should handle this, but just in case)
        UPDATE ORDERS
        SET ORDER_TOTAL = (
          SELECT SUM(UNIT_PRICE * QUANTITY * (1 - DISCOUNT_PERCENT/100))
          FROM ORDER_ITEMS
          WHERE ORDER_ID = l_order_id
        )
        WHERE ORDER_ID = l_order_id;
      END LOOP;
      
      COMMIT;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('Test data generation completed at ' || 
                         TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
  END;
  
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Error in test data generation: ' || SQLERRM);
    RAISE;
END GENERATE_TEST_DATA;
/"""
        test_data_proc.add_dependency("EMPLOYEES")
        test_data_proc.add_dependency("CUSTOMERS")
        test_data_proc.add_dependency("ORDERS")
        test_data_proc.add_dependency("ORDER_ITEMS")
        test_data_proc.add_dependency("PRODUCTS")
        self.objects.append(test_data_proc)
        
    def _generate_validation_procedures(self) -> None:
        """Generate data validation procedures"""
        # Procedure to validate email addresses
        email_proc = OracleObject("VALIDATE_EMAIL", "PROCEDURE")
        email_proc.sql = """-- Procedure to validate email addresses
CREATE OR REPLACE PROCEDURE VALIDATE_EMAIL(
  p_email IN VARCHAR2,
  p_is_valid OUT BOOLEAN,
  p_error_message OUT VARCHAR2
)
IS
BEGIN
  -- Initialize output parameters
  p_is_valid := TRUE;
  p_error_message := NULL;
  
  -- Basic validation checks
  IF p_email IS NULL THEN
    p_is_valid := FALSE;
    p_error_message := 'Email address cannot be null';
    RETURN;
  END IF;
  
  -- Check for minimum length
  IF LENGTH(p_email) < 6 THEN
    p_is_valid := FALSE;
    p_error_message := 'Email address is too short';
    RETURN;
  END IF;
  
  -- Check for @ symbol
  IF INSTR(p_email, '@') = 0 THEN
    p_is_valid := FALSE;
    p_error_message := 'Email address must contain @ symbol';
    RETURN;
  END IF;
  
  -- Check format using regular expression
  IF NOT REGEXP_LIKE(p_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}) THEN
    p_is_valid := FALSE;
    p_error_message := 'Email address format is invalid';
    RETURN;
  END IF;
  
  -- Check for consecutive dots
  IF INSTR(p_email, '..') > 0 THEN
    p_is_valid := FALSE;
    p_error_message := 'Email address contains consecutive dots';
    RETURN;
  END IF;
  
  -- Check for Japanese characters (if present, not invalid but flag it)
  IF REGEXP_LIKE(p_email, '[\\p{IsHiragana}\\p{IsKatakana}\\p{IsHan}]') THEN
    p_error_message := 'Warning: Email contains Japanese characters which may cause compatibility issues';
  END IF;
  
  -- Email is valid
  p_is_valid := TRUE;
END VALIDATE_EMAIL;
/"""
        self.objects.append(email_proc)
        
        # Procedure to validate postal codes
        postal_proc = OracleObject("VALIDATE_POSTAL_CODE", "PROCEDURE")
        postal_proc.sql = """-- Procedure to validate postal codes based on country
CREATE OR REPLACE PROCEDURE VALIDATE_POSTAL_CODE(
  p_postal_code IN VARCHAR2,
  p_country IN VARCHAR2,
  p_is_valid OUT BOOLEAN,
  p_error_message OUT VARCHAR2
)
IS
BEGIN
  -- Initialize output parameters
  p_is_valid := TRUE;
  p_error_message := NULL;
  
  -- Handle NULL values
  IF p_postal_code IS NULL THEN
    p_is_valid := FALSE;
    p_error_message := 'Postal code cannot be null';
    RETURN;
  END IF;
  
  -- Check based on country
  CASE UPPER(p_country)
    -- Japanese postal code validation (NNN-NNNN)
    WHEN 'JAPAN' THEN
      IF NOT REGEXP_LIKE(p_postal_code, '^[0-9]{3}-[0-9]{4}) AND 
         NOT REGEXP_LIKE(p_postal_code, '^[0-9]{7}) THEN
        p_is_valid := FALSE;
        p_error_message := 'Japanese postal code must be in format NNN-NNNN or NNNNNNN';
      END IF;
    
    -- US zip code validation (NNNNN or NNNNN-NNNN)
    WHEN 'USA' THEN
      IF NOT REGEXP_LIKE(p_postal_code, '^[0-9]{5}(-[0-9]{4})?) THEN
        p_is_valid := FALSE;
        p_error_message := 'US zip code must be in format NNNNN or NNNNN-NNNN';
      END IF;
    
    -- UK postcode validation (more complex pattern)
    WHEN 'UK' THEN
      IF NOT REGEXP_LIKE(p_postal_code, '^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}, 'i') THEN
        p_is_valid := FALSE;
        p_error_message := 'UK postcode format is invalid';
      END IF;
    
    -- Canadian postal code validation (ANA NAN)
    WHEN 'CANADA' THEN
      IF NOT REGEXP_LIKE(p_postal_code, '^[A-Z][0-9][A-Z] [0-9][A-Z][0-9], 'i') THEN
        p_is_valid := FALSE;
        p_error_message := 'Canadian postal code must be in format ANA NAN';
      END IF;
    
    -- Default validation - just check for alphanumeric characters
    ELSE
      IF NOT REGEXP_LIKE(p_postal_code, '^[A-Z0-9 -]+, 'i') THEN
        p_is_valid := FALSE;
        p_error_message := 'Postal code contains invalid characters';
      END IF;
  END CASE;
END VALIDATE_POSTAL_CODE;
/"""
        self.objects.append(postal_proc)
        
        return self.objects