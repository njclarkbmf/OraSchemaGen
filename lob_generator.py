#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
lob_generator.py - LOB operations generation for OraSchemaGen

This module provides the LobGenerator class which creates Oracle database
procedures and functions for handling LOB data (CLOB, BLOB).

Author: John Clark Naldoza
"""

from typing import List, Dict, Any
from core import OracleObjectGenerator, OracleObject, TableInfo

class LobGenerator(OracleObjectGenerator):
    """
    Generates Oracle LOB handling objects
    """
    def __init__(self):
        super().__init__()
        
    def generate(self, tables: List[TableInfo], **kwargs) -> List[OracleObject]:
        """Generate Oracle LOB handling objects"""
        # Generate CLOB handling procedures
        self._generate_clob_procedures()
        
        # Generate BLOB handling procedures
        self._generate_blob_procedures()
        
        # Generate document storage procedures
        self._generate_document_procedures()
        
        # Generate text analysis procedures
        self._generate_text_analysis_procedures()
        
        return self.objects
        
    def _generate_clob_procedures(self) -> None:
        """Generate CLOB handling procedures"""
        # Procedure to append to CLOB
        append_proc = OracleObject("APPEND_TO_CLOB", "PROCEDURE")
        append_proc.sql = f"""-- Procedure to append text to a CLOB
CREATE OR REPLACE PROCEDURE APPEND_TO_CLOB(
  p_table_name IN VARCHAR2,
  p_column_name IN VARCHAR2,
  p_pk_column IN VARCHAR2,
  p_pk_value IN VARCHAR2,
  p_text_to_append IN VARCHAR2,
  p_add_newline IN BOOLEAN DEFAULT TRUE
)
IS
  l_clob CLOB;
  l_sql VARCHAR2(4000);
  l_count NUMBER;
BEGIN
  -- Validate parameters
  IF p_table_name IS NULL OR p_column_name IS NULL OR p_pk_column IS NULL OR p_pk_value IS NULL THEN
    RAISE_APPLICATION_ERROR(-20001, 'Required parameters cannot be NULL');
  END IF;
  
  -- Check if the record exists
  EXECUTE IMMEDIATE 
    'SELECT COUNT(*) FROM ' || p_table_name || 
    ' WHERE ' || p_pk_column || ' = :1'
  INTO l_count
  USING p_pk_value;
  
  IF l_count = 0 THEN
    RAISE_APPLICATION_ERROR(-20002, 'Record not found');
  END IF;
  
  -- Get the current CLOB value
  l_sql := 'SELECT ' || p_column_name || ' FROM ' || p_table_name || 
           ' WHERE ' || p_pk_column || ' = :1 FOR UPDATE';
  
  EXECUTE IMMEDIATE l_sql INTO l_clob USING p_pk_value;
  
  -- Initialize CLOB if NULL
  IF l_clob IS NULL THEN
    l_clob := EMPTY_CLOB();
  END IF;
  
  -- Append the text
  IF p_add_newline AND DBMS_LOB.GETLENGTH(l_clob) > 0 THEN
    -- Add a newline before appending if requested and CLOB is not empty
    DBMS_LOB.APPEND(l_clob, CHR(10));
  END IF;
  
  -- Append the new text
  DBMS_LOB.APPEND(l_clob, p_text_to_append);
  
  -- Update the CLOB in the table
  l_sql := 'UPDATE ' || p_table_name || 
           ' SET ' || p_column_name || ' = :1' ||
           ' WHERE ' || p_pk_column || ' = :2';
  
  EXECUTE IMMEDIATE l_sql USING l_clob, p_pk_value;
  
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back the transaction
    ROLLBACK;
    RAISE;
END APPEND_TO_CLOB;
/"""
        self.objects.append(append_proc)
        
        # Function to get CLOB substring
        substr_func = OracleObject("GET_CLOB_SUBSTRING", "FUNCTION")
        substr_func.sql = f"""-- Function to get a substring from a CLOB
CREATE OR REPLACE FUNCTION GET_CLOB_SUBSTRING(
  p_clob IN CLOB,
  p_start_pos IN NUMBER,
  p_length IN NUMBER DEFAULT NULL
) RETURN VARCHAR2
IS
  l_result VARCHAR2(32767);
  l_clob_length NUMBER;
  l_bytes_to_read NUMBER;
  l_max_varchar_length CONSTANT NUMBER := 32767;
BEGIN
  -- Handle NULL input
  IF p_clob IS NULL OR p_start_pos IS NULL THEN
    RETURN NULL;
  END IF;
  
  -- Get CLOB length
  l_clob_length := DBMS_LOB.GETLENGTH(p_clob);
  
  -- Return empty string if CLOB is empty or start position is beyond end
  IF l_clob_length = 0 OR p_start_pos > l_clob_length THEN
    RETURN '';
  END IF;
  
  -- Calculate how many bytes to read
  IF p_length IS NULL THEN
    l_bytes_to_read := l_clob_length - p_start_pos + 1;
  ELSE
    l_bytes_to_read := p_length;
  END IF;
  
  -- Limit to maximum VARCHAR2 length
  l_bytes_to_read := LEAST(l_bytes_to_read, l_max_varchar_length);
  
  -- Make sure we don't try to read beyond the end of the CLOB
  l_bytes_to_read := LEAST(l_bytes_to_read, l_clob_length - p_start_pos + 1);
  
  -- Read from CLOB
  DBMS_LOB.READ(p_clob, l_bytes_to_read, p_start_pos, l_result);
  
  RETURN l_result;
EXCEPTION
  WHEN OTHERS THEN
    -- Return error information
    RETURN 'ERROR: ' || SQLERRM;
END GET_CLOB_SUBSTRING;
/"""
        self.objects.append(substr_func)
        
        # Function to search in CLOB
        search_func = OracleObject("SEARCH_CLOB", "FUNCTION")
        search_func.sql = f"""-- Function to search for text in a CLOB
CREATE OR REPLACE FUNCTION SEARCH_CLOB(
  p_clob IN CLOB,
  p_search_string IN VARCHAR2,
  p_start_position IN NUMBER DEFAULT 1,
  p_occurrence IN NUMBER DEFAULT 1,
  p_case_sensitive IN BOOLEAN DEFAULT TRUE
) RETURN NUMBER
IS
  l_position NUMBER := p_start_position;
  l_occurrence NUMBER := 0;
  l_chunk_size CONSTANT NUMBER := 32767;
  l_chunk VARCHAR2(32767);
  l_chunk_pos NUMBER;
  l_clob_length NUMBER;
  l_search_expr VARCHAR2(100);
  l_search_string VARCHAR2(1000) := p_search_string;
BEGIN
  -- Handle NULL input
  IF p_clob IS NULL OR p_search_string IS NULL THEN
    RETURN 0;
  END IF;
  
  -- Get CLOB length
  l_clob_length := DBMS_LOB.GETLENGTH(p_clob);
  
  -- Return 0 if CLOB is empty
  IF l_clob_length = 0 THEN
    RETURN 0;
  END IF;
  
  -- Handle case sensitivity
  IF NOT p_case_sensitive THEN
    l_search_string := UPPER(p_search_string);
  END IF;
  
  -- Search in chunks
  WHILE l_position <= l_clob_length LOOP
    -- Read a chunk
    l_chunk := GET_CLOB_SUBSTRING(p_clob, l_position, l_chunk_size);
    
    -- Handle case sensitivity
    IF NOT p_case_sensitive THEN
      l_chunk := UPPER(l_chunk);
    END IF;
    
    -- Search in chunk
    l_chunk_pos := INSTR(l_chunk, l_search_string);
    
    -- Found match in this chunk
    WHILE l_chunk_pos > 0 LOOP
      l_occurrence := l_occurrence + 1;
      
      -- If this is the occurrence we're looking for
      IF l_occurrence = p_occurrence THEN
        RETURN l_position + l_chunk_pos - 1;
      END IF;
      
      -- Look for next occurrence in this chunk
      l_chunk_pos := INSTR(l_chunk, l_search_string, l_chunk_pos + 1);
    END LOOP;
    
    -- Move to next chunk
    l_position := l_position + l_chunk_size;
  END LOOP;
  
  -- Return 0 if not found or occurrence is not found
  RETURN 0;
END SEARCH_CLOB;
/"""
        self.objects.append(search_func)
        
    def _generate_blob_procedures(self) -> None:
        """Generate BLOB handling procedures"""
        # Function to get BLOB size
        blob_size_func = OracleObject("GET_BLOB_SIZE", "FUNCTION")
        blob_size_func.sql = f"""-- Function to get size information for a BLOB
CREATE OR REPLACE FUNCTION GET_BLOB_SIZE(
  p_blob IN BLOB,
  p_format IN VARCHAR2 DEFAULT 'BYTES'  -- 'BYTES', 'KB', 'MB', 'GB'
) RETURN NUMBER
IS
  l_bytes NUMBER;
  l_result NUMBER;
BEGIN
  -- Handle NULL input
  IF p_blob IS NULL THEN
    RETURN NULL;
  END IF;
  
  -- Get BLOB size in bytes
  l_bytes := DBMS_LOB.GETLENGTH(p_blob);
  
  -- Convert to requested format
  CASE UPPER(p_format)
    WHEN 'BYTES' THEN
      l_result := l_bytes;
    WHEN 'KB' THEN
      l_result := l_bytes / 1024;
    WHEN 'MB' THEN
      l_result := l_bytes / (1024 * 1024);
    WHEN 'GB' THEN
      l_result := l_bytes / (1024 * 1024 * 1024);
    ELSE
      RAISE_APPLICATION_ERROR(-20001, 'Invalid format. Use BYTES, KB, MB, or GB');
  END CASE;
  
  RETURN l_result;
END GET_BLOB_SIZE;
/"""
        self.objects.append(blob_size_func)
        
        # Procedure to convert BLOB to Base64
        base64_proc = OracleObject("BLOB_TO_BASE64", "FUNCTION")
        base64_proc.sql = f"""-- Function to convert BLOB to Base64 string
CREATE OR REPLACE FUNCTION BLOB_TO_BASE64(
  p_blob IN BLOB
) RETURN CLOB
IS
  l_result CLOB;
  l_step PLS_INTEGER := 12000;  -- Multiple of 3 to avoid padding issues
  l_temp RAW(32767);
  l_offset NUMBER := 1;
  l_amount NUMBER;
  l_blob_size NUMBER;
BEGIN
  -- Handle NULL input
  IF p_blob IS NULL OR DBMS_LOB.GETLENGTH(p_blob) = 0 THEN
    RETURN NULL;
  END IF;
  
  -- Initialize result CLOB
  DBMS_LOB.CREATETEMPORARY(l_result, TRUE);
  
  -- Get BLOB size
  l_blob_size := DBMS_LOB.GETLENGTH(p_blob);
  
  -- Process in chunks
  WHILE l_offset <= l_blob_size LOOP
    -- Determine amount to read
    l_amount := LEAST(l_step, l_blob_size - l_offset + 1);
    
    -- Read chunk from BLOB
    DBMS_LOB.READ(p_blob, l_amount, l_offset, l_temp);
    
    -- Append Base64-encoded data to result CLOB
    DBMS_LOB.APPEND(l_result, UTL_ENCODE.BASE64_ENCODE(l_temp));
    
    -- Move to next chunk
    l_offset := l_offset + l_amount;
  END LOOP;
  
  RETURN l_result;
END BLOB_TO_BASE64;
/"""
        self.objects.append(base64_proc)
        
        # Function to convert Base64 to BLOB
        base64_to_blob_func = OracleObject("BASE64_TO_BLOB", "FUNCTION")
        base64_to_blob_func.sql = f"""-- Function to convert Base64 string to BLOB
CREATE OR REPLACE FUNCTION BASE64_TO_BLOB(
  p_base64 IN CLOB
) RETURN BLOB
IS
  l_result BLOB;
  l_step PLS_INTEGER := 16000;  -- Size of chunk to process at once
  l_offset NUMBER := 1;
  l_amount NUMBER;
  l_clob_size NUMBER;
  l_buffer VARCHAR2(32767);
  l_raw RAW(32767);
BEGIN
  -- Handle NULL input
  IF p_base64 IS NULL OR DBMS_LOB.GETLENGTH(p_base64) = 0 THEN
    RETURN NULL;
  END IF;
  
  -- Initialize result BLOB
  DBMS_LOB.CREATETEMPORARY(l_result, TRUE);
  
  -- Get CLOB size
  l_clob_size := DBMS_LOB.GETLENGTH(p_base64);
  
  -- Process in chunks
  WHILE l_offset <= l_clob_size LOOP
    -- Determine amount to read
    l_amount := LEAST(l_step, l_clob_size - l_offset + 1);
    
    -- Read chunk from CLOB
    DBMS_LOB.READ(p_base64, l_amount, l_offset, l_buffer);
    
    -- Convert Base64 to RAW
    l_raw := UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(l_buffer));
    
    -- Append decoded data to result BLOB
    DBMS_LOB.APPEND(l_result, l_raw);
    
    -- Move to next chunk
    l_offset := l_offset + l_amount;
  END LOOP;
  
  RETURN l_result;
END BASE64_TO_BLOB;
/"""
        self.objects.append(base64_to_blob_func)
        
    def _generate_document_procedures(self) -> None:
        """Generate document storage procedures"""
        # Procedure to store document
        store_doc_proc = OracleObject("STORE_DOCUMENT", "PROCEDURE")
        store_doc_proc.sql = f"""-- Procedure to store a document in a BLOB column
CREATE OR REPLACE PROCEDURE STORE_DOCUMENT(
  p_table_name IN VARCHAR2,
  p_column_name IN VARCHAR2,
  p_pk_column IN VARCHAR2,
  p_pk_value IN VARCHAR2,
  p_document_data IN BLOB,
  p_document_name IN VARCHAR2,
  p_mime_type IN VARCHAR2,
  p_metadata_column IN VARCHAR2 DEFAULT NULL,
  p_metadata IN CLOB DEFAULT NULL
)
IS
  l_sql VARCHAR2(4000);
  l_count NUMBER;
  l_metadata_update VARCHAR2(1000);
BEGIN
  -- Validate parameters
  IF p_table_name IS NULL OR p_column_name IS NULL OR 
     p_pk_column IS NULL OR p_pk_value IS NULL OR
     p_document_data IS NULL THEN
    RAISE_APPLICATION_ERROR(-20001, 'Required parameters cannot be NULL');
  END IF;
  
  -- Check if the record exists
  EXECUTE IMMEDIATE 
    'SELECT COUNT(*) FROM ' || p_table_name || 
    ' WHERE ' || p_pk_column || ' = :1'
  INTO l_count
  USING p_pk_value;
  
  IF l_count = 0 THEN
    RAISE_APPLICATION_ERROR(-20002, 'Record not found');
  END IF;
  
  -- Prepare metadata update if metadata column is provided
  IF p_metadata_column IS NOT NULL THEN
    l_metadata_update := ', ' || p_metadata_column || ' = :4';
  ELSE
    l_metadata_update := '';
  END IF;
  
  -- Create dynamic SQL
  l_sql := 'UPDATE ' || p_table_name || 
           ' SET ' || p_column_name || ' = :1, ' ||
           'DOCUMENT_NAME = :2, ' ||
           'MIME_TYPE = :3' ||
           l_metadata_update ||
           ' WHERE ' || p_pk_column || ' = :5';
  
  -- Execute update
  IF p_metadata_column IS NOT NULL THEN
    EXECUTE IMMEDIATE l_sql
    USING p_document_data, p_document_name, p_mime_type, p_metadata, p_pk_value;
  ELSE
    EXECUTE IMMEDIATE l_sql
    USING p_document_data, p_document_name, p_mime_type, p_pk_value;
  END IF;
  
  COMMIT;
  
  -- Log document storage
  DBMS_OUTPUT.PUT_LINE(
    'Document "' || p_document_name || '" (' || p_mime_type || ') ' ||
    'stored in ' || p_table_name || '.' || p_column_name || 
    ' for ' || p_pk_column || ' = ' || p_pk_value
  );
EXCEPTION
  WHEN OTHERS THEN
    -- Roll back the transaction
    ROLLBACK;
    RAISE;
END STORE_DOCUMENT;
/"""
        self.objects.append(store_doc_proc)
        
        # Function to extract file extension
        file_ext_func = OracleObject("GET_FILE_EXTENSION", "FUNCTION")
        file_ext_func.sql = f"""-- Function to extract file extension from filename
CREATE OR REPLACE FUNCTION GET_FILE_EXTENSION(
  p_filename IN VARCHAR2
) RETURN VARCHAR2
IS
  l_dot_pos NUMBER;
BEGIN
  -- Handle NULL or empty filename
  IF p_filename IS NULL OR LENGTH(p_filename) = 0 THEN
    RETURN NULL;
  END IF;
  
  -- Find the last dot in the filename
  l_dot_pos := INSTR(p_filename, '.', -1);
  
  -- If no dot or dot is at the beginning/end, return empty string
  IF l_dot_pos <= 1 OR l_dot_pos = LENGTH(p_filename) THEN
    RETURN '';
  END IF;
  
  -- Extract and return extension
  RETURN SUBSTR(p_filename, l_dot_pos + 1);
END GET_FILE_EXTENSION;
/"""
        self.objects.append(file_ext_func)
        
        # Function to get MIME type from file extension
        mime_type_func = OracleObject("GET_MIME_TYPE", "FUNCTION")
        mime_type_func.sql = f"""-- Function to get MIME type from file extension
CREATE OR REPLACE FUNCTION GET_MIME_TYPE(
  p_filename IN VARCHAR2
) RETURN VARCHAR2
IS
  l_extension VARCHAR2(100);
BEGIN
  -- Extract file extension
  l_extension := UPPER(GET_FILE_EXTENSION(p_filename));
  
  -- Return MIME type based on extension
  CASE l_extension
    WHEN 'PDF' THEN
      RETURN 'application/pdf';
    WHEN 'DOC' THEN
      RETURN 'application/msword';
    WHEN 'DOCX' THEN
      RETURN 'application/vnd.openxmlformats-officedocument.wordprocessingml.document';
    WHEN 'XLS' THEN
      RETURN 'application/vnd.ms-excel';
    WHEN 'XLSX' THEN
      RETURN 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    WHEN 'PPT' THEN
      RETURN 'application/vnd.ms-powerpoint';
    WHEN 'PPTX' THEN
      RETURN 'application/vnd.openxmlformats-officedocument.presentationml.presentation';
    WHEN 'TXT' THEN
      RETURN 'text/plain';
    WHEN 'CSV' THEN
      RETURN 'text/csv';
    WHEN 'HTML' THEN
      RETURN 'text/html';
    WHEN 'XML' THEN
      RETURN 'application/xml';
    WHEN 'JSON' THEN
      RETURN 'application/json';
    WHEN 'ZIP' THEN
      RETURN 'application/zip';
    WHEN 'RAR' THEN
      RETURN 'application/x-rar-compressed';
    WHEN 'TAR' THEN
      RETURN 'application/x-tar';
    WHEN 'GZ' THEN
      RETURN 'application/gzip';
    WHEN 'JPG', 'JPEG' THEN
      RETURN 'image/jpeg';
    WHEN 'PNG' THEN
      RETURN 'image/png';
    WHEN 'GIF' THEN
      RETURN 'image/gif';
    WHEN 'BMP' THEN
      RETURN 'image/bmp';
    WHEN 'TIFF', 'TIF' THEN
      RETURN 'image/tiff';
    WHEN 'MP3' THEN
      RETURN 'audio/mpeg';
    WHEN 'WAV' THEN
      RETURN 'audio/wav';
    WHEN 'MP4' THEN
      RETURN 'video/mp4';
    WHEN 'AVI' THEN
      RETURN 'video/x-msvideo';
    WHEN 'MPEG', 'MPG' THEN
      RETURN 'video/mpeg';
    ELSE
      RETURN 'application/octet-stream';  -- Default binary MIME type
  END CASE;
END GET_MIME_TYPE;
/"""
        self.objects.append(mime_type_func)
        
    def _generate_text_analysis_procedures(self) -> None:
        """Generate text analysis procedures"""
        # Function to extract keywords from text
        extract_keywords_func = OracleObject("EXTRACT_KEYWORDS", "FUNCTION")
        extract_keywords_func.sql = f"""-- Function to extract keywords from text
CREATE OR REPLACE FUNCTION EXTRACT_KEYWORDS(
  p_text IN CLOB,
  p_max_keywords IN NUMBER DEFAULT 10,
  p_min_length IN NUMBER DEFAULT 4,
  p_exclude_words IN VARCHAR2 DEFAULT NULL
) RETURN VARCHAR2
IS
  TYPE word_count_rec IS RECORD (
    word VARCHAR2(100),
    count_val NUMBER
  );
  TYPE word_count_tab IS TABLE OF word_count_rec INDEX BY VARCHAR2(100);
  
  l_words word_count_tab;
  l_exclude_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
  l_exclude_words VARCHAR2(32767) := LOWER(NVL(p_exclude_words, 
    'a,an,the,and,or,but,if,of,at,by,for,with,about,to,from,in,on,is,are,was,were,be,been,being,have,has,had,do,does,did,shall,will,should,would,may,might,must,can,could'));
  
  l_text CLOB := LOWER(p_text);
  l_word VARCHAR2(100);
  l_word_list VARCHAR2(32767);
  l_keyword_list VARCHAR2(32767);
  l_top_words SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
  l_cur_pos NUMBER := 1;
  l_end_pos NUMBER;
  l_max_count NUMBER := 0;
  l_count NUMBER;
  l_pos NUMBER;
BEGIN
  -- Handle NULL or empty text
  IF p_text IS NULL OR DBMS_LOB.GETLENGTH(p_text) = 0 THEN
    RETURN NULL;
  END IF;
  
  -- Parse exclude words into collection
  l_pos := 1;
  LOOP
    l_end_pos := INSTR(l_exclude_words, ',', l_pos);
    IF l_end_pos = 0 THEN
      l_exclude_list.EXTEND;
      l_exclude_list(l_exclude_list.COUNT) := TRIM(SUBSTR(l_exclude_words, l_pos));
      EXIT;
    ELSE
      l_exclude_list.EXTEND;
      l_exclude_list(l_exclude_list.COUNT) := TRIM(SUBSTR(l_exclude_words, l_pos, l_end_pos - l_pos));
      l_pos := l_end_pos + 1;
    END IF;
  END LOOP;
  
  -- Normalize text: replace non-alphanumeric with space
  l_text := REGEXP_REPLACE(l_text, '[^a-z0-9 ]', ' ');
  
  -- Count word occurrences
  l_cur_pos := 1;
  LOOP
    -- Find next word
    WHILE l_cur_pos <= DBMS_LOB.GETLENGTH(l_text) AND 
          SUBSTR(l_text, l_cur_pos, 1) = ' ' LOOP
      l_cur_pos := l_cur_pos + 1;
    END LOOP;
    
    EXIT WHEN l_cur_pos > DBMS_LOB.GETLENGTH(l_text);
    
    -- Find end of word
    l_end_pos := INSTR(l_text, ' ', l_cur_pos);
    IF l_end_pos = 0 THEN
      l_end_pos := DBMS_LOB.GETLENGTH(l_text) + 1;
    END IF;
    
    -- Extract word
    l_word := SUBSTR(l_text, l_cur_pos, l_end_pos - l_cur_pos);
    
    -- Check word length
    IF LENGTH(l_word) >= p_min_length THEN
      -- Check if word is in exclude list
      l_count := 0;
      FOR i IN 1..l_exclude_list.COUNT LOOP
        IF l_word = l_exclude_list(i) THEN
          l_count := 1;
          EXIT;
        END IF;
      END LOOP;
      
      -- If not excluded, add to count
      IF l_count = 0 THEN
        IF l_words.EXISTS(l_word) THEN
          l_words(l_word).count_val := l_words(l_word).count_val + 1;
        ELSE
          l_words(l_word).word := l_word;
          l_words(l_word).count_val := 1;
        END IF;
        
        -- Update max count
        IF l_words(l_word).count_val > l_max_count THEN
          l_max_count := l_words(l_word).count_val;
        END IF;
      END IF;
    END IF;
    
    -- Move to next position
    l_cur_pos := l_end_pos + 1;
  END LOOP;
  
  -- Sort words by frequency (implementation simplified for this example)
  -- In a real system, we would use a more efficient sorting algorithm
  -- or the ORDER BY clause in a SQL query
  
  -- For each count value (from max to 1), add words to the list
  FOR i IN REVERSE 1..l_max_count LOOP
    -- For each word in the hash table
    l_word := l_words.FIRST;
    WHILE l_word IS NOT NULL LOOP
      -- If count matches current level
      IF l_words(l_word).count_val = i THEN
        -- Add to top words list
        l_top_words.EXTEND;
        l_top_words(l_top_words.COUNT) := l_word;
        
        -- Exit if we have enough keywords
        EXIT WHEN l_top_words.COUNT >= p_max_keywords;
      END IF;
      
      -- Move to next word
      l_word := l_words.NEXT(l_word);
    END LOOP;
    
    -- Exit if we have enough keywords
    EXIT WHEN l_top_words.COUNT >= p_max_keywords;
  END LOOP;
  
  -- Convert top words to comma-separated list
  FOR i IN 1..l_top_words.COUNT LOOP
    IF i > 1 THEN
      l_keyword_list := l_keyword_list || ', ';
    END IF;
    l_keyword_list := l_keyword_list || l_top_words(i);
  END LOOP;
  
  RETURN l_keyword_list;
END EXTRACT_KEYWORDS;
/"""
        self.objects.append(extract_keywords_func)
        
        # Function to generate a text summary
        text_summary_func = OracleObject("GENERATE_TEXT_SUMMARY", "FUNCTION")
        text_summary_func.sql = f"""-- Function to generate a summary of text
CREATE OR REPLACE FUNCTION GENERATE_TEXT_SUMMARY(
  p_text IN CLOB,
  p_max_length IN NUMBER DEFAULT 500,
  p_summary_type IN VARCHAR2 DEFAULT 'BEGINNING'  -- 'BEGINNING', 'MIDDLE', 'END', 'SMART'
) RETURN VARCHAR2
IS
  l_summary VARCHAR2(32767);
  l_text_length NUMBER;
  l_start_pos NUMBER;
  l_end_pos NUMBER;
  l_max_sentences NUMBER := 3;
  l_sentence_count NUMBER := 0;
  l_current_pos NUMBER := 1;
  l_sentence_end NUMBER;
  l_sentence VARCHAR2(32767);
BEGIN
  -- Handle NULL or empty text
  IF p_text IS NULL OR DBMS_LOB.GETLENGTH(p_text) = 0 THEN
    RETURN NULL;
  END IF;
  
  -- Get text length
  l_text_length := DBMS_LOB.GETLENGTH(p_text);
  
  -- Handle different summary types
  CASE UPPER(p_summary_type)
    WHEN 'BEGINNING' THEN
      -- Take text from the beginning
      l_summary := SUBSTR(p_text, 1, LEAST(p_max_length, l_text_length));
      
      -- Try to end at sentence boundary
      l_end_pos := INSTR(l_summary, '. ', -1);
      IF l_end_pos > p_max_length * 0.7 THEN
        l_summary := SUBSTR(l_summary, 1, l_end_pos + 1);
      END IF;
      
    WHEN 'MIDDLE' THEN
      -- Take text from the middle
      l_start_pos := GREATEST(1, ROUND(l_text_length / 2) - ROUND(p_max_length / 2));
      l_end_pos := LEAST(l_text_length, l_start_pos + p_max_length - 1);
      
      l_summary := SUBSTR(p_text, l_start_pos, l_end_pos - l_start_pos + 1);
      
      -- Try to start at sentence boundary
      l_start_pos := INSTR(l_summary, '. ');
      IF l_start_pos > 0 AND l_start_pos < p_max_length * 0.3 THEN
        l_summary := SUBSTR(l_summary, l_start_pos + 2);
      END IF;
      
      -- Try to end at sentence boundary
      l_end_pos := INSTR(l_summary, '. ', -1);
      IF l_end_pos > p_max_length * 0.7 THEN
        l_summary := SUBSTR(l_summary, 1, l_end_pos + 1);
      END IF;
      
    WHEN 'END' THEN
      -- Take text from the end
      l_start_pos := GREATEST(1, l_text_length - p_max_length + 1);
      
      l_summary := SUBSTR(p_text, l_start_pos);
      
      -- Try to start at sentence boundary
      l_start_pos := INSTR(l_summary, '. ');
      IF l_start_pos > 0 AND l_start_pos < p_max_length * 0.3 THEN
        l_summary := SUBSTR(l_summary, l_start_pos + 2);
      END IF;
      
    WHEN 'SMART' THEN
      -- Extract first few sentences (simplified 'smart' summary)
      WHILE l_current_pos <= l_text_length AND l_sentence_count < l_max_sentences LOOP
        -- Find end of current sentence
        l_sentence_end := INSTR(p_text, '. ', l_current_pos);
        
        -- If no more sentences, break
        IF l_sentence_end = 0 THEN
          -- Add remaining text if it's not too long
          IF l_text_length - l_current_pos + 1 <= p_max_length - LENGTH(l_summary) THEN
            l_summary := l_summary || SUBSTR(p_text, l_current_pos);
          END IF;
          EXIT;
        END IF;
        
        -- Extract sentence
        l_sentence := SUBSTR(p_text, l_current_pos, l_sentence_end - l_current_pos + 1);
        
        -- Add sentence to summary if it fits
        IF LENGTH(l_summary || l_sentence) <= p_max_length THEN
          l_summary := l_summary || l_sentence || ' ';
          l_sentence_count := l_sentence_count + 1;
        ELSE
          EXIT;
        END IF;
        
        -- Move to next sentence
        l_current_pos := l_sentence_end + 2;
      END LOOP;
      
    ELSE
      -- Default to BEGINNING
      l_summary := SUBSTR(p_text, 1, LEAST(p_max_length, l_text_length));
  END CASE;
  
  -- Add ellipsis if summary is shorter than text
  IF LENGTH(l_summary) < l_text_length THEN
    l_summary := RTRIM(l_summary) || '...';
  END IF;
  
  RETURN l_summary;
END GENERATE_TEXT_SUMMARY;
/"""
        self.objects.append(text_summary_func)
        
        return self.objects
  