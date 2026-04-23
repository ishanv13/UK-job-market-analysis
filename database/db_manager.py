"""
Database Connection Module for UK Job Market Intelligence Platform

This module provides a SQLAlchemy-based connection manager with:
- Connection pooling for efficient database access
- Helper functions for bulk inserts and updates
- Error handling and transaction management
- Support for PostgreSQL database operations
"""

import os
import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import create_engine, text, Table, MetaData, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Database connection manager with connection pooling and transaction support.
    
    Provides a centralized interface for database operations including:
    - Connection pooling for efficient resource usage
    - Bulk insert and update operations
    - Transaction management with automatic rollback on errors
    - Error handling and logging
    """
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600
    ):
        """
        Initialize database manager with connection pooling.
        
        Args:
            connection_string: PostgreSQL connection string (defaults to DB_CONNECTION_STRING env var)
            pool_size: Number of connections to maintain in the pool (default: 5)
            max_overflow: Maximum number of connections to create beyond pool_size (default: 10)
            pool_timeout: Seconds to wait before giving up on getting a connection (default: 30)
            pool_recycle: Seconds after which to recycle connections (default: 3600)
            
        Raises:
            ValueError: If connection string is not provided or found in environment
        """
        # Get connection string from parameter or environment
        self.connection_string = connection_string or os.getenv('DB_CONNECTION_STRING')
        
        if not self.connection_string:
            raise ValueError(
                "Database connection string not found. "
                "Please set DB_CONNECTION_STRING environment variable or pass connection_string parameter."
            )
        
        # Create engine with connection pooling
        try:
            self.engine = create_engine(
                self.connection_string,
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                echo=False  # Set to True for SQL query logging
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(
                f"DatabaseManager initialized successfully "
                f"(pool_size={pool_size}, max_overflow={max_overflow})"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise
        
        # Initialize metadata for table reflection
        self.metadata = MetaData()
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        
        Automatically handles connection lifecycle and ensures proper cleanup.
        
        Yields:
            SQLAlchemy connection object
            
        Example:
            with db_manager.get_connection() as conn:
                result = conn.execute(text("SELECT * FROM job_postings"))
        """
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()
    
    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.
        
        Automatically commits on success or rolls back on error.
        
        Yields:
            SQLAlchemy connection object with active transaction
            
        Example:
            with db_manager.transaction() as conn:
                conn.execute(text("INSERT INTO ..."))
                conn.execute(text("UPDATE ..."))
                # Automatically commits if no exception
        """
        connection = self.engine.connect()
        trans = connection.begin()
        try:
            yield connection
            trans.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            trans.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
        finally:
            connection.close()
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            params: Optional dictionary of query parameters
            
        Returns:
            Query result object
            
        Raises:
            SQLAlchemyError: If query execution fails
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                return result
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def bulk_insert(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        batch_size: int = 1000
    ) -> int:
        """
        Perform bulk insert operation with batching.
        
        Inserts records in batches to optimize performance and memory usage.
        Uses transactions to ensure atomicity within each batch.
        
        Args:
            table_name: Name of the table to insert into
            records: List of dictionaries representing rows to insert
            batch_size: Number of records to insert per batch (default: 1000)
            
        Returns:
            Total number of records inserted
            
        Raises:
            SQLAlchemyError: If insert operation fails
            ValueError: If records list is empty or table doesn't exist
        """
        if not records:
            logger.warning(f"No records provided for bulk insert into {table_name}")
            return 0
        
        total_inserted = 0
        
        try:
            # Reflect table structure
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            
            # Process records in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                with self.transaction() as conn:
                    result = conn.execute(table.insert(), batch)
                    batch_count = result.rowcount
                    total_inserted += batch_count
                    
                    logger.info(
                        f"Inserted batch {i // batch_size + 1}: "
                        f"{batch_count} records into {table_name}"
                    )
            
            logger.info(f"Bulk insert completed: {total_inserted} total records into {table_name}")
            return total_inserted
            
        except SQLAlchemyError as e:
            logger.error(f"Bulk insert failed for table {table_name}: {e}")
            raise
    
    def bulk_update(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000
    ) -> int:
        """
        Perform bulk update operation with batching.
        
        Updates records based on key columns. Records must include key columns
        to identify which rows to update.
        
        Args:
            table_name: Name of the table to update
            records: List of dictionaries with updated values (must include key columns)
            key_columns: List of column names to use for matching records
            batch_size: Number of records to update per batch (default: 1000)
            
        Returns:
            Total number of records updated
            
        Raises:
            SQLAlchemyError: If update operation fails
            ValueError: If records list is empty, key_columns is empty, or table doesn't exist
        """
        if not records:
            logger.warning(f"No records provided for bulk update in {table_name}")
            return 0
        
        if not key_columns:
            raise ValueError("key_columns must contain at least one column name")
        
        total_updated = 0
        
        try:
            # Reflect table structure
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            
            # Process records in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                with self.transaction() as conn:
                    for record in batch:
                        # Build WHERE clause from key columns
                        where_clause = " AND ".join([
                            f"{col} = :{col}" for col in key_columns
                        ])
                        
                        # Build SET clause from non-key columns
                        set_columns = [k for k in record.keys() if k not in key_columns]
                        set_clause = ", ".join([
                            f"{col} = :{col}" for col in set_columns
                        ])
                        
                        if not set_clause:
                            continue
                        
                        # Execute update
                        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                        result = conn.execute(text(query), record)
                        total_updated += result.rowcount
                    
                    logger.info(
                        f"Updated batch {i // batch_size + 1}: "
                        f"processed {len(batch)} records in {table_name}"
                    )
            
            logger.info(f"Bulk update completed: {total_updated} total records in {table_name}")
            return total_updated
            
        except SQLAlchemyError as e:
            logger.error(f"Bulk update failed for table {table_name}: {e}")
            raise
    
    def bulk_upsert(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000
    ) -> Dict[str, int]:
        """
        Perform bulk upsert (insert or update) operation.
        
        Inserts new records or updates existing ones based on key columns.
        Uses PostgreSQL's ON CONFLICT clause for efficient upserts.
        
        Args:
            table_name: Name of the table
            records: List of dictionaries representing rows
            key_columns: List of column names to use for conflict detection
            batch_size: Number of records to process per batch (default: 1000)
            
        Returns:
            Dictionary with 'inserted' and 'updated' counts
            
        Raises:
            SQLAlchemyError: If upsert operation fails
            ValueError: If records list is empty or key_columns is empty
        """
        if not records:
            logger.warning(f"No records provided for bulk upsert in {table_name}")
            return {'inserted': 0, 'updated': 0}
        
        if not key_columns:
            raise ValueError("key_columns must contain at least one column name")
        
        total_inserted = 0
        total_updated = 0
        
        try:
            # Process records in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                with self.transaction() as conn:
                    for record in batch:
                        # Get all column names
                        columns = list(record.keys())
                        update_columns = [col for col in columns if col not in key_columns]
                        
                        # Build INSERT clause
                        col_names = ", ".join(columns)
                        col_placeholders = ", ".join([f":{col}" for col in columns])
                        
                        # Build ON CONFLICT clause
                        conflict_cols = ", ".join(key_columns)
                        update_set = ", ".join([
                            f"{col} = EXCLUDED.{col}" for col in update_columns
                        ])
                        
                        # Build complete upsert query
                        query = f"""
                            INSERT INTO {table_name} ({col_names})
                            VALUES ({col_placeholders})
                            ON CONFLICT ({conflict_cols})
                            DO UPDATE SET {update_set}
                        """
                        
                        conn.execute(text(query), record)
                    
                    # Note: PostgreSQL doesn't provide separate insert/update counts for upserts
                    # We'll count all as "inserted" for simplicity
                    total_inserted += len(batch)
                    
                    logger.info(
                        f"Upserted batch {i // batch_size + 1}: "
                        f"{len(batch)} records in {table_name}"
                    )
            
            logger.info(f"Bulk upsert completed: {total_inserted} total records in {table_name}")
            return {'inserted': total_inserted, 'updated': 0}
            
        except SQLAlchemyError as e:
            logger.error(f"Bulk upsert failed for table {table_name}: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
        except SQLAlchemyError as e:
            logger.error(f"Failed to check if table {table_name} exists: {e}")
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
            
        Raises:
            SQLAlchemyError: If query fails
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                return count
        except SQLAlchemyError as e:
            logger.error(f"Failed to get row count for table {table_name}: {e}")
            raise
    
    def execute_sql_file(self, filepath: str) -> None:
        """
        Execute SQL commands from a file.
        
        Useful for running schema creation scripts or migrations.
        
        Args:
            filepath: Path to SQL file
            
        Raises:
            FileNotFoundError: If SQL file doesn't exist
            SQLAlchemyError: If SQL execution fails
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            with self.transaction() as conn:
                # Split by semicolon and execute each statement
                statements = [s.strip() for s in sql_content.split(';') if s.strip()]
                
                for statement in statements:
                    if statement:
                        conn.execute(text(statement))
                
                logger.info(f"Successfully executed SQL file: {filepath}")
                
        except FileNotFoundError:
            logger.error(f"SQL file not found: {filepath}")
            raise
        except SQLAlchemyError as e:
            logger.error(f"Failed to execute SQL file {filepath}: {e}")
            raise
    
    def close(self) -> None:
        """
        Close database engine and dispose of connection pool.
        
        Should be called when the application is shutting down.
        """
        try:
            self.engine.dispose()
            logger.info("Database connection pool disposed")
        except Exception as e:
            logger.error(f"Error disposing database connection pool: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - automatically closes connections."""
        self.close()


# Convenience function to create a database manager instance
def create_db_manager(
    connection_string: Optional[str] = None,
    **kwargs
) -> DatabaseManager:
    """
    Create and return a DatabaseManager instance.
    
    Args:
        connection_string: Optional PostgreSQL connection string
        **kwargs: Additional arguments to pass to DatabaseManager constructor
        
    Returns:
        DatabaseManager instance
        
    Example:
        db = create_db_manager()
        with db.transaction() as conn:
            conn.execute(text("INSERT INTO ..."))
    """
    return DatabaseManager(connection_string=connection_string, **kwargs)
