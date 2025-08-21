"""
Database Service API for CandleThrob

This module provides a FastAPI-based REST service for database operations,
offering a clean HTTP interface for database queries and data retrieval.

Features:
- RESTful API for database operations
- Query execution with parameter binding
- Data retrieval with filtering and pagination
- Health checks and monitoring
- Comprehensive error handling

Author: Adetunji Fasiku
Version: 1.0.0
"""

import os
import sys
import logging
import uvicorn
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Union

try:
    from fastapi import FastAPI, HTTPException, Query, Depends
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel, Field
except ImportError as e:
    print(f"Required packages not installed: {e}")
    print("Please install: pip install fastapi uvicorn")
    sys.exit(1)

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.core.logging import get_ingestion_logger, get_transform_logger, log_ingestion_start, log_ingestion_success, log_ingestion_error, log_rate_limiting, log_batch_progress

# Initialize logger
logger = get_ingestion_logger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="CandleThrob Database Service",
    description="REST API for CandleThrob database operations",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global database connection
db_connection: Optional[OracleDB] = None

# Request/Response models
class QueryRequest(BaseModel):
    query: str = Field(..., description="SQL query to execute")
    params: Optional[Dict[str, Any]] = Field(None, description="Query parameters")

class QueryResponse(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    message: Optional[str] = None
    row_count: int = 0

class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    database_connected: bool
    connection_stats: Optional[Dict[str, Any]] = None

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup."""
    global db_connection
    try:
        logger.info("Initializing database connection...")
        db_connection = OracleDB()
        logger.info("Database service started successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database connection: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up database connection on shutdown."""
    global db_connection
    if db_connection:
        logger.info("Closing database connection...")
        # Add cleanup logic if needed
        logger.info("Database service stopped")

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint."""
    return {
        "service": "CandleThrob Database Service",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        database_connected = False
        connection_stats = None
        
        if db_connection:
            try:
                with db_connection.establish_connection() as conn:
                    # Simple connectivity test
                    result = conn.execute("SELECT 1 FROM DUAL")
                    result.fetchone()
                    database_connected = True
                    connection_stats = db_connection.get_connection_stats()
            except Exception as e:
                logger.error(f"Database health check failed: {e}")
        
        return HealthResponse(
            status="healthy" if database_connected else "unhealthy",
            timestamp=datetime.now(),
            database_connected=database_connected,
            connection_stats=connection_stats
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute a SQL query."""
    try:
        if not db_connection:
            raise HTTPException(status_code=500, detail="Database connection not initialized")
        
        with db_connection.establish_connection() as conn:
            with conn.cursor() as cursor:
                if request.params:
                    cursor.execute(request.query, request.params)
                else:
                    cursor.execute(request.query)
                
                # Handle different query types
                if request.query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    return QueryResponse(
                        success=True,
                        data=[dict(zip(columns, row)) for row in results],
                        row_count=len(results)
                    )
                else:
                    conn.commit()
                    return QueryResponse(
                        success=True,
                        message="Query executed successfully",
                        row_count=cursor.rowcount
                    )
                    
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/connection")
async def get_connection_stats():
    """Get database connection statistics."""
    try:
        if not db_connection:
            raise HTTPException(status_code=500, detail="Database connection not initialized")
        
        return db_connection.get_connection_stats()
    except Exception as e:
        logger.error(f"Failed to get connection stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/performance")
async def get_performance_stats():
    """Get database performance statistics."""
    try:
        if not db_connection:
            raise HTTPException(status_code=500, detail="Database connection not initialized")
        
        return db_connection.get_performance_stats()
    except Exception as e:
        logger.error(f"Failed to get performance stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/tickers")
async def get_ticker_data(
    ticker: Optional[str] = Query(None, description="Ticker symbol"),
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, description="Maximum number of records", le=10000)
):
    """Get ticker data with optional filtering."""
    try:
        if not db_connection:
            raise HTTPException(status_code=500, detail="Database connection not initialized")
        
        # Build query with filters
        query = "SELECT * FROM TICKERS WHERE 1=1"
        params = {}
        
        if ticker:
            query += " AND ticker = :ticker"
            params['ticker'] = ticker.upper()
        
        if start_date:
            query += " AND trade_date >= :start_date"
            params['start_date'] = start_date
        
        if end_date:
            query += " AND trade_date <= :end_date"
            params['end_date'] = end_date
        
        query += " ORDER BY trade_date DESC"
        
        if limit:
            query += f" FETCH FIRST {limit} ROWS ONLY"
        
        with db_connection.establish_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                return {
                    "success": True,
                    "data": [dict(zip(columns, row)) for row in results],
                    "record_count": len(results)
                }
                
    except Exception as e:
        logger.error(f"Data fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Start the service
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
