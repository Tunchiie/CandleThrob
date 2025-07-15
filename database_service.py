#!/usr/bin/env python3
"""
Database Service for Kestra Integration
=======================================

This service provides a REST API interface for database operations,
eliminating the need to mount Oracle wallets in ephemeral containers.

Usage:
    python database_service.py

The service will start on port 8000 and provide endpoints for:
- /health: Health check
- /execute: Execute SQL queries
- /ingest: Bulk data ingestion
- /fetch: Data retrieval
"""

import os
import sys
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass
import traceback

# FastAPI for REST API
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.logging_config import get_ingestion_logger

# Initialize logger
logger = get_ingestion_logger(__name__)

# FastAPI app
app = FastAPI(
    title="CandleThrob Database Service",
    description="REST API for database operations in Kestra workflows",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global database connection
db_connection: Optional[OracleDB] = None

# Request/Response models
class QueryRequest(BaseModel):
    query: str
    params: Optional[List[Any]] = None

class IngestRequest(BaseModel):
    ticker: str
    data: List[Dict[str, Any]]

class FetchRequest(BaseModel):
    ticker: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    database_connected: bool

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

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        # Test database connection
        db_connected = False
        if db_connection:
            with db_connection.establish_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM DUAL")
                    result = cursor.fetchone()
                    db_connected = result is not None
        
        return HealthResponse(
            status="healthy" if db_connected else "unhealthy",
            timestamp=datetime.now().isoformat(),
            database_connected=db_connected
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.now().isoformat(),
            database_connected=False
        )

@app.post("/execute")
async def execute_query(request: QueryRequest):
    """Execute a SQL query."""
    try:
        if not db_connection:
            raise HTTPException(status_code=500, detail="Database not connected")
        
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
                    return {
                        "success": True,
                        "data": [dict(zip(columns, row)) for row in results],
                        "row_count": len(results)
                    }
                else:
                    conn.commit()
                    return {
                        "success": True,
                        "message": "Query executed successfully",
                        "row_count": cursor.rowcount
                    }
                    
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest")
async def ingest_data(request: IngestRequest, background_tasks: BackgroundTasks):
    """Ingest ticker data."""
    try:
        if not db_connection:
            raise HTTPException(status_code=500, detail="Database not connected")
        
        def process_ingestion():
            try:
                with db_connection.establish_connection() as conn:
                    with conn.cursor() as cursor:
                        # Create table if not exists
                        cursor.execute("""
                            BEGIN
                                EXECUTE IMMEDIATE 'CREATE TABLE ticker_data (
                                    ticker VARCHAR2(10),
                                    date_col DATE,
                                    open_price NUMBER(10,2),
                                    high_price NUMBER(10,2),
                                    low_price NUMBER(10,2),
                                    close_price NUMBER(10,2),
                                    volume NUMBER(15),
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    CONSTRAINT pk_ticker_data PRIMARY KEY (ticker, date_col)
                                )';
                            EXCEPTION
                                WHEN OTHERS THEN
                                    IF SQLCODE != -955 THEN -- Table already exists
                                        RAISE;
                                    END IF;
                            END;
                        """)
                        
                        # Insert data
                        for record in request.data:
                            cursor.execute("""
                                MERGE INTO ticker_data t
                                USING (SELECT :1 as ticker, TO_DATE(:2, 'YYYY-MM-DD') as date_col,
                                             :3 as open_price, :4 as high_price, :5 as low_price,
                                             :6 as close_price, :7 as volume FROM dual) s
                                ON (t.ticker = s.ticker AND t.date_col = s.date_col)
                                WHEN NOT MATCHED THEN
                                    INSERT (ticker, date_col, open_price, high_price, low_price, close_price, volume)
                                    VALUES (s.ticker, s.date_col, s.open_price, s.high_price, s.low_price, s.close_price, s.volume)
                            """, [
                                request.ticker,
                                record['date'],
                                record['open'],
                                record['high'],
                                record['low'],
                                record['close'],
                                record['volume']
                            ])
                        
                        conn.commit()
                        logger.info(f"Successfully ingested {len(request.data)} records for {request.ticker}")
                        
            except Exception as e:
                logger.error(f"Background ingestion failed for {request.ticker}: {e}")
        
        # Process in background
        background_tasks.add_task(process_ingestion)
        
        return {
            "success": True,
            "message": f"Ingestion started for {request.ticker}",
            "record_count": len(request.data)
        }
        
    except Exception as e:
        logger.error(f"Ingestion request failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/fetch")
async def fetch_data(request: FetchRequest):
    """Fetch ticker data."""
    try:
        if not db_connection:
            raise HTTPException(status_code=500, detail="Database not connected")
        
        query = """
            SELECT ticker, date_col, open_price, high_price, low_price, close_price, volume
            FROM ticker_data
            WHERE ticker = :1
        """
        params = [request.ticker]
        
        if request.start_date:
            query += " AND date_col >= TO_DATE(:2, 'YYYY-MM-DD')"
            params.append(request.start_date)
        
        if request.end_date:
            query += " AND date_col <= TO_DATE(:3, 'YYYY-MM-DD')"
            params.append(request.end_date)
        
        query += " ORDER BY date_col"
        
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
