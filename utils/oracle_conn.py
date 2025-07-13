import os
import oracledb
import logging
from contextlib import contextmanager
from CandleThrob.utils.vault import get_secret
from sqlalchemy import create_engine

logging.basicConfig(
    filename="oracle_debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient_23_8")

# Oracle DB credentials from environment variables or vault
ORACLE_USER = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaa6qj7ezmhqtuqxjn7f54xbpfalsszppuhoo6zilyxzra")
ORACLE_PASSWORD = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaf63vh4v5e3ozokfp32cyk2ct6qjo5eb6z5mutkoppl4q")
ORACLE_DSN = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyacow4ffwcgtu6uwsw5ujw5w64eeashh5ndeldxzy3y52q")
if not ORACLE_USER or not ORACLE_PASSWORD or not ORACLE_DSN:
    raise ValueError("Oracle DB credentials are not set in environment variables.")

class OracleDB():
    
    def __init__(self):
        """
        Initialize the OracleDB class.
        This class provides methods to create and manage a SQLAlchemy conn for Oracle DB.
        """
        self.conn = None
        
    def get_sqlalchemy_engine(self):
        """Create a SQLAlchemy engine for pandas operations."""
        try:
            # Create Oracle connection string for SQLAlchemy
            connection_string = f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_DSN}"
            engine = create_engine(connection_string)
            logger.info("SQLAlchemy engine created successfully")
            return engine
        except Exception as e:
            logger.error(f"Error creating SQLAlchemy engine: {e}")
            raise

    @contextmanager
    def establish_connection(self):
        """Create connection if it doesn't exist."""
        
        try:
            logger.info("Establishing Oracle DB connection")
            
            self.conn = oracledb.connect(
                user=ORACLE_USER,
                password=ORACLE_PASSWORD,
                dsn=ORACLE_DSN
            )
            yield self.conn
            
        except oracledb.DatabaseError as e:
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error establishing connection: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Oracle DB connection closed")
                
    def query_adb(self, query : str):
        """
        Execute a query on the Oracle DB.
        
        Args:
            query (str): The SQL query to execute.
            
        Returns:
            list: List of results from the query.
        """
        with self.establish_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logger.info(f"Executed query: {query}")
                return results