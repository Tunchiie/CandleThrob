from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

class OracleDB():
    
    def __init__(self):
        """
        Initialize the OracleDB class.
        This class provides methods to create and manage a SQLAlchemy session for Oracle DB.
        """
        self.engine = None
        self.Session = None

    def _get_engine(self):
        """Create engine if it doesn't exist."""
        if self.engine is None:
            self.engine = create_engine("sqlite:///local_test.db", echo=False)
            self.Session = sessionmaker(bind=self.engine)
        return self.engine

    @contextmanager
    def get_oracledb_session(self):
        """
        Create a new SQLAlchemy session for Oracle DB as a context manager.
        Returns:
            Session: SQLAlchemy session object
        """
        self._get_engine()
        if self.Session is None:
            raise RuntimeError("Session factory not initialized")
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def test_connection(self):
        """
        Test the connection to the Oracle database.
        
        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            with self.get_oracledb_session() as session:
                # Test the connection with a simple query
                from sqlalchemy import text
                result = session.execute(text("SELECT 1")).fetchone()
                if result:
                    print("Oracle DB connection test successful.")
                    return True
                else:
                    print("Oracle DB connection test failed.")
                    return False
        except Exception as e:
            print(f"Oracle DB connection test failed with error: {e}")
            return False