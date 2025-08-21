"""
OCI Vault Integration for CandleThrob
====================================

Secure credential management using Oracle Cloud Infrastructure Vault.
Handles secret retrieval, caching, and rotation.
"""

import os
import json
import time
from typing import Dict, Optional, Any
from functools import lru_cache
import oci
from oci.exceptions import ServiceError

from ..core.logging import CandleThrobLogger

class VaultManager:
    """Manages secure credential retrieval from OCI Vault"""
    
    def __init__(self, vault_id: str, region: str = "us-phoenix-1"):
        self.vault_id = vault_id
        self.region = region
        self.logger = CandleThrobLogger("vault_manager")
        
        # Initialize OCI clients
        try:
            self.config = oci.config.from_file()
            self.secrets_client = oci.secrets.SecretsClient(self.config)
            self.vault_client = oci.vault.VaultsClient(self.config)
            self.logger.info("OCI Vault clients initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize OCI clients: {e}")
            raise
    
    @lru_cache(maxsize=32)
    def get_secret(self, secret_name: str, version_number: Optional[int] = None) -> str:
        """
        Retrieve secret from OCI Vault with caching
        
        Args:
            secret_name: Name of the secret in vault
            version_number: Specific version (latest if None)
            
        Returns:
            Secret value as string
        """
        try:
            # Find secret OCID by name
            secret_ocid = self._get_secret_ocid(secret_name)
            
            # Get secret bundle
            get_secret_bundle_response = self.secrets_client.get_secret_bundle(
                secret_id=secret_ocid,
                version_number=version_number
            )
            
            # Decode base64 content
            import base64
            secret_content = base64.b64decode(
                get_secret_bundle_response.data.secret_bundle_content.content
            ).decode('utf-8')
            
            self.logger.info(f"Successfully retrieved secret: {secret_name}")
            return secret_content
            
        except ServiceError as e:
            self.logger.error(f"OCI Service error retrieving secret {secret_name}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error retrieving secret {secret_name}: {e}")
            raise
    
    def _get_secret_ocid(self, secret_name: str) -> str:
        """Get secret OCID from name"""
        # This would typically involve listing secrets and finding by name
        # For now, assume secret name is configured in environment
        secret_ocid = os.getenv(f"OCI_SECRET_{secret_name.upper()}_OCID")
        if not secret_ocid:
            raise ValueError(f"Secret OCID not found for {secret_name}")
        return secret_ocid
    
    def get_database_credentials(self) -> Dict[str, str]:
        """Get database connection credentials"""
        return {
            'username': os.getenv('DB_USERNAME'),  # Non-sensitive, from env
            'password': self.get_secret('candlethrob-db-password'),
            'dsn': os.getenv('DB_DSN')  # Non-sensitive, from env
        }
    
    def get_polygon_api_key(self) -> str:
        """Get Polygon.io API key"""
        return self.get_secret('candlethrob-polygon-api-key')
    
    def get_email_credentials(self) -> Dict[str, str]:
        """Get email SMTP credentials"""
        return {
            'host': os.getenv('SMTP_HOST'),
            'port': int(os.getenv('SMTP_PORT', '587')),
            'username': os.getenv('SMTP_USERNAME'),
            'password': self.get_secret('candlethrob-smtp-password')
        }
    
    def get_airflow_credentials(self) -> Dict[str, str]:
        """Get Airflow admin credentials"""
        return {
            'username': os.getenv('AIRFLOW_ADMIN_USERNAME', 'admin'),
            'password': self.get_secret('candlethrob-airflow-admin-password'),
            'firstname': os.getenv('AIRFLOW_ADMIN_FIRSTNAME', 'CandleThrob'),
            'lastname': os.getenv('AIRFLOW_ADMIN_LASTNAME', 'Admin'),
            'email': os.getenv('AIRFLOW_ADMIN_EMAIL', 'admin@candlethrob.local')
        }
    
    def get_airflow_secret_key(self) -> str:
        """Get Airflow webserver secret key"""
        return self.get_secret('candlethrob-airflow-secret-key')
    
    def health_check(self) -> bool:
        """Check vault connectivity"""
        try:
            # Try to list secrets to verify connection
            self.vault_client.list_secrets(
                compartment_id=self.config['tenancy'],
                vault_id=self.vault_id,
                limit=1
            )
            return True
        except Exception as e:
            self.logger.error(f"Vault health check failed: {e}")
            return False


class SecureConfigManager:
    """Manages configuration combining environment and vault sources"""
    
    def __init__(self, vault_id: str, environment: str = "development"):
        self.environment = environment
        self.logger = CandleThrobLogger("secure_config")
        
        # Initialize vault manager
        try:
            self.vault_manager = VaultManager(vault_id)
        except Exception as e:
            self.logger.warning(f"Vault initialization failed: {e}. Using fallback mode.")
            self.vault_manager = None
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get complete database configuration"""
        if self.vault_manager:
            creds = self.vault_manager.get_database_credentials()
        else:
            # Fallback to environment (less secure)
            creds = {
                'username': os.getenv('DB_USERNAME'),
                'password': os.getenv('DB_PASSWORD'),
                'dsn': os.getenv('DB_DSN')
            }
        
        # Add non-sensitive config from environment
        return {
            **creds,
            'pool_size': int(os.getenv('DB_POOL_SIZE', '10')),
            'max_overflow': int(os.getenv('DB_MAX_OVERFLOW', '20')),
            'pool_timeout': int(os.getenv('DB_POOL_TIMEOUT', '30'))
        }
    
    def get_api_config(self) -> Dict[str, str]:
        """Get API configuration"""
        polygon_key = (
            self.vault_manager.get_polygon_api_key() 
            if self.vault_manager 
            else os.getenv('POLYGON_API_KEY')
        )
        
        return {
            'polygon_api_key': polygon_key,
            'rate_limit': int(os.getenv('API_RATE_LIMIT', '5')),
            'timeout': int(os.getenv('API_TIMEOUT', '30'))
        }