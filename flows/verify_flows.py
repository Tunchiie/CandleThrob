#!/usr/bin/env python3
"""
Flow Verification Script for CandleThrob
Verifies that flows are properly accessible and can be loaded by Kestra
"""

import os
import yaml
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def verify_flows_directory():
    """Verify that flows directory exists and contains valid YAML files"""
    flows_dir = Path("/app/flows")
    
    if not flows_dir.exists():
        logger.error(f"Flows directory does not exist: {flows_dir}")
        return False
    
    logger.info(f"Flows directory exists: {flows_dir}")
    
    # List all YAML files
    yaml_files = list(flows_dir.glob("*.yaml")) + list(flows_dir.glob("*.yml"))
    
    if not yaml_files:
        logger.error("No YAML files found in flows directory")
        return False
    
    logger.info(f"Found {len(yaml_files)} YAML files:")
    
    valid_flows = []
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r') as f:
                flow_data = yaml.safe_load(f)
            
            # Check required fields
            if 'id' not in flow_data:
                logger.error(f"Flow {yaml_file.name} missing 'id' field")
                continue
            
            if 'namespace' not in flow_data:
                logger.error(f"Flow {yaml_file.name} missing 'namespace' field")
                continue
            
            if 'tasks' not in flow_data:
                logger.error(f"Flow {yaml_file.name} missing 'tasks' field")
                continue
            
            logger.info(f"✅ {yaml_file.name}: {flow_data['id']} (namespace: {flow_data['namespace']})")
            valid_flows.append(yaml_file.name)
            
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML in {yaml_file.name}: {e}")
        except Exception as e:
            logger.error(f"Error reading {yaml_file.name}: {e}")
    
    logger.info(f"Valid flows: {valid_flows}")
    return len(valid_flows) > 0

def verify_docker_compose_mapping():
    """Verify that the docker-compose volume mapping is correct"""
    logger.info("Checking docker-compose volume mapping...")
    
    # Check if we're in the container
    if os.path.exists("/app/flows"):
        logger.info("✅ /app/flows directory exists in container")
        
        # List contents
        flows_contents = os.listdir("/app/flows")
        logger.info(f"Contents of /app/flows: {flows_contents}")
        
        return True
    else:
        logger.error("❌ /app/flows directory does not exist in container")
        return False

def main():
    """Main verification function"""
    logger.info("Starting flow verification...")
    
    # Check docker-compose mapping
    mapping_ok = verify_docker_compose_mapping()
    
    # Check flows directory
    flows_ok = verify_flows_directory()
    
    if mapping_ok and flows_ok:
        logger.info("✅ Flow verification completed successfully!")
        print("\n=== FLOW VERIFICATION RESULTS ===")
        print("✅ Docker volume mapping: OK")
        print("✅ Flows directory: OK")
        print("✅ YAML files: Valid")
        print("\nFlows should now be visible in Kestra UI at http://localhost:8080")
        return True
    else:
        logger.error("❌ Flow verification failed!")
        print("\n=== FLOW VERIFICATION RESULTS ===")
        print(f"❌ Docker volume mapping: {'OK' if mapping_ok else 'FAILED'}")
        print(f"❌ Flows directory: {'OK' if flows_ok else 'FAILED'}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 