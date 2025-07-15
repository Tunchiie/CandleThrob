#!/usr/bin/env python3
"""
CandleThrob Main Entry Point
===========================

This module serves as the main entry point for the CandleThrob application
when run as a module with `python -m CandleThrob`.

Usage:
    python -m CandleThrob [command]

Commands:
    ingest      - Run data ingestion pipeline
    transform   - Run data transformation pipeline  
    validate    - Run data validation
    macro       - Update macro data
    help        - Show this help message

If no command is provided, runs the default ingestion pipeline.
"""

import sys
import os
import argparse
from typing import Optional

# Add the parent directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_ingestion():
    """Run the data ingestion pipeline."""
    try:
        from CandleThrob.ingestion.ingest_data import main
        print("🚀 Starting CandleThrob data ingestion pipeline...")
        main()
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)

def run_transform():
    """Run the data transformation pipeline."""
    try:
        from CandleThrob.transform.transform_data import main
        print("🔄 Starting CandleThrob data transformation pipeline...")
        main()
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        sys.exit(1)

def run_validation():
    """Run data validation."""
    try:
        from CandleThrob.ingestion.validate_data import main
        print("✅ Starting CandleThrob data validation...")
        main()
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        sys.exit(1)

def run_macro_update():
    """Run macro data update."""
    try:
        from CandleThrob.ingestion.update_macro import main
        print("📊 Starting CandleThrob macro data update...")
        main()
    except Exception as e:
        print(f"❌ Macro update failed: {e}")
        sys.exit(1)

def show_help():
    """Show help message."""
    print(__doc__)

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="CandleThrob Financial Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m CandleThrob                    # Run default ingestion
    python -m CandleThrob ingest            # Run ingestion pipeline
    python -m CandleThrob transform         # Run transformation pipeline
    python -m CandleThrob validate          # Run data validation
    python -m CandleThrob macro             # Update macro data
        """
    )
    
    parser.add_argument(
        'command',
        nargs='?',
        default='ingest',
        choices=['ingest', 'transform', 'validate', 'macro', 'help'],
        help='Command to run (default: ingest)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🎯 CandleThrob Financial Data Pipeline")
    print("=" * 60)
    
    if args.command == 'ingest':
        run_ingestion()
    elif args.command == 'transform':
        run_transform()
    elif args.command == 'validate':
        run_validation()
    elif args.command == 'macro':
        run_macro_update()
    elif args.command == 'help':
        show_help()
    else:
        print(f"❌ Unknown command: {args.command}")
        show_help()
        sys.exit(1)
    
    print("=" * 60)
    print("✅ CandleThrob pipeline completed successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
