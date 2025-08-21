#!/usr/bin/env python3
"""
CLI for Personal Finance Optimization
=====================================

Simple command-line interface to test and run budgeting analysis.
"""

import argparse
import os
import sys
from typing import List
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from optimization.budgeting import PersonalFinanceOptimizer, FinancialGoal
from optimization.finance_config import get_finance_config


def test_plaid_connection():
    """Test Plaid API connection."""
    print("🔗 Testing Plaid connection...")
    
    config = get_finance_config()
    if not config.plaid.client_id or not config.plaid.secret:
        print("❌ Plaid credentials not found. Set PLAID_CLIENT_ID and PLAID_SECRET environment variables.")
        return False
    
    try:
        from optimization.budgeting import PlaidDataProcessor
        processor = PlaidDataProcessor(config.plaid.client_id, config.plaid.secret, config.plaid.environment)
        print(f"✅ Plaid connection successful (Environment: {config.plaid.environment})")
        return True
    except Exception as e:
        print(f"❌ Plaid connection failed: {e}")
        return False


def analyze_demo_data():
    """Run analysis with demo/mock data."""
    print("📊 Running demo financial analysis...")
    
    # Demo goals
    goals = [
        FinancialGoal(
            name="Emergency Fund",
            target_amount=25000,
            target_date=datetime(2025, 12, 31),
            priority=1
        ),
        FinancialGoal(
            name="House Down Payment", 
            target_amount=75000,
            target_date=datetime(2026, 6, 1),
            priority=2
        )
    ]
    
    # Mock recommendation
    from optimization.budgeting import BudgetRecommendation
    demo_recommendation = BudgetRecommendation(
        monthly_income=8000.0,
        monthly_expenses=5500.0,
        available_for_savings=2500.0,
        available_for_investment=1750.0,
        emergency_fund_target=33000.0,
        emergency_fund_current=15000.0,
        subscription_savings_potential=150.0
    )
    
    print_analysis_results(demo_recommendation, "Demo Analysis")


def analyze_with_plaid(access_token: str):
    """Run analysis with real Plaid data."""
    print("🏦 Analyzing with Plaid data...")
    
    config = get_finance_config()
    optimizer = PersonalFinanceOptimizer(config.plaid.client_id, config.plaid.secret)
    
    # Example goals - in real usage, these would come from user input
    goals = [
        FinancialGoal(
            name="Emergency Fund",
            target_amount=30000,
            target_date=datetime(2025, 12, 31),
            priority=1
        )
    ]
    
    try:
        recommendation = optimizer.analyze_finances(access_token, goals)
        investment_recs = optimizer.get_investment_recommendations(recommendation)
        
        print_analysis_results(recommendation, "Plaid Analysis")
        print_investment_recommendations(investment_recs)
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")


def print_analysis_results(recommendation, title: str):
    """Print formatted analysis results."""
    print(f"\n{'='*60}")
    print(f"{title.upper()}")
    print('='*60)
    print(f"💰 Monthly Income:           ${recommendation.monthly_income:,.2f}")
    print(f"💸 Monthly Expenses:         ${recommendation.monthly_expenses:,.2f}")
    print(f"💾 Available for Savings:    ${recommendation.available_for_savings:,.2f}")
    print(f"📈 Available for Investment: ${recommendation.available_for_investment:,.2f}")
    print(f"🛡️  Emergency Fund Target:    ${recommendation.emergency_fund_target:,.2f}")
    print(f"🛡️  Emergency Fund Current:   ${recommendation.emergency_fund_current:,.2f}")
    print(f"🔄 Subscription Savings:     ${recommendation.subscription_savings_potential:,.2f}")
    
    if recommendation.recommended_vaults:
        print(f"\n📦 Recommended Vaults:")
        for vault in recommendation.recommended_vaults:
            print(f"   • {vault['type']}: ${vault['monthly_contribution']:.2f}/month")
    
    if recommendation.investment_allocation:
        print(f"\n📊 Investment Allocation:")
        for asset, percentage in recommendation.investment_allocation.items():
            amount = recommendation.available_for_investment * percentage
            print(f"   • {asset}: {percentage:.1%} (${amount:,.2f}/month)")


def print_investment_recommendations(investment_recs):
    """Print investment recommendations."""
    if 'specific_recommendations' in investment_recs:
        print(f"\n🎯 CandleThrob Integration Recommendations:")
        for rec in investment_recs['specific_recommendations']:
            print(f"   • {rec['type']}: ${rec['monthly_amount']:.2f}/month")
            print(f"     Rationale: {rec['rationale']}")


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(description='CandleThrob Personal Finance Optimization CLI')
    parser.add_argument('--test-plaid', action='store_true', help='Test Plaid API connection')
    parser.add_argument('--demo', action='store_true', help='Run demo analysis with mock data')
    parser.add_argument('--access-token', type=str, help='Plaid access token for real analysis')
    parser.add_argument('--setup-guide', action='store_true', help='Show setup instructions')
    
    args = parser.parse_args()
    
    if args.setup_guide:
        print_setup_guide()
    elif args.test_plaid:
        test_plaid_connection()
    elif args.demo:
        analyze_demo_data()
    elif args.access_token:
        analyze_with_plaid(args.access_token)
    else:
        print("🚀 CandleThrob Personal Finance Optimization")
        print("Use --help for available commands")
        print("Start with: python cli.py --demo")


def print_setup_guide():
    """Print setup instructions."""
    print("""
🚀 CandleThrob Personal Finance Optimization Setup Guide
========================================================

1. Get Plaid API Credentials:
   • Sign up at https://plaid.com/
   • Get client_id and secret from dashboard
   • Start with sandbox environment

2. Set Environment Variables:
   export PLAID_CLIENT_ID='your_client_id'
   export PLAID_SECRET='your_secret'
   export PLAID_ENVIRONMENT='sandbox'

3. Test Connection:
   python cli.py --test-plaid

4. Run Demo Analysis:
   python cli.py --demo

5. Integration with CandleThrob:
   • Budgeting data will integrate with your existing stock analysis
   • Investment recommendations will use CandleThrob market data
   • Portfolio optimization will consider both personal finance and market conditions

Next Steps:
• Complete Plaid Link flow to get access_token
• Run real analysis: python cli.py --access-token YOUR_TOKEN
• Build dashboard integration (future)
""")


if __name__ == "__main__":
    main()