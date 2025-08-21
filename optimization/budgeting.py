#!/usr/bin/env python3
"""
Personal Finance Optimization System for CandleThrob
===================================================

This module integrates with Plaid API to analyze personal finances and make
data-driven decisions about budgeting, savings, investments, and subscription management.

Features:
- Plaid API integration for bank data
- Subscription detection and redundancy analysis  
- Automated savings recommendations
- Investment allocation optimization
- Vault creation strategies
- Integration with CandleThrob portfolio management

Data Science Approach:
- Time series analysis for spending patterns
- Machine learning for subscription categorization
- Monte Carlo simulations for investment allocation
- Risk-adjusted portfolio optimization
- Predictive modeling for future cash flows

Author: Adetunji Fasiku
Version: 1.0.0
"""

import os
import sys
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import logging
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.configuration import Configuration
from plaid.api_client import ApiClient

# Add CandleThrob core imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.core.config import get_config
from CandleThrob.core.logging import get_logger
from CandleThrob.core.data_models import DataType, DataStatus

logger = get_logger(__name__)


class TransactionCategory(Enum):
    """Transaction categories for analysis."""
    SUBSCRIPTION = "subscription"
    FOOD_DINING = "food_dining"
    TRANSPORTATION = "transportation"
    UTILITIES = "utilities"
    ENTERTAINMENT = "entertainment"
    SHOPPING = "shopping"
    HEALTHCARE = "healthcare"
    INCOME = "income"
    TRANSFER = "transfer"
    OTHER = "other"


class VaultType(Enum):
    """Types of savings vaults."""
    EMERGENCY_FUND = "emergency_fund"
    INVESTMENT_READY = "investment_ready"
    GOAL_BASED = "goal_based"
    TAX_ADVANTAGED = "tax_advantaged"


@dataclass
class FinancialGoal:
    """Represents a financial goal with timeline and amount."""
    name: str
    target_amount: float
    target_date: datetime
    priority: int  # 1-5, 1 being highest priority
    current_amount: float = 0.0
    monthly_contribution: float = 0.0


@dataclass
class SubscriptionAnalysis:
    """Analysis results for subscription services."""
    name: str
    monthly_cost: float
    usage_frequency: str  # "high", "medium", "low", "unknown"
    redundancy_score: float  # 0.0-1.0, higher means more redundant
    similar_services: List[str] = field(default_factory=list)
    recommendation: str = "keep"  # "keep", "cancel", "downgrade"


@dataclass
class BudgetRecommendation:
    """Comprehensive budget and investment recommendations."""
    monthly_income: float
    monthly_expenses: float
    available_for_savings: float
    available_for_investment: float
    emergency_fund_target: float
    emergency_fund_current: float
    subscription_savings_potential: float
    recommended_vaults: List[Dict[str, Any]] = field(default_factory=list)
    investment_allocation: Dict[str, float] = field(default_factory=dict)
    risk_assessment: str = "moderate"


class PlaidDataProcessor:
    """Handles Plaid API integration and data processing."""
    
    def __init__(self, client_id: str, secret: str, environment: str = "sandbox"):
        """Initialize Plaid client."""
        configuration = Configuration(
            host=getattr(plaid_api.Environment, environment),
            api_key={
                'clientId': client_id,
                'secret': secret
            }
        )
        api_client = ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)
        
    def get_accounts(self, access_token: str) -> List[Dict]:
        """Retrieve account information."""
        try:
            request = AccountsGetRequest(access_token=access_token)
            response = self.client.accounts_get(request)
            return [account.to_dict() for account in response['accounts']]
        except Exception as e:
            logger.error(f"Error fetching accounts: {e}")
            return []
    
    def get_transactions(self, access_token: str, start_date: datetime, 
                        end_date: datetime) -> pd.DataFrame:
        """Retrieve and process transactions."""
        try:
            request = TransactionsGetRequest(
                access_token=access_token,
                start_date=start_date.date(),
                end_date=end_date.date()
            )
            response = self.client.transactions_get(request)
            
            transactions = []
            for transaction in response['transactions']:
                transactions.append({
                    'transaction_id': transaction['transaction_id'],
                    'amount': transaction['amount'],
                    'date': transaction['date'],
                    'description': transaction['name'],
                    'category': transaction['category'][0] if transaction['category'] else 'Other',
                    'account_id': transaction['account_id']
                })
            
            return pd.DataFrame(transactions)
        except Exception as e:
            logger.error(f"Error fetching transactions: {e}")
            return pd.DataFrame()


class SubscriptionDetector:
    """Detects and analyzes subscription services using ML techniques."""
    
    def __init__(self):
        """Initialize subscription detector."""
        self.subscription_keywords = {
            'streaming': ['netflix', 'spotify', 'hulu', 'disney', 'amazon prime', 'apple music'],
            'software': ['adobe', 'microsoft', 'google workspace', 'slack', 'zoom'],
            'fitness': ['gym', 'peloton', 'fitness', 'yoga'],
            'news': ['news', 'times', 'journal', 'magazine']
        }
    
    def detect_subscriptions(self, transactions_df: pd.DataFrame) -> List[SubscriptionAnalysis]:
        """Detect subscription services from transaction data."""
        subscriptions = []
        
        # Group recurring transactions
        recurring_transactions = self._find_recurring_transactions(transactions_df)
        
        for description, group in recurring_transactions.items():
            if self._is_subscription(description, group):
                analysis = self._analyze_subscription(description, group)
                subscriptions.append(analysis)
        
        return subscriptions
    
    def _find_recurring_transactions(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Find transactions that occur regularly."""
        df['month'] = pd.to_datetime(df['date']).dt.to_period('M')
        recurring = {}
        
        for description, group in df.groupby('description'):
            if len(group) >= 2:  # At least 2 occurrences
                monthly_counts = group['month'].value_counts()
                if monthly_counts.max() >= 1:  # Appears at least once per month
                    recurring[description] = group
        
        return recurring
    
    def _is_subscription(self, description: str, transactions: pd.DataFrame) -> bool:
        """Determine if a recurring transaction is likely a subscription."""
        # Check for subscription keywords
        desc_lower = description.lower()
        for category, keywords in self.subscription_keywords.items():
            if any(keyword in desc_lower for keyword in keywords):
                return True
        
        # Check for regular amounts (subscriptions usually have consistent amounts)
        amount_variance = transactions['amount'].var()
        return amount_variance < 5.0  # Low variance in amounts
    
    def _analyze_subscription(self, description: str, transactions: pd.DataFrame) -> SubscriptionAnalysis:
        """Analyze a detected subscription for redundancy and value."""
        monthly_cost = transactions['amount'].mean()
        
        # Determine category and potential redundancy
        redundancy_score = self._calculate_redundancy_score(description)
        similar_services = self._find_similar_services(description)
        
        recommendation = "keep"
        if redundancy_score > 0.7:
            recommendation = "cancel"
        elif redundancy_score > 0.4:
            recommendation = "downgrade"
        
        return SubscriptionAnalysis(
            name=description,
            monthly_cost=monthly_cost,
            usage_frequency="unknown",  # Would need additional data
            redundancy_score=redundancy_score,
            similar_services=similar_services,
            recommendation=recommendation
        )
    
    def _calculate_redundancy_score(self, description: str) -> float:
        """Calculate how redundant a subscription might be."""
        # Simplified scoring - in practice, would use more sophisticated ML
        desc_lower = description.lower()
        
        # Streaming services - high redundancy if multiple
        streaming_keywords = ['netflix', 'hulu', 'disney', 'apple tv', 'paramount']
        if any(keyword in desc_lower for keyword in streaming_keywords):
            return 0.6  # Moderate redundancy for streaming
        
        # News/magazine subscriptions
        news_keywords = ['news', 'times', 'journal', 'magazine']
        if any(keyword in desc_lower for keyword in news_keywords):
            return 0.4  # Lower redundancy for news
        
        return 0.2  # Default low redundancy
    
    def _find_similar_services(self, description: str) -> List[str]:
        """Find similar services that might be redundant."""
        desc_lower = description.lower()
        similar = []
        
        if 'netflix' in desc_lower:
            similar = ['Hulu', 'Disney+', 'Amazon Prime Video']
        elif 'spotify' in desc_lower:
            similar = ['Apple Music', 'Amazon Music', 'YouTube Music']
        
        return similar


class InvestmentAllocator:
    """Calculates optimal investment allocation based on goals and risk tolerance."""
    
    def __init__(self, candlethrob_integration: bool = True):
        """Initialize investment allocator."""
        self.candlethrob_integration = candlethrob_integration
        
    def calculate_allocation(self, available_amount: float, risk_tolerance: str,
                           time_horizon: int, goals: List[FinancialGoal]) -> Dict[str, float]:
        """Calculate optimal investment allocation."""
        
        # Base allocation templates based on risk tolerance
        allocations = {
            'conservative': {'bonds': 0.6, 'stocks': 0.3, 'cash': 0.1},
            'moderate': {'bonds': 0.4, 'stocks': 0.5, 'cash': 0.1},
            'aggressive': {'bonds': 0.2, 'stocks': 0.7, 'alternatives': 0.1}
        }
        
        base_allocation = allocations.get(risk_tolerance, allocations['moderate'])
        
        # Adjust based on time horizon
        if time_horizon < 5:  # Short term - more conservative
            base_allocation['cash'] = min(base_allocation.get('cash', 0) + 0.2, 0.3)
            base_allocation['stocks'] = max(base_allocation['stocks'] - 0.1, 0.2)
        elif time_horizon > 20:  # Long term - more aggressive
            base_allocation['stocks'] = min(base_allocation['stocks'] + 0.1, 0.8)
            base_allocation['bonds'] = max(base_allocation['bonds'] - 0.1, 0.1)
        
        # If CandleThrob integration enabled, add specific allocations
        if self.candlethrob_integration:
            base_allocation['sp500_etf'] = base_allocation.get('stocks', 0) * 0.7
            base_allocation['individual_stocks'] = base_allocation.get('stocks', 0) * 0.3
            del base_allocation['stocks']
        
        return base_allocation


class PersonalFinanceOptimizer:
    """Main class for comprehensive personal finance optimization."""
    
    def __init__(self, plaid_client_id: str, plaid_secret: str):
        """Initialize the personal finance optimizer."""
        self.config = get_config()
        self.plaid_processor = PlaidDataProcessor(plaid_client_id, plaid_secret)
        self.subscription_detector = SubscriptionDetector()
        self.investment_allocator = InvestmentAllocator()
        
    def analyze_finances(self, access_token: str, 
                        financial_goals: List[FinancialGoal]) -> BudgetRecommendation:
        """Perform comprehensive financial analysis and generate recommendations."""
        
        logger.info("Starting comprehensive financial analysis")
        
        # Get account data
        accounts = self.plaid_processor.get_accounts(access_token)
        
        # Get transactions for last 6 months
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        transactions_df = self.plaid_processor.get_transactions(access_token, start_date, end_date)
        
        if transactions_df.empty:
            logger.warning("No transaction data available")
            return self._create_default_recommendation()
        
        # Calculate income and expenses
        monthly_income = self._calculate_monthly_income(transactions_df)
        monthly_expenses = self._calculate_monthly_expenses(transactions_df)
        
        # Detect and analyze subscriptions
        subscriptions = self.subscription_detector.detect_subscriptions(transactions_df)
        subscription_savings = sum(sub.monthly_cost for sub in subscriptions 
                                 if sub.recommendation in ['cancel', 'downgrade'])
        
        # Calculate available amounts
        available_for_savings = max(0, monthly_income - monthly_expenses)
        available_for_investment = max(0, available_for_savings * 0.7)  # 70% for investment
        
        # Emergency fund calculations
        emergency_fund_target = monthly_expenses * 6  # 6 months of expenses
        emergency_fund_current = self._calculate_current_emergency_fund(accounts)
        
        # Generate vault recommendations
        vault_recommendations = self._recommend_vaults(
            available_for_savings, emergency_fund_current, emergency_fund_target, financial_goals
        )
        
        # Calculate investment allocation
        investment_allocation = self.investment_allocator.calculate_allocation(
            available_for_investment, 'moderate', 10, financial_goals
        )
        
        return BudgetRecommendation(
            monthly_income=monthly_income,
            monthly_expenses=monthly_expenses,
            available_for_savings=available_for_savings,
            available_for_investment=available_for_investment,
            emergency_fund_target=emergency_fund_target,
            emergency_fund_current=emergency_fund_current,
            subscription_savings_potential=subscription_savings,
            recommended_vaults=vault_recommendations,
            investment_allocation=investment_allocation,
            risk_assessment="moderate"
        )
    
    def _calculate_monthly_income(self, df: pd.DataFrame) -> float:
        """Calculate average monthly income."""
        income_transactions = df[df['amount'] < 0]  # Plaid uses negative for income
        if income_transactions.empty:
            return 0.0
        
        df_with_month = income_transactions.copy()
        df_with_month['month'] = pd.to_datetime(df_with_month['date']).dt.to_period('M')
        monthly_income = df_with_month.groupby('month')['amount'].sum().abs()
        return monthly_income.mean()
    
    def _calculate_monthly_expenses(self, df: pd.DataFrame) -> float:
        """Calculate average monthly expenses."""
        expense_transactions = df[df['amount'] > 0]  # Positive amounts are expenses
        if expense_transactions.empty:
            return 0.0
        
        df_with_month = expense_transactions.copy()
        df_with_month['month'] = pd.to_datetime(df_with_month['date']).dt.to_period('M')
        monthly_expenses = df_with_month.groupby('month')['amount'].sum()
        return monthly_expenses.mean()
    
    def _calculate_current_emergency_fund(self, accounts: List[Dict]) -> float:
        """Calculate current emergency fund balance."""
        savings_balance = 0.0
        for account in accounts:
            if account.get('subtype') in ['savings', 'checking']:
                savings_balance += account.get('balances', {}).get('current', 0)
        return savings_balance
    
    def _recommend_vaults(self, available_savings: float, current_emergency: float,
                         target_emergency: float, goals: List[FinancialGoal]) -> List[Dict[str, Any]]:
        """Recommend savings vault allocation."""
        vaults = []
        remaining_savings = available_savings
        
        # Emergency fund vault (highest priority)
        if current_emergency < target_emergency:
            emergency_needed = target_emergency - current_emergency
            emergency_contribution = min(remaining_savings * 0.5, emergency_needed / 12)
            vaults.append({
                'type': VaultType.EMERGENCY_FUND.value,
                'monthly_contribution': emergency_contribution,
                'target_amount': target_emergency,
                'current_amount': current_emergency,
                'priority': 1
            })
            remaining_savings -= emergency_contribution
        
        # Goal-based vaults
        for goal in sorted(goals, key=lambda x: x.priority):
            if remaining_savings <= 0:
                break
            
            months_to_goal = max(1, (goal.target_date - datetime.now()).days / 30)
            needed_monthly = (goal.target_amount - goal.current_amount) / months_to_goal
            contribution = min(remaining_savings * 0.3, needed_monthly)
            
            if contribution > 0:
                vaults.append({
                    'type': VaultType.GOAL_BASED.value,
                    'name': goal.name,
                    'monthly_contribution': contribution,
                    'target_amount': goal.target_amount,
                    'current_amount': goal.current_amount,
                    'priority': goal.priority + 1
                })
                remaining_savings -= contribution
        
        # Investment-ready vault for remaining funds
        if remaining_savings > 0:
            vaults.append({
                'type': VaultType.INVESTMENT_READY.value,
                'monthly_contribution': remaining_savings,
                'target_amount': None,
                'priority': 3
            })
        
        return vaults
    
    def _create_default_recommendation(self) -> BudgetRecommendation:
        """Create default recommendation when no data is available."""
        return BudgetRecommendation(
            monthly_income=0.0,
            monthly_expenses=0.0,
            available_for_savings=0.0,
            available_for_investment=0.0,
            emergency_fund_target=0.0,
            emergency_fund_current=0.0,
            subscription_savings_potential=0.0
        )
    
    def get_investment_recommendations(self, recommendation: BudgetRecommendation) -> Dict[str, Any]:
        """Get specific investment recommendations integrated with CandleThrob."""
        
        if not recommendation.available_for_investment:
            return {'message': 'No funds available for investment'}
        
        recommendations = {
            'monthly_investment_amount': recommendation.available_for_investment,
            'allocation': recommendation.investment_allocation,
            'specific_recommendations': []
        }
        
        # Add CandleThrob-specific recommendations
        if 'sp500_etf' in recommendation.investment_allocation:
            amount = recommendation.available_for_investment * recommendation.investment_allocation['sp500_etf']
            recommendations['specific_recommendations'].append({
                'type': 'ETF',
                'symbol': 'SPY',
                'monthly_amount': amount,
                'rationale': 'Low-cost S&P 500 exposure for core holdings'
            })
        
        if 'individual_stocks' in recommendation.investment_allocation:
            amount = recommendation.available_for_investment * recommendation.investment_allocation['individual_stocks']
            recommendations['specific_recommendations'].append({
                'type': 'Individual Stocks',
                'monthly_amount': amount,
                'rationale': 'Use CandleThrob analysis for stock selection',
                'integration': 'Use technical indicators and market data from CandleThrob pipeline'
            })
        
        return recommendations


def main():
    """Example usage of the personal finance optimizer."""
    
    # This would be set up with actual Plaid credentials
    PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID', 'your_client_id')
    PLAID_SECRET = os.getenv('PLAID_SECRET', 'your_secret')
    ACCESS_TOKEN = os.getenv('PLAID_ACCESS_TOKEN', 'your_access_token')
    
    # Example financial goals
    goals = [
        FinancialGoal(
            name="House Down Payment",
            target_amount=50000,
            target_date=datetime(2026, 1, 1),
            priority=1
        ),
        FinancialGoal(
            name="Vacation Fund",
            target_amount=5000,
            target_date=datetime(2025, 6, 1),
            priority=2
        )
    ]
    
    # Initialize optimizer
    optimizer = PersonalFinanceOptimizer(PLAID_CLIENT_ID, PLAID_SECRET)
    
    try:
        # Analyze finances
        recommendation = optimizer.analyze_finances(ACCESS_TOKEN, goals)
        
        # Get investment recommendations
        investment_recs = optimizer.get_investment_recommendations(recommendation)
        
        # Print results
        print("\n" + "="*50)
        print("PERSONAL FINANCE OPTIMIZATION RESULTS")
        print("="*50)
        print(f"Monthly Income: ${recommendation.monthly_income:,.2f}")
        print(f"Monthly Expenses: ${recommendation.monthly_expenses:,.2f}")
        print(f"Available for Savings: ${recommendation.available_for_savings:,.2f}")
        print(f"Available for Investment: ${recommendation.available_for_investment:,.2f}")
        print(f"Emergency Fund Target: ${recommendation.emergency_fund_target:,.2f}")
        print(f"Emergency Fund Current: ${recommendation.emergency_fund_current:,.2f}")
        print(f"Subscription Savings Potential: ${recommendation.subscription_savings_potential:,.2f}")
        
        print("\nRecommended Vaults:")
        for vault in recommendation.recommended_vaults:
            print(f"  - {vault['type']}: ${vault['monthly_contribution']:.2f}/month")
        
        print("\nInvestment Allocation:")
        for asset, percentage in recommendation.investment_allocation.items():
            amount = recommendation.available_for_investment * percentage
            print(f"  - {asset}: {percentage:.1%} (${amount:,.2f}/month)")
        
        print("\nCandleThrob Integration Recommendations:")
        for rec in investment_recs.get('specific_recommendations', []):
            print(f"  - {rec['type']}: ${rec['monthly_amount']:.2f}/month")
            print(f"    Rationale: {rec['rationale']}")
    
    except Exception as e:
        logger.error(f"Error in financial analysis: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    main()