"""
Configuration for Personal Finance Optimization Module
=====================================================

This module provides configuration settings for the budgeting and 
investment optimization system.
"""

from dataclasses import dataclass
from typing import Dict, List
from CandleThrob.core.config import BaseConfig


@dataclass
class PlaidConfig(BaseConfig):
    """Configuration for Plaid API integration."""
    
    client_id: str = ""
    secret: str = ""
    environment: str = "sandbox"  # sandbox, development, production
    
    def validate(self) -> None:
        """Validate Plaid configuration."""
        if not self.client_id:
            raise ValueError("Plaid client_id is required")
        if not self.secret:
            raise ValueError("Plaid secret is required")
        if self.environment not in ["sandbox", "development", "production"]:
            raise ValueError("Environment must be sandbox, development, or production")


@dataclass
class BudgetingConfig(BaseConfig):
    """Configuration for budgeting analysis."""
    
    # Emergency fund settings
    emergency_fund_months: int = 6
    emergency_fund_max_months: int = 12
    
    # Savings allocation percentages
    emergency_fund_priority: float = 0.5  # 50% of available savings
    investment_allocation: float = 0.7    # 70% of remaining after emergency fund
    goal_allocation: float = 0.3           # 30% for goals
    
    # Subscription analysis
    subscription_redundancy_threshold: float = 0.7
    subscription_analysis_months: int = 6
    
    # Risk tolerance settings
    default_risk_tolerance: str = "moderate"
    risk_tolerance_options: List[str] = None
    
    # Investment thresholds
    min_monthly_investment: float = 100.0
    max_individual_stock_allocation: float = 0.3
    
    def __post_init__(self):
        if self.risk_tolerance_options is None:
            self.risk_tolerance_options = ["conservative", "moderate", "aggressive"]
        super().__post_init__()
    
    def validate(self) -> None:
        """Validate budgeting configuration."""
        if self.emergency_fund_months <= 0:
            raise ValueError("Emergency fund months must be positive")
        if not 0.0 <= self.emergency_fund_priority <= 1.0:
            raise ValueError("Emergency fund priority must be between 0 and 1")
        if not 0.0 <= self.investment_allocation <= 1.0:
            raise ValueError("Investment allocation must be between 0 and 1")
        if self.default_risk_tolerance not in self.risk_tolerance_options:
            raise ValueError(f"Default risk tolerance must be one of {self.risk_tolerance_options}")


@dataclass
class InvestmentAllocationConfig(BaseConfig):
    """Configuration for investment allocation strategies."""
    
    # Risk-based allocations
    conservative_allocation: Dict[str, float] = None
    moderate_allocation: Dict[str, float] = None
    aggressive_allocation: Dict[str, float] = None
    
    # Age-based adjustments
    enable_age_adjustment: bool = True
    young_investor_age: int = 30
    older_investor_age: int = 55
    
    # CandleThrob integration
    enable_candlethrob_integration: bool = True
    sp500_etf_symbols: List[str] = None
    bond_etf_symbols: List[str] = None
    
    def __post_init__(self):
        if self.conservative_allocation is None:
            self.conservative_allocation = {
                'bonds': 0.6,
                'stocks': 0.3,
                'cash': 0.1
            }
        
        if self.moderate_allocation is None:
            self.moderate_allocation = {
                'bonds': 0.4,
                'stocks': 0.5,
                'cash': 0.1
            }
        
        if self.aggressive_allocation is None:
            self.aggressive_allocation = {
                'bonds': 0.2,
                'stocks': 0.7,
                'alternatives': 0.1
            }
        
        if self.sp500_etf_symbols is None:
            self.sp500_etf_symbols = ['SPY', 'VOO', 'IVV']
        
        if self.bond_etf_symbols is None:
            self.bond_etf_symbols = ['BND', 'AGG', 'VXUS']
        
        super().__post_init__()
    
    def validate(self) -> None:
        """Validate investment allocation configuration."""
        for name, allocation in [
            ('conservative', self.conservative_allocation),
            ('moderate', self.moderate_allocation),
            ('aggressive', self.aggressive_allocation)
        ]:
            total = sum(allocation.values())
            if abs(total - 1.0) > 0.01:  # Allow small rounding errors
                raise ValueError(f"{name} allocation must sum to 1.0, got {total}")


@dataclass
class FinanceOptimizationConfig(BaseConfig):
    """Main configuration combining all finance optimization settings."""
    
    plaid: PlaidConfig = None
    budgeting: BudgetingConfig = None
    investment: InvestmentAllocationConfig = None
    
    # Analysis settings
    transaction_analysis_months: int = 6
    enable_subscription_detection: bool = True
    enable_goal_tracking: bool = True
    enable_vault_recommendations: bool = True
    
    # Reporting settings
    generate_reports: bool = True
    report_frequency_days: int = 30
    save_analysis_results: bool = True
    
    def __post_init__(self):
        if self.plaid is None:
            self.plaid = PlaidConfig()
        if self.budgeting is None:
            self.budgeting = BudgetingConfig()
        if self.investment is None:
            self.investment = InvestmentAllocationConfig()
        super().__post_init__()
    
    def validate(self) -> None:
        """Validate all configuration components."""
        self.plaid.validate()
        self.budgeting.validate()
        self.investment.validate()
        
        if self.transaction_analysis_months <= 0:
            raise ValueError("Transaction analysis months must be positive")
        if self.report_frequency_days <= 0:
            raise ValueError("Report frequency must be positive")


# Default configuration instance
def get_finance_config() -> FinanceOptimizationConfig:
    """Get finance optimization configuration, preferring environment variables."""
    try:
        config = FinanceOptimizationConfig()
        
        # Load from environment variables if available
        import os
        if os.getenv('PLAID_CLIENT_ID'):
            config.plaid.client_id = os.getenv('PLAID_CLIENT_ID')
        if os.getenv('PLAID_SECRET'):
            config.plaid.secret = os.getenv('PLAID_SECRET')
        if os.getenv('PLAID_ENVIRONMENT'):
            config.plaid.environment = os.getenv('PLAID_ENVIRONMENT')
        
        return config
    except Exception:
        return FinanceOptimizationConfig()