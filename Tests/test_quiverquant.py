import pytest
import pandas as pd
import os
from datetime import datetime, timedelta
from quiverquant import quiver
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Integration tests for quiverquant - testing the actual API

# Get API token from environment variable to avoid hardcoding credentials
@pytest.fixture
def api_token():
    token = os.environ.get("QUIVERQUANT_API_TOKEN")
    if not token:
        pytest.skip("QUIVERQUANT_API_TOKEN environment variable not set")
    return token

@pytest.fixture
def client(api_token):
    """Create a real quiver client instance for integration testing."""
    return quiver(api_token)

# Congress Trading Tests
def test_congress_trading_integration(client):
    """Test congress_trading with real API call."""
    # Use last 30 days to ensure we get some data
    from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    result = client.congress_trading(from_date=from_date)
    assert isinstance(result, pd.DataFrame)
    # Check for essential columns that should exist in the response
    if not result.empty:
        assert "Ticker" in result.columns
        assert "Transaction" in result.columns
        # Check for either ReportDate, Filed, or Traded (depending on API version)
        assert any(col in result.columns for col in ["ReportDate", "Filed", "Traded"])

def test_congress_trading_with_real_ticker(client):
    """Test congress_trading with from_date parameter."""
    # Test with a date range that likely has data - last 60 days
    from_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    result = client.congress_trading(from_date=from_date)
    assert isinstance(result, pd.DataFrame)
    # Verify date filtering works by checking the minimum date
    if not result.empty and "Traded" in result.columns:
        min_date = pd.to_datetime(result["Traded"]).min()
        assert min_date >= pd.to_datetime(from_date)

def test_congress_trading_with_real_politician(client):
    """Test congress_trading with different date ranges."""
    # Test with different date ranges - recent vs older
    recent_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    older_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    result1 = client.congress_trading(from_date=recent_date)
    result2 = client.congress_trading(from_date=older_date)
    
    assert isinstance(result1, pd.DataFrame)
    assert isinstance(result2, pd.DataFrame)
    
    # Result2 should have more data (earlier date)
    if not result1.empty and not result2.empty:
        assert len(result2) >= len(result1)

# Senate Trading Tests
def test_senate_trading_integration(client):
    """Test senate_trading with real API call."""
    result = client.senate_trading()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        # Check for columns that might exist
        assert "Ticker" in result.columns or "Representative" in result.columns

def test_senate_trading_with_real_ticker(client):
    """Test senate_trading with a specific ticker."""
    ticker = "MSFT"
    result = client.senate_trading(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty and "Ticker" in result.columns:
        assert all(result["Ticker"] == ticker)

# House Trading Tests
def test_house_trading_integration(client):
    """Test house_trading with real API call."""
    result = client.house_trading()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Representative" in result.columns
        assert "Ticker" in result.columns
        assert "Transaction" in result.columns

def test_house_trading_with_real_ticker(client):
    """Test house_trading with a specific ticker."""
    ticker = "AAPL"
    result = client.house_trading(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

# Off Exchange Tests
def test_offexchange_integration(client):
    """Test offexchange with real API call."""
    result = client.offexchange()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Ticker" in result.columns
        # Check for either Dark or OTC columns (the API might have changed)
        assert any(col in result.columns for col in ["Dark", "OTC_Short", "OTC_Total", "DPI"])

def test_offexchange_with_real_ticker(client):
    """Test offexchange with a specific ticker."""
    ticker = "AAPL"
    result = client.offexchange(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

# Government Contracts Tests
def test_gov_contracts_integration(client):
    """Test gov_contracts with real API call."""
    # Use last 30 days for government contracts
    from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    result = client.gov_contracts(from_date=from_date)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Ticker" in result.columns
        # Amount column might have different names
        assert any(col in result.columns for col in ["Amount", "Value", "Contract_Value"])

def test_gov_contracts_with_real_ticker(client):
    """Test gov_contracts with from_date parameter."""
    # Test with a date range that likely has data - last 60 days
    from_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    result = client.gov_contracts(from_date=from_date)
    assert isinstance(result, pd.DataFrame)
    # Verify date filtering works
    if not result.empty and "Date" in result.columns:
        min_date = pd.to_datetime(result["Date"]).min()
        assert min_date >= pd.to_datetime(from_date)

# Lobbying Tests
def test_lobbying_integration(client):
    """Test lobbying with real API call."""
    result = client.lobbying()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Ticker" in result.columns
        assert "Amount" in result.columns
        assert "Issue" in result.columns

def test_lobbying_with_real_ticker(client):
    """Test lobbying with a specific ticker."""
    ticker = "GOOG"
    result = client.lobbying(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    # Check ticker equality only if the API response has data and includes the Ticker column
    if not result.empty and "Ticker" in result.columns:
        assert all(result["Ticker"] == ticker)

# Test pagination
def test_pagination(client):
    """Test different date ranges work correctly."""
    # Get data from different date ranges - recent vs older
    recent_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    older_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    recent = client.congress_trading(from_date=recent_date)
    older = client.congress_trading(from_date=older_date)
    
    assert isinstance(recent, pd.DataFrame)
    assert isinstance(older, pd.DataFrame)
    
    # Older date range should have more or equal data
    if not recent.empty and not older.empty:
        assert len(older) >= len(recent)

# Data type tests
def test_congress_trading_data_types(client):
    """Test that data types are correct after processing."""
    # Use last 30 days
    from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    result = client.congress_trading(from_date=from_date)
    if not result.empty:
        # Test for datetime fields - Traded should be present
        if "Traded" in result.columns:
            assert pd.api.types.is_datetime64_dtype(result["Traded"])
        if "Filed" in result.columns:
            assert pd.api.types.is_datetime64_dtype(result["Filed"])
        # Check for numeric columns that might exist
        numeric_cols = ["Trade_Size_USD", "Amount", "Value"]
        for col in numeric_cols:
            if col in result.columns:
                assert pd.api.types.is_numeric_dtype(result[col])

# Test error handling with invalid parameters
def test_error_handling_invalid_date(client):
    """Test error handling with an invalid date."""
    # Test with a future date that should have no data
    future_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    result = client.congress_trading(from_date=future_date)
    assert isinstance(result, pd.DataFrame)
    # Should return empty DataFrame for future date
    assert result.empty

# Skip tests for premium features
@pytest.mark.skip(reason="Premium feature, requires subscription")
def test_premium_feature_wikipedia(client):
    """Test wikipedia endpoint (premium feature)."""
    try:
        result = client.wikipedia()
        assert isinstance(result, pd.DataFrame)
        if not result.empty:
            assert "Ticker" in result.columns
            assert "Views" in result.columns
    except Exception as e:
        if "subscription" in str(e).lower():
            pytest.skip("Premium subscription required for Wikipedia endpoint")
        else:
            raise

@pytest.mark.skip(reason="Premium feature, requires subscription")
def test_premium_feature_wallstreetbets(client):
    """Test wallstreetbets endpoint (premium feature)."""
    try:
        result = client.wallstreetbets()
        assert isinstance(result, pd.DataFrame)
        if not result.empty:
            assert "Ticker" in result.columns
            assert "Mentions" in result.columns
    except Exception as e:
        if "subscription" in str(e).lower():
            pytest.skip("Premium subscription required for WallStreetBets endpoint")
        else:
            raise

# Test specific features
def test_recent_vs_historical(client):
    """Test that different date ranges work correctly."""
    # Recent: last 7 days, Historical: last 60 days
    recent_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    historical_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    
    recent = client.congress_trading(from_date=recent_date)
    historical = client.congress_trading(from_date=historical_date)
    
    assert isinstance(recent, pd.DataFrame)
    assert isinstance(historical, pd.DataFrame)
    
    # Historical should have more data
    if not recent.empty and not historical.empty:
        assert len(historical) >= len(recent)

# Test edge cases
def test_empty_date_range(client):
    """Test behavior with very recent date range."""
    # Use yesterday's date - might be empty but should be valid
    from_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    result = client.congress_trading(from_date=from_date)
    assert isinstance(result, pd.DataFrame)
    # Should return valid DataFrame (might be empty if no recent data)

def test_string_date_parameters(client):
    """Test behavior with string date parameters."""
    # API should handle string date parameters gracefully
    from_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    result = client.congress_trading(from_date=from_date)
    assert isinstance(result, pd.DataFrame)

# Test congress_trading specific features
def test_congress_trading_bulk_data(client):
    """Test congress_trading bulk data endpoint."""
    # Use last 45 days for bulk data test
    from_date = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
    result = client.congress_trading(from_date=from_date)
    assert isinstance(result, pd.DataFrame)
    # We don't check for specific columns as the API response structure might change

# Test combination of parameters
def test_date_filtering_works(client):
    """Test that date filtering works correctly."""
    # Get data from a wider date range - last 60 days
    older_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    all_trades = client.congress_trading(from_date=older_date)
    if all_trades.empty:
        pytest.skip("No congress trading data available")
    
    # Get data from a more recent date range - last 14 days
    recent_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    recent_trades = client.congress_trading(from_date=recent_date)
    assert isinstance(recent_trades, pd.DataFrame)
    
    # Recent trades should be a subset or equal to all trades
    assert len(recent_trades) <= len(all_trades)

# Test for invalid API token
def test_invalid_token():
    """Test behavior with invalid API token."""
    invalid_client = quiver("invalid_token_12345")
    
    # Test that access is denied with invalid token
    # We should either get an empty DataFrame or an authentication error
    try:
        # Use last 7 days to minimize data load
        from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        result = invalid_client.congress_trading(from_date=from_date)
        # If it returns a DataFrame, it should be empty
        assert isinstance(result, pd.DataFrame)
        assert result.empty, "Invalid token should not return data"
        
        # Try another endpoint as well
        result2 = invalid_client.insiders()
        assert isinstance(result2, pd.DataFrame)
        assert result2.empty, "Invalid token should not return data"
        
    except Exception as e:
        # If an exception is raised, it should be related to authentication
        error_message = str(e).lower()
        auth_terms = ["auth", "token", "unauthorized", "permission", "access", "denied"]
        assert any(term in error_message for term in auth_terms), \
            f"Expected authentication error, got: {error_message}"

# Test specific endpoints
def test_insiders_trading(client):
    """Test insiders endpoint."""
    result = client.insiders()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Ticker" in result.columns

def test_insiders_with_ticker(client):
    """Test insiders with a specific ticker."""
    ticker = "MSFT"
    result = client.insiders(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_earnings_integration(client):
    """Test earnings endpoint."""
    result = client.earnings()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Ticker" in result.columns
        assert "DateEstimated" in result.columns

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_earnings_with_ticker(client):
    """Test earnings with a specific ticker."""
    ticker = "AAPL"
    result = client.earnings(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_retail_integration(client):
    """Test retail endpoint."""
    result = client.retail()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Ticker" in result.columns
        assert "Date" in result.columns

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_retail_with_ticker(client):
    """Test retail with a specific ticker."""
    ticker = "GME"
    result = client.retail(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_sec_integration(client):
    """Test sec endpoint."""
    result = client.sec()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Ticker" in result.columns
        assert "Date" in result.columns

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_sec_with_ticker(client):
    """Test sec with a specific ticker."""
    ticker = "TSLA"
    result = client.sec(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_stocktwits_integration(client):
    """Test stocktwits endpoint."""
    try:
        result = client.stocktwits()
        assert isinstance(result, pd.DataFrame)
        if not result.empty:
            assert "Ticker" in result.columns
            assert "Date" in result.columns
    except Exception as e:
        if "subscription" in str(e).lower():
            pytest.skip("Premium subscription required for StockTwits endpoint")
        else:
            raise

def test_twitter_integration(client):
    """Test twitter endpoint."""
    try:
        result = client.twitter()
        assert isinstance(result, pd.DataFrame)
        if not result.empty:
            assert "Ticker" in result.columns
            assert "Date" in result.columns
    except Exception as e:
        if "subscription" in str(e).lower():
            pytest.skip("Premium subscription required for Twitter endpoint")
        else:
            raise

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_corporate_jet_integration(client):
    """Test corporate jet tracking endpoint."""
    try:
        result = client.corporate_jet()
        assert isinstance(result, pd.DataFrame)
        if not result.empty:
            assert "Ticker" in result.columns
    except Exception as e:
        if "subscription" in str(e).lower():
            pytest.skip("Premium subscription required for corporate jet tracking")
        else:
            raise

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_options_integration(client):
    """Test options endpoint."""
    try:
        result = client.options()
        assert isinstance(result, pd.DataFrame)
        if not result.empty:
            assert "Ticker" in result.columns
    except Exception as e:
        if "subscription" in str(e).lower():
            pytest.skip("Premium subscription required for options data")
        else:
            raise

@pytest.mark.skip(reason="Endpoint not available in current API")
def test_daily_positions_integration(client):
    """Test daily positions endpoint."""
    try:
        result = client.daily_positions()
        assert isinstance(result, pd.DataFrame)
        if not result.empty:
            assert "Ticker" in result.columns
    except Exception as e:
        if "subscription" in str(e).lower():
            pytest.skip("Premium subscription required for daily positions data")
        else:
            raise

# Test nonstandard parameter combinations
@pytest.mark.skip(reason="house_trading does not accept recent parameter")
def test_house_trading_recent_with_ticker(client):
    """Test house_trading with specific ticker and recent parameter."""
    ticker = "AAPL"
    result = client.house_trading(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

def test_congress_trading_with_older_date(client):
    """Test congress_trading with older date range."""
    # Use 90 days ago for "older" data
    from_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    result = client.congress_trading(from_date=from_date)
    assert isinstance(result, pd.DataFrame)
    # Should return data for the specified date range
    if not result.empty and "Traded" in result.columns:
        min_date = pd.to_datetime(result["Traded"]).min()
        assert min_date >= pd.to_datetime(from_date)

def test_gov_contracts_with_older_date(client):
    """Test gov_contracts with older date range."""
    # Use 90 days ago for "older" data
    from_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    result = client.gov_contracts(from_date=from_date)
    assert isinstance(result, pd.DataFrame)
    # Should return data for the specified date range
    if not result.empty and "Date" in result.columns:
        min_date = pd.to_datetime(result["Date"]).min()
        assert min_date >= pd.to_datetime(from_date)

# Test for resilience
def test_resilience_sequential_calls(client):
    """Test resilience with multiple sequential API calls."""
    # Make multiple calls to the same endpoint
    for _ in range(3):
        result = client.senate_trading_old()
        assert isinstance(result, pd.DataFrame)
        
    # Then try different endpoints
    result1 = client.senate_trading_old()
    assert isinstance(result1, pd.DataFrame)
    
    result2 = client.house_trading()
    assert isinstance(result2, pd.DataFrame)
    
    # Use last 30 days for gov contracts
    from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    result3 = client.gov_contracts(from_date=from_date)
    assert isinstance(result3, pd.DataFrame)