import pytest
import pandas as pd
import os
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

# Basic connectivity tests
def test_api_connectivity(client):
    """Test basic API connectivity with a simple request."""
    # Test a simple API call to verify connectivity
    result = client.congress_trading(page=1, page_size=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 5  # Should respect page_size parameter

# Congress Trading Tests
def test_congress_trading_integration(client):
    """Test congress_trading with real API call."""
    result = client.congress_trading(page=1, page_size=10)
    assert isinstance(result, pd.DataFrame)
    # Check for essential columns that should exist in the response
    assert "Ticker" in result.columns
    assert "Transaction" in result.columns
    # Check for either ReportDate or Filed (depending on API version)
    assert any(col in result.columns for col in ["ReportDate", "Filed"])
    assert len(result) <= 10  # Should respect page_size

def test_congress_trading_with_real_ticker(client):
    """Test congress_trading with a specific ticker."""
    # Choose a common ticker like AAPL or MSFT that likely has data
    ticker = "AAPL"
    result = client.congress_trading(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

def test_congress_trading_with_real_politician(client):
    """Test congress_trading with a real politician."""
    # This test is more fragile as politician names change
    # Get a list of representatives first
    all_trades = client.congress_trading(page=1, page_size=20)
    if all_trades.empty:
        pytest.skip("No congress trading data available")
    
    if "Representative" in all_trades.columns:
        politician = all_trades["Representative"].iloc[0]
        result = client.congress_trading(ticker=politician, politician=True)
        assert isinstance(result, pd.DataFrame)
    else:
        pytest.skip("Representative column not found in API response")

# Senate Trading Tests
def test_senate_trading_integration(client):
    """Test senate_trading with real API call."""
    result = client.senate_trading()
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Senator" in result.columns
        assert "Ticker" in result.columns
        assert "Transaction" in result.columns

def test_senate_trading_with_real_ticker(client):
    """Test senate_trading with a specific ticker."""
    ticker = "MSFT"
    result = client.senate_trading(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
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
    result = client.gov_contracts(page=1, page_size=5)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert "Ticker" in result.columns
        assert "Amount" in result.columns
    assert len(result) <= 5  # Should respect page_size

def test_gov_contracts_with_real_ticker(client):
    """Test gov_contracts with a specific ticker."""
    # Companies that often have government contracts
    ticker = "LMT"  # Lockheed Martin
    result = client.gov_contracts(ticker=ticker)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

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
    """Test pagination works correctly."""
    # Get two pages with different page sizes
    page1 = client.congress_trading(page=1, page_size=5)
    page2 = client.congress_trading(page=2, page_size=5)
    
    assert isinstance(page1, pd.DataFrame)
    assert isinstance(page2, pd.DataFrame)
    
    if not page1.empty and not page2.empty:
        # Ensure the pages are different
        assert not page1.equals(page2)
        
        # Get combined result with larger page size
        combined = client.congress_trading(page=1, page_size=10)
        assert len(combined) <= 10

# Data type tests
def test_congress_trading_data_types(client):
    """Test that data types are correct after processing."""
    result = client.congress_trading(page=1, page_size=5)
    if not result.empty:
        # Test for datetime fields - either Filed or ReportDate should be present
        if "Filed" in result.columns:
            assert pd.api.types.is_datetime64_dtype(result["Filed"])
        if "Traded" in result.columns:
            assert pd.api.types.is_datetime64_dtype(result["Traded"])
        if "Trade_Size_USD" in result.columns:
            assert pd.api.types.is_numeric_dtype(result["Trade_Size_USD"])

# Test error handling with invalid parameters
def test_error_handling_invalid_ticker(client):
    """Test error handling with an invalid ticker."""
    result = client.congress_trading(ticker="INVALID_TICKER_12345")
    assert isinstance(result, pd.DataFrame)
    # Should return empty DataFrame for invalid ticker
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
    """Test that recent and historical data endpoints work differently."""
    recent = client.congress_trading(recent=True, page=1, page_size=5)
    historical = client.congress_trading(recent=False, page=1, page_size=5)
    
    assert isinstance(recent, pd.DataFrame)
    assert isinstance(historical, pd.DataFrame)

# Test edge cases
def test_empty_ticker(client):
    """Test behavior with empty ticker parameter."""
    result = client.congress_trading(ticker="")
    assert isinstance(result, pd.DataFrame)
    # Should behave the same as default call

def test_string_page_parameters(client):
    """Test behavior with string page parameters."""
    # API should handle string page parameters gracefully
    result = client.congress_trading(page="1", page_size="5")
    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 5

# Test congress_trading specific features
def test_congress_trading_bulk_data(client):
    """Test congress_trading bulk data endpoint."""
    result = client.congress_trading(recent=False, page=1, page_size=5)
    assert isinstance(result, pd.DataFrame)
    # We don't check for specific columns as the API response structure might change

# Test combination of parameters
def test_politician_with_page_params(client):
    """Test politician parameter with paging."""
    all_trades = client.congress_trading(page=1, page_size=20)
    if all_trades.empty:
        pytest.skip("No congress trading data available")
    
    politician = all_trades["Representative"].iloc[0]
    result = client.congress_trading(ticker=politician, politician=True, page=1, page_size=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 5

# Test for invalid API token
def test_invalid_token():
    """Test behavior with invalid API token."""
    invalid_client = quiver("invalid_token_12345")
    
    # Should either return empty DataFrame or raise an authentication error
    try:
        result = invalid_client.congress_trading()
        assert isinstance(result, pd.DataFrame)
        # If it returns a DataFrame, it should be empty
        assert result.empty
    except Exception as e:
        # If it raises an exception, it should contain authentication-related message
        assert any(term in str(e).lower() for term in ["auth", "token", "unauthorized"])

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

def test_congress_trading_with_recent_false_and_ticker(client):
    """Test congress_trading with ticker and recent=False."""
    ticker = "MSFT"
    result = client.congress_trading(ticker=ticker, recent=False)
    assert isinstance(result, pd.DataFrame)
    if not result.empty:
        assert all(result["Ticker"] == ticker)

def test_gov_contracts_with_large_page_size(client):
    """Test gov_contracts with larger page size."""
    result = client.gov_contracts(page=1, page_size=50)
    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 50

# Test for resilience
def test_resilience_sequential_calls(client):
    """Test resilience with multiple sequential API calls."""
    # Make multiple calls to the same endpoint
    for _ in range(3):
        result = client.senate_trading()
        assert isinstance(result, pd.DataFrame)
        
    # Then try different endpoints
    result1 = client.senate_trading()
    assert isinstance(result1, pd.DataFrame)
    
    result2 = client.house_trading()
    assert isinstance(result2, pd.DataFrame)
    
    result3 = client.gov_contracts(page=1, page_size=5)
    assert isinstance(result3, pd.DataFrame)
    assert len(result3) <= 5