"""
Fullbay API client for data retrieval.

Handles authentication, rate limiting, and data fetching from the Fullbay API.
Implements proper token generation and error handling for the Fullbay invoice endpoint.
"""

import logging
import time
import hashlib
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Config

logger = logging.getLogger(__name__)


class FullbayClient:
    """
    Client for interacting with the Fullbay API with proper authentication.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the Fullbay API client.
        
        Args:
            config: Configuration object containing API credentials and endpoints
        """
        self.config = config
        self.api_key = config.fullbay_api_key
        self.base_url = "https://app.fullbay.com/services"
        self.session = self._create_session()
        
        if not self.api_key:
            raise ValueError("Fullbay API key is required")
        
    def _create_session(self) -> requests.Session:
        """
        Create HTTP session with retry strategy and timeout configuration.
        
        Returns:
            Configured requests Session
        """
        session = requests.Session()
        
        # Configure retry strategy for slow Fullbay API
        retry_strategy = Retry(
            total=5,  # Increased retries for slow API
            backoff_factor=2,  # Exponential backoff
            status_forcelist=[429, 500, 502, 503, 504, 408],
            allowed_methods=["GET", "POST"],
            respect_retry_after_header=True
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set longer timeout for Fullbay API
        session.timeout = 300  # 5 minutes timeout
        
        return session
    
    def _get_public_ip(self) -> str:
        """
        Get the public IP address of the current Lambda function.
        
        Returns:
            Public IP address as string
        """
        try:
            # Use a simple service to get public IP
            response = self.session.get("https://httpbin.org/ip", timeout=10)
            response.raise_for_status()
            ip_data = response.json()
            return ip_data.get("origin", "").split(",")[0].strip()
        except Exception as e:
            logger.warning(f"Failed to get public IP: {e}")
            # Fallback to a default IP if we can't get the real one
            return "127.0.0.1"
    
    def _generate_token(self, date_str: str) -> str:
        """
        Generate authentication token for Fullbay API.
        
        Args:
            date_str: Date in format 'YYYY-MM-DD'
            
        Returns:
            SHA1 hash token
        """
        try:
            # Get current public IP
            ip_address = self._get_public_ip()
            
            # Concatenate key + date + IP
            token_string = f"{self.api_key}{date_str}{ip_address}"
            
            # Generate SHA1 hash
            token = hashlib.sha1(token_string.encode('utf-8')).hexdigest()
            
            logger.info(f"Generated token for date {date_str} with IP {ip_address}")
            return token
            
        except Exception as e:
            logger.error(f"Failed to generate token: {e}")
            raise Exception(f"Token generation failed: {e}")
    
    def fetch_invoices_for_date(self, target_date: datetime) -> List[Dict[str, Any]]:
        """
        Fetch invoices for a specific date from Fullbay API.
        
        Args:
            target_date: Date to fetch invoices for
            
        Returns:
            List of invoice records
            
        Raises:
            Exception: If API request fails or returns invalid data
        """
        date_str = target_date.strftime('%Y-%m-%d')
        
        try:
            logger.info(f"Fetching invoices for date: {date_str}")
            
            # Generate authentication token
            token = self._generate_token(date_str)
            
            # Build request parameters
            params = {
                'key': self.api_key,
                'token': token,
                'startDate': date_str,
                'endDate': date_str
            }
            
            # Make API request
            url = f"{self.base_url}/getInvoices.php"
            logger.info(f"Making request to: {url}")
            logger.debug(f"Request parameters: {params}")
            
            start_time = time.time()
            response = self.session.get(url, params=params)
            response_time = time.time() - start_time
            
            logger.info(f"API response status: {response.status_code}")
            logger.info(f"API response time: {response_time:.2f} seconds")
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                return self.fetch_invoices_for_date(target_date)
            
            # Raise for HTTP errors
            response.raise_for_status()
            
            # Parse response
            try:
                data = response.json()
            except ValueError as e:
                logger.error(f"Invalid JSON response: {response.text[:500]}")
                raise Exception(f"Invalid JSON response from Fullbay API: {e}")
            
            # Handle different response formats
            if isinstance(data, dict):
                # Check for error in response
                if 'error' in data:
                    error_msg = data.get('error', 'Unknown error')
                    logger.error(f"Fullbay API error: {error_msg}")
                    raise Exception(f"Fullbay API error: {error_msg}")
                
                # Extract invoices from response
                invoices = data.get('invoices', data.get('data', []))
                if not isinstance(invoices, list):
                    invoices = [data] if data else []
                    
            elif isinstance(data, list):
                invoices = data
            else:
                raise Exception(f"Unexpected response format: {type(data)}")
            
            logger.info(f"Retrieved {len(invoices)} invoices for {date_str}")
            
            # Validate and enrich records
            validated_invoices = self._validate_and_enrich_invoices(invoices, date_str)
            
            logger.info(f"Successfully processed {len(validated_invoices)} valid invoices")
            return validated_invoices
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise Exception(f"Failed to fetch invoices from Fullbay API: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during API fetch: {e}")
            raise
    
    def fetch_yesterday_invoices(self) -> List[Dict[str, Any]]:
        """
        Fetch invoices for yesterday's date.
        
        Returns:
            List of invoice records from yesterday
        """
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        return self.fetch_invoices_for_date(yesterday)
    
    def _validate_and_enrich_invoices(self, invoices: List[Dict[str, Any]], date_str: str) -> List[Dict[str, Any]]:
        """
        Validate and enrich invoice records with metadata.
        
        Args:
            invoices: Raw invoices from API
            date_str: Date string for logging
            
        Returns:
            List of validated and enriched invoices
        """
        validated_invoices = []
        current_time = datetime.now(timezone.utc).isoformat()
        
        for i, invoice in enumerate(invoices):
            try:
                # Basic validation - ensure it's a dict
                if not isinstance(invoice, dict):
                    logger.warning(f"Skipping invalid invoice (not a dict) at index {i}: {invoice}")
                    continue
                
                # Add ingestion metadata
                enriched_invoice = invoice.copy()
                enriched_invoice["_ingestion_timestamp"] = current_time
                enriched_invoice["_ingestion_source"] = "fullbay_api"
                enriched_invoice["_ingestion_date"] = date_str
                
                # Validate required fields (adjust based on actual Fullbay API response)
                required_fields = ["primaryKey"]  # Minimum required field
                missing_fields = [field for field in required_fields if field not in invoice]
                
                if missing_fields:
                    logger.warning(f"Skipping invoice missing required fields {missing_fields} at index {i}")
                    continue
                
                validated_invoices.append(enriched_invoice)
                
            except Exception as e:
                logger.warning(f"Error validating invoice at index {i}: {e}")
                continue
        
        return validated_invoices
    
    def test_connection(self) -> bool:
        """
        Test API connection and authentication.
        
        Returns:
            True if connection is successful
        """
        try:
            # Test with today's date
            today = datetime.now(timezone.utc)
            test_invoices = self.fetch_invoices_for_date(today)
            
            logger.info(f"API connection test successful - retrieved {len(test_invoices)} invoices")
            return True
            
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False
    
    def get_api_status(self) -> Dict[str, Any]:
        """
        Get API status and connection information.
        
        Returns:
            Dictionary with API status information
        """
        try:
            # Test token generation
            today = datetime.now(timezone.utc)
            date_str = today.strftime('%Y-%m-%d')
            token = self._generate_token(date_str)
            ip_address = self._get_public_ip()
            
            return {
                "status": "connected",
                "api_key_configured": bool(self.api_key),
                "token_generated": bool(token),
                "public_ip": ip_address,
                "test_date": date_str,
                "token_length": len(token) if token else 0
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "api_key_configured": bool(self.api_key)
            }