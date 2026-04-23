"""
Data Collection Module for UK Job Market Intelligence Platform

This module handles API interactions with Adzuna, Reed, and ONS APIs.
It includes rate limiting, retry logic, and credential management.
"""

import os
import time
import logging
import csv
from typing import Optional, Dict, Any, Callable, List
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter to control API request frequency.
    
    Tracks request counts within a time window and enforces limits.
    """
    
    def __init__(self, max_requests: int, time_window: timedelta):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed in the time window
            time_window: Time window for rate limiting (e.g., timedelta(days=1))
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        logger.info(f"RateLimiter initialized: {max_requests} requests per {time_window}")
    
    def _clean_old_requests(self):
        """Remove requests outside the current time window."""
        cutoff_time = datetime.now() - self.time_window
        self.requests = [req_time for req_time in self.requests if req_time > cutoff_time]
    
    def can_make_request(self) -> bool:
        """
        Check if a request can be made without exceeding the rate limit.
        
        Returns:
            True if request can be made, False otherwise
        """
        self._clean_old_requests()
        return len(self.requests) < self.max_requests
    
    def wait_if_needed(self):
        """Block until a request can be made within rate limits."""
        self._clean_old_requests()
        
        if len(self.requests) >= self.max_requests:
            # Calculate wait time until oldest request expires
            oldest_request = min(self.requests)
            wait_until = oldest_request + self.time_window
            wait_seconds = (wait_until - datetime.now()).total_seconds()
            
            if wait_seconds > 0:
                logger.warning(f"Rate limit reached. Waiting {wait_seconds:.1f} seconds...")
                time.sleep(wait_seconds)
                self._clean_old_requests()
    
    def record_request(self):
        """Record that a request was made."""
        self.requests.append(datetime.now())
    
    def get_remaining_requests(self) -> int:
        """
        Get the number of requests remaining in the current window.
        
        Returns:
            Number of requests that can still be made
        """
        self._clean_old_requests()
        return self.max_requests - len(self.requests)


class APIClientManager:
    """
    Base API client with retry logic, exponential backoff, and rate limiting.
    
    Handles common API interaction patterns including:
    - Automatic retries with exponential backoff
    - Rate limiting
    - Credential management
    - Error handling
    """
    
    def __init__(
        self,
        base_url: str,
        rate_limiter: Optional[RateLimiter] = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 30
    ):
        """
        Initialize API client manager.
        
        Args:
            base_url: Base URL for the API
            rate_limiter: Optional RateLimiter instance
            max_retries: Maximum number of retry attempts
            backoff_factor: Multiplier for exponential backoff (seconds)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.rate_limiter = rate_limiter
        self.timeout = timeout
        
        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        logger.info(f"APIClientManager initialized for {base_url}")
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make an HTTP request with rate limiting and retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (will be appended to base_url)
            params: Query parameters
            headers: HTTP headers
            json_data: JSON body for POST requests
            
        Returns:
            Response object
            
        Raises:
            requests.exceptions.RequestException: If request fails after retries
        """
        # Apply rate limiting if configured
        if self.rate_limiter:
            self.rate_limiter.wait_if_needed()
        
        # Construct full URL
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Make request
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                json=json_data,
                timeout=self.timeout
            )
            
            # Record request for rate limiting
            if self.rate_limiter:
                self.rate_limiter.record_request()
            
            # Raise exception for bad status codes
            response.raise_for_status()
            
            logger.info(f"Request successful: {method} {url}")
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {method} {url} - {str(e)}")
            raise
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        """
        Make a GET request.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            headers: HTTP headers
            
        Returns:
            Response object
        """
        return self._make_request("GET", endpoint, params=params, headers=headers)
    
    def post(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make a POST request.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            headers: HTTP headers
            json_data: JSON body
            
        Returns:
            Response object
        """
        return self._make_request("POST", endpoint, params=params, headers=headers, json_data=json_data)
    
    def get_json(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Make a GET request and return JSON response.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            headers: HTTP headers
            
        Returns:
            Parsed JSON response as dictionary
        """
        response = self.get(endpoint, params=params, headers=headers)
        return response.json()


class AdzunaClient(APIClientManager):
    """API client for Adzuna job search API."""
    
    def __init__(self):
        """Initialize Adzuna client with credentials from environment."""
        self.app_id = os.getenv('ADZUNA_APP_ID')
        self.api_key = os.getenv('ADZUNA_API_KEY')
        
        if not self.app_id or not self.api_key:
            raise ValueError(
                "Adzuna credentials not found. "
                "Please set ADZUNA_APP_ID and ADZUNA_API_KEY environment variables."
            )
        
        # Adzuna rate limit: 250 requests per day
        rate_limiter = RateLimiter(max_requests=250, time_window=timedelta(days=1))
        
        super().__init__(
            base_url="https://api.adzuna.com/v1/api/jobs/gb",
            rate_limiter=rate_limiter,
            max_retries=3,
            backoff_factor=2.0
        )
        
        logger.info("AdzunaClient initialized successfully")
    
    def search_jobs(
        self,
        what: str,
        where: Optional[str] = None,
        results_per_page: int = 50,
        page: int = 1,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Search for jobs on Adzuna.
        
        Args:
            what: Job title or keywords
            where: Location
            results_per_page: Number of results per page (max 50)
            page: Page number (1-indexed)
            **kwargs: Additional search parameters
            
        Returns:
            JSON response with job listings
        """
        params = {
            'app_id': self.app_id,
            'app_key': self.api_key,
            'what': what,
            'results_per_page': min(results_per_page, 50)
        }
        
        if where:
            params['where'] = where
        
        # Add any additional parameters
        params.update(kwargs)
        
        # Adzuna requires page number in the URL path, not as a parameter
        # Format: /search/{page}
        endpoint = f'search/{page}'
        
        return self.get_json(endpoint, params=params)


class ReedClient(APIClientManager):
    """API client for Reed job search API."""
    
    def __init__(self):
        """Initialize Reed client with credentials from environment."""
        self.api_key = os.getenv('REED_API_KEY')
        
        if not self.api_key:
            raise ValueError(
                "Reed API key not found. "
                "Please set REED_API_KEY environment variable."
            )
        
        # Reed doesn't specify strict rate limits, but we'll be conservative
        rate_limiter = RateLimiter(max_requests=500, time_window=timedelta(days=1))
        
        super().__init__(
            base_url="https://www.reed.co.uk/api/1.0",
            rate_limiter=rate_limiter,
            max_retries=3,
            backoff_factor=2.0
        )
        
        logger.info("ReedClient initialized successfully")
    
    def search_jobs(
        self,
        keywords: str,
        location: Optional[str] = None,
        results_to_take: int = 100,
        results_to_skip: int = 0,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Search for jobs on Reed.
        
        Args:
            keywords: Job title or keywords
            location: Location
            results_to_take: Number of results to return (max 100)
            results_to_skip: Number of results to skip (for pagination)
            **kwargs: Additional search parameters
            
        Returns:
            JSON response with job listings
        """
        params = {
            'keywords': keywords,
            'resultsToTake': min(results_to_take, 100),
            'resultsToSkip': results_to_skip
        }
        
        if location:
            params['locationName'] = location
        
        # Add any additional parameters
        params.update(kwargs)
        
        # Reed uses Basic Auth with API key as username and empty password
        # Need to properly encode as base64
        import base64
        auth_string = f"{self.api_key}:"
        auth_bytes = auth_string.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        
        headers = {
            'Authorization': f'Basic {auth_b64}'
        }
        
        return self.get_json('search', params=params, headers=headers)


class ONSClient(APIClientManager):
    """API client for ONS (Office for National Statistics) data."""
    
    def __init__(self):
        """Initialize ONS client."""
        # ONS APIs are generally open and don't require authentication
        # No strict rate limits, but we'll be respectful
        rate_limiter = RateLimiter(max_requests=1000, time_window=timedelta(days=1))
        
        super().__init__(
            base_url="https://api.ons.gov.uk",
            rate_limiter=rate_limiter,
            max_retries=3,
            backoff_factor=1.5
        )
        
        logger.info("ONSClient initialized successfully")
    
    def get_dataset(
        self,
        dataset_id: str,
        edition: Optional[str] = None,
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get ONS dataset metadata.
        
        Args:
            dataset_id: Dataset identifier
            edition: Dataset edition (optional)
            version: Dataset version (optional)
            
        Returns:
            JSON response with dataset information
        """
        if edition and version:
            endpoint = f"datasets/{dataset_id}/editions/{edition}/versions/{version}"
        elif edition:
            endpoint = f"datasets/{dataset_id}/editions/{edition}"
        else:
            endpoint = f"datasets/{dataset_id}"
        
        return self.get_json(endpoint)
    
    def download_csv(
        self,
        url: str,
        output_path: str
    ) -> str:
        """
        Download a CSV file from a URL.
        
        Args:
            url: URL of the CSV file to download
            output_path: Path where the CSV file should be saved
            
        Returns:
            Path to the downloaded file
            
        Raises:
            requests.exceptions.RequestException: If download fails
        """
        try:
            # Apply rate limiting if configured
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()
            
            # Download the file
            response = self.session.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()
            
            # Record request for rate limiting
            if self.rate_limiter:
                self.rate_limiter.record_request()
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Write to file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded CSV to {output_path}")
            return output_path
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download CSV from {url}: {str(e)}")
            raise


def fetch_adzuna_jobs(
    role: str,
    location: Optional[str] = None,
    max_pages: int = 10,
    results_per_page: int = 50,
    output_dir: str = "data/raw"
) -> List[Dict[str, Any]]:
    """
    Fetch job postings from Adzuna API with pagination support.
    
    Extracts the following fields from each job posting:
    - title: Job title
    - company: Company name
    - location: Job location
    - salary_min: Minimum salary
    - salary_max: Maximum salary
    - posting_date: Date job was posted
    - description: Job description
    - category: Job category
    - contract_type: Contract type (full-time, part-time, etc.)
    
    Args:
        role: Job role to search for (e.g., "Data Analyst")
        location: Optional location filter (e.g., "London")
        max_pages: Maximum number of pages to fetch (default: 10)
        results_per_page: Results per page (max 50, default: 50)
        output_dir: Directory to save CSV files (default: "data/raw")
    
    Returns:
        List of job posting dictionaries with extracted fields
        
    Raises:
        ValueError: If Adzuna credentials are not configured
        requests.exceptions.RequestException: If API requests fail
    """
    logger.info(f"Starting Adzuna job collection for role: {role}")
    
    # Initialize Adzuna client
    try:
        client = AdzunaClient()
    except ValueError as e:
        logger.error(f"Failed to initialize Adzuna client: {e}")
        raise
    
    all_jobs = []
    page = 1
    
    # Fetch jobs with pagination
    while page <= max_pages:
        try:
            logger.info(f"Fetching page {page}/{max_pages} for role: {role}")
            
            # Make API request
            response = client.search_jobs(
                what=role,
                where=location,
                results_per_page=results_per_page,
                page=page
            )
            
            # Extract job results
            results = response.get('results', [])
            
            if not results:
                logger.info(f"No more results found at page {page}. Stopping pagination.")
                break
            
            # Extract required fields from each job
            for job in results:
                job_data = {
                    'title': job.get('title', ''),
                    'company': job.get('company', {}).get('display_name', '') if isinstance(job.get('company'), dict) else job.get('company', ''),
                    'location': job.get('location', {}).get('display_name', '') if isinstance(job.get('location'), dict) else job.get('location', ''),
                    'salary_min': job.get('salary_min', None),
                    'salary_max': job.get('salary_max', None),
                    'posting_date': job.get('created', ''),
                    'description': job.get('description', ''),
                    'category': job.get('category', {}).get('label', '') if isinstance(job.get('category'), dict) else job.get('category', ''),
                    'contract_type': job.get('contract_type', '')
                }
                all_jobs.append(job_data)
            
            logger.info(f"Extracted {len(results)} jobs from page {page}")
            
            # Check if we've reached the last page
            total_results = response.get('count', 0)
            if page * results_per_page >= total_results:
                logger.info(f"Reached end of results. Total: {total_results}")
                break
            
            page += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed at page {page}: {e}")
            # Continue with what we have so far
            break
        except Exception as e:
            logger.error(f"Unexpected error at page {page}: {e}")
            break
    
    # Save results to CSV
    if all_jobs:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp and role
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_role = role.replace(' ', '_').replace('/', '_')
        filename = f"adzuna_{safe_role}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Write to CSV
        fieldnames = [
            'title', 'company', 'location', 'salary_min', 'salary_max',
            'posting_date', 'description', 'category', 'contract_type'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_jobs)
        
        logger.info(f"Saved {len(all_jobs)} jobs to {filepath}")
    else:
        logger.warning(f"No jobs found for role: {role}")
    
    return all_jobs


def fetch_reed_jobs(
    role: str,
    location: Optional[str] = None,
    max_pages: int = 10,
    results_per_page: int = 100,
    output_dir: str = "data/raw"
) -> List[Dict[str, Any]]:
    """
    Fetch job postings from Reed API with pagination support.
    
    Maps Reed API response fields to standardized schema:
    - title: Job title (from jobTitle)
    - company: Company name (from employerName)
    - location: Job location (from locationName)
    - salary_min: Minimum salary (from minimumSalary)
    - salary_max: Maximum salary (from maximumSalary)
    - posting_date: Date job was posted (from date)
    - description: Job description (from jobDescription)
    - category: Job category (from jobType or empty)
    - contract_type: Contract type (from contractType)
    
    Args:
        role: Job role to search for (e.g., "Data Analyst")
        location: Optional location filter (e.g., "London")
        max_pages: Maximum number of pages to fetch (default: 10)
        results_per_page: Results per page (max 100, default: 100)
        output_dir: Directory to save CSV files (default: "data/raw")
    
    Returns:
        List of job posting dictionaries with standardized fields
        
    Raises:
        ValueError: If Reed API key is not configured
        requests.exceptions.RequestException: If API requests fail
    """
    logger.info(f"Starting Reed job collection for role: {role}")
    
    # Initialize Reed client
    try:
        client = ReedClient()
    except ValueError as e:
        logger.error(f"Failed to initialize Reed client: {e}")
        raise
    
    all_jobs = []
    page = 0  # Reed uses 0-based offset pagination
    
    # Fetch jobs with pagination
    while page < max_pages:
        try:
            # Calculate offset for pagination
            results_to_skip = page * results_per_page
            
            logger.info(f"Fetching page {page + 1}/{max_pages} for role: {role} (offset: {results_to_skip})")
            
            # Make API request
            response = client.search_jobs(
                keywords=role,
                location=location,
                results_to_take=results_per_page,
                results_to_skip=results_to_skip
            )
            
            # Extract job results
            results = response.get('results', [])
            
            if not results:
                logger.info(f"No more results found at page {page + 1}. Stopping pagination.")
                break
            
            # Map Reed API fields to standardized schema
            for job in results:
                job_data = {
                    'title': job.get('jobTitle', ''),
                    'company': job.get('employerName', ''),
                    'location': job.get('locationName', ''),
                    'salary_min': job.get('minimumSalary', None),
                    'salary_max': job.get('maximumSalary', None),
                    'posting_date': job.get('date', ''),
                    'description': job.get('jobDescription', ''),
                    'category': job.get('jobType', ''),
                    'contract_type': job.get('contractType', '')
                }
                all_jobs.append(job_data)
            
            logger.info(f"Extracted {len(results)} jobs from page {page + 1}")
            
            # Check if we've reached the last page
            total_results = response.get('totalResults', 0)
            if results_to_skip + len(results) >= total_results:
                logger.info(f"Reached end of results. Total: {total_results}")
                break
            
            page += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed at page {page + 1}: {e}")
            # Continue with what we have so far
            break
        except Exception as e:
            logger.error(f"Unexpected error at page {page + 1}: {e}")
            break
    
    # Save results to CSV
    if all_jobs:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp and role
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_role = role.replace(' ', '_').replace('/', '_')
        filename = f"reed_{safe_role}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Write to CSV with standardized field names
        fieldnames = [
            'title', 'company', 'location', 'salary_min', 'salary_max',
            'posting_date', 'description', 'category', 'contract_type'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_jobs)
        
        logger.info(f"Saved {len(all_jobs)} jobs to {filepath}")
    else:
        logger.warning(f"No jobs found for role: {role}")
    
    return all_jobs


def fetch_ons_vacs01(
    output_dir: str = "data/raw"
) -> str:
    """
    Download ONS VACS01 dataset (Vacancies by Industry).
    
    VACS01 contains vacancy data broken down by industry sector,
    providing context for job market trends across different sectors.
    
    Note: ONS has changed their data distribution system. This function
    provides instructions for manual download as the direct download URLs
    are no longer available.
    
    Args:
        output_dir: Directory to save CSV file (default: "data/raw")
    
    Returns:
        Path where the file should be saved
        
    Raises:
        NotImplementedError: Direct download no longer supported
    """
    logger.info("ONS VACS01 (Vacancies by Industry) - Manual download required")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"ons_vacs01_{timestamp}.csv"
    output_path = os.path.join(output_dir, filename)
    
    # Provide instructions for manual download
    instructions = """
    ╔════════════════════════════════════════════════════════════════╗
    ║  ONS VACS01 - Manual Download Required                        ║
    ╚════════════════════════════════════════════════════════════════╝
    
    ONS has changed their data distribution system. Please download manually:
    
    1. Visit: https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/datasets/vacanciesandunemploymentvacs01
    
    2. Click "Download" button for the latest dataset
    
    3. Save the file to: {output_path}
    
    Alternative - Use Nomis Web:
    
    1. Visit: https://www.nomisweb.co.uk/datasets/vsic
    
    2. Select your parameters (geography, time period, industry)
    
    3. Download as CSV
    
    4. Save to: {output_path}
    """.format(output_path=output_path)
    
    logger.warning(instructions)
    print(instructions)
    
    raise NotImplementedError(
        "ONS direct download URLs have changed. Please download manually using the instructions above."
    )


def fetch_ons_earn01(
    output_dir: str = "data/raw"
) -> str:
    """
    Download ONS EARN01 dataset (Average Weekly Earnings).
    
    EARN01 contains average weekly earnings data broken down by industry,
    providing salary benchmarks for comparison with job posting data.
    
    Note: ONS has changed their data distribution system. This function
    provides instructions for manual download as the direct download URLs
    are no longer available.
    
    Args:
        output_dir: Directory to save CSV file (default: "data/raw")
    
    Returns:
        Path where the file should be saved
        
    Raises:
        NotImplementedError: Direct download no longer supported
    """
    logger.info("ONS EARN01 (Average Weekly Earnings) - Manual download required")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"ons_earn01_{timestamp}.csv"
    output_path = os.path.join(output_dir, filename)
    
    # Provide instructions for manual download
    instructions = """
    ╔════════════════════════════════════════════════════════════════╗
    ║  ONS EARN01 - Manual Download Required                        ║
    ╚════════════════════════════════════════════════════════════════╝
    
    ONS has changed their data distribution system. Please download manually:
    
    1. Visit: https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/averageweeklyearningsearn01
    
    2. Click "Download" button for the latest dataset
    
    3. Save the file to: {output_path}
    
    Alternative - Use ASHE Data:
    
    1. Visit: https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/ashe1997to2015selectedestimates
    
    2. Download the latest Annual Survey of Hours and Earnings (ASHE) data
    
    3. Save to: {output_path}
    """.format(output_path=output_path)
    
    logger.warning(instructions)
    print(instructions)
    
    raise NotImplementedError(
        "ONS direct download URLs have changed. Please download manually using the instructions above."
    )


def fetch_ons_regional_labour_market(
    output_dir: str = "data/raw"
) -> str:
    """
    Download ONS Regional Labour Market tables (NUTS1 regions).
    
    Regional labour market data provides employment statistics broken down
    by UK NUTS1 regions (e.g., London, South East, North West), enabling
    regional opportunity scoring and analysis.
    
    Note: ONS has changed their data distribution system. This function
    provides instructions for manual download as the direct download URLs
    are no longer available.
    
    Args:
        output_dir: Directory to save CSV file (default: "data/raw")
    
    Returns:
        Path where the file should be saved
        
    Raises:
        NotImplementedError: Direct download no longer supported
    """
    logger.info("ONS Regional Labour Market (NUTS1) - Manual download required")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"ons_regional_labour_market_{timestamp}.csv"
    output_path = os.path.join(output_dir, filename)
    
    # Provide instructions for manual download
    instructions = """
    ╔════════════════════════════════════════════════════════════════╗
    ║  ONS Regional Labour Market - Manual Download Required        ║
    ╚════════════════════════════════════════════════════════════════╝
    
    ONS has changed their data distribution system. Please download manually:
    
    1. Visit: https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/regionallabourmarket/latest
    
    2. Scroll to "Data downloads" section
    
    3. Download the regional labour market summary tables
    
    4. Save the file to: {output_path}
    
    Alternative - Use Nomis Web:
    
    1. Visit: https://www.nomisweb.co.uk/
    
    2. Search for "regional employment" or "workforce jobs"
    
    3. Select dataset: "Workforce jobs by industry" (wfjsa)
    
    4. Download as CSV with regional breakdown
    
    5. Save to: {output_path}
    """.format(output_path=output_path)
    
    logger.warning(instructions)
    print(instructions)
    
    raise NotImplementedError(
        "ONS direct download URLs have changed. Please download manually using the instructions above."
    )


# Example usage and testing
if __name__ == "__main__":
    # Test credential loading
    try:
        adzuna = AdzunaClient()
        logger.info("✓ Adzuna client initialized")
    except ValueError as e:
        logger.error(f"✗ Adzuna client failed: {e}")
    
    try:
        reed = ReedClient()
        logger.info("✓ Reed client initialized")
    except ValueError as e:
        logger.error(f"✗ Reed client failed: {e}")
    
    try:
        ons = ONSClient()
        logger.info("✓ ONS client initialized")
    except ValueError as e:
        logger.error(f"✗ ONS client failed: {e}")
    
    # Test fetch_adzuna_jobs function (commented out to avoid API calls during testing)
    # Uncomment to test with real API:
    # jobs = fetch_adzuna_jobs(
    #     role="Data Analyst",
    #     location="London",
    #     max_pages=2,
    #     results_per_page=10
    # )
    # logger.info(f"Fetched {len(jobs)} Adzuna jobs")
    
    # Test fetch_reed_jobs function (commented out to avoid API calls during testing)
    # Uncomment to test with real API:
    # jobs = fetch_reed_jobs(
    #     role="Data Analyst",
    #     location="London",
    #     max_pages=2,
    #     results_per_page=50
    # )
    # logger.info(f"Fetched {len(jobs)} Reed jobs")
    
    # Test ONS data collection functions (commented out to avoid API calls during testing)
    # Uncomment to test with real API:
    # vacs01_path = fetch_ons_vacs01(output_dir="data/raw")
    # logger.info(f"Downloaded VACS01 to {vacs01_path}")
    #
    # earn01_path = fetch_ons_earn01(output_dir="data/raw")
    # logger.info(f"Downloaded EARN01 to {earn01_path}")
    #
    # regional_path = fetch_ons_regional_labour_market(output_dir="data/raw")
    # logger.info(f"Downloaded Regional Labour Market data to {regional_path}")
