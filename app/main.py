from fastapi import FastAPI, HTTPException, Body, BackgroundTasks
from .database.connection import execute_query
import requests
import json
import datetime
from typing import Dict, Any, Optional, List
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import threading
import time
import atexit

app = FastAPI(title="Google Accounts API")

# Load environment variables
load_dotenv()

# Google OAuth credentials from environment variables
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")

# Initialize the background scheduler
scheduler = BackgroundScheduler()

class TokenRequest(BaseModel):
    refresh_token: str

class GaqlRequest(BaseModel):
    query: str

@app.on_event("startup")
def start_scheduler():
    """Start the background scheduler when the application starts."""
    try:
        # Add a job to run every 12 hours
        scheduler.add_job(
            func=run_scheduler_fetch_and_save,
            trigger=IntervalTrigger(hours=12),
            id="fetch_and_save_job",
            name="Fetch and save campaign conversion goals every 12 hours",
            replace_existing=True
        )
        
        # Add immediate run job (will run once after 1 minute of startup)
        scheduler.add_job(
            func=run_scheduler_fetch_and_save,
            trigger=IntervalTrigger(minutes=1),
            id="initial_fetch_and_save_job",
            name="Initial fetch and save after startup",
            replace_existing=True,
            max_instances=1,
            next_run_time=datetime.datetime.now() + datetime.timedelta(minutes=1)
        )
        
        # Start the scheduler
        if not scheduler.running:
            scheduler.start()
            print("Background scheduler started. Will run fetch and save job every 12 hours.")
        
        # Register the scheduler shutdown with atexit
        atexit.register(lambda: scheduler.shutdown(wait=False))
    except Exception as e:
        print(f"Error starting scheduler: {str(e)}")

@app.on_event("shutdown")
def shutdown_scheduler():
    """Shut down the background scheduler when the application shuts down."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("Background scheduler shut down.")

def run_scheduler_fetch_and_save():
    """
    Function to be called by the scheduler to fetch and save campaign conversion goals.
    This function is isolated from the web server to avoid any request context dependencies.
    """
    try:
        print(f"[{datetime.datetime.now()}] Starting scheduled fetch and save campaign conversion goals...")
        
        # We'll create a new thread for this to avoid blocking the scheduler
        thread = threading.Thread(target=execute_fetch_and_save)
        thread.daemon = True  # Daemon threads are killed when the process exits
        thread.start()
        
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error in scheduled fetch and save: {str(e)}")

def execute_fetch_and_save():
    """Execute the fetch and save operation in a separate thread."""
    try:
        # Get all accounts with tokens
        accounts_with_tokens = get_account_tokens_internal()
        
        print(f"[{datetime.datetime.now()}] Processing {len(accounts_with_tokens)} accounts...")
        
        all_results = []
        
        # For each account, make the Google Ads API call
        for account in accounts_with_tokens:
            account_id = account["account_id"]
            access_token = account["access_token"]
            
            try:
                # Call Google Ads API
                url = f"https://googleads.googleapis.com/v17/customers/{account_id}/googleAds:searchStream"
                
                query = """
                SELECT 
                    campaign_conversion_goal.campaign, 
                    campaign_conversion_goal.category, 
                    campaign_conversion_goal.origin, 
                    campaign.app_campaign_setting.bidding_strategy_goal_type, 
                    campaign_conversion_goal.biddable, 
                    campaign_conversion_goal.resource_name, 
                    campaign.id, 
                    customer.id 
                FROM campaign_conversion_goal
                """
                
                payload = json.dumps({
                    "query": query
                })
                
                headers = {
                    'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                }
                
                response = requests.post(url, headers=headers, data=payload)
                
                if response.status_code == 200:
                    # Process the response
                    data = response.json()
                    
                    # Process the data in the required format
                    if isinstance(data, list) and len(data) > 0 and "results" in data[0]:
                        results = data[0]["results"]
                        for item in results:
                            formatted_item = {
                                "account_id": item.get("customer", {}).get("id", account_id),
                                "campaign_id": item.get("campaign", {}).get("id"),
                                "category": item.get("campaignConversionGoal", {}).get("category"),
                                "origin": item.get("campaignConversionGoal", {}).get("origin"),
                                "biddable": item.get("campaignConversionGoal", {}).get("biddable")
                            }
                            all_results.append(formatted_item)
                        
                        print(f"[{datetime.datetime.now()}] Fetched {len(results)} records for account {account_id}")
                    else:
                        print(f"[{datetime.datetime.now()}] No results found for account {account_id}")
                else:
                    print(f"[{datetime.datetime.now()}] Error fetching data for account {account_id}: {response.status_code}, {response.text}")
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Exception for account {account_id}: {str(e)}")
                continue
        
        # Insert all results into the database
        if all_results:
            try:
                inserted_count = bulk_insert_conversion_goals(all_results)
                print(f"[{datetime.datetime.now()}] Successfully inserted {inserted_count} records into the database.")
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Error inserting data into database: {str(e)}")
        else:
            print(f"[{datetime.datetime.now()}] No data to insert.")
        
        print(f"[{datetime.datetime.now()}] Scheduled fetch and save completed.")
        
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error in execute_fetch_and_save: {str(e)}")

@app.get("/scheduler/status")
async def get_scheduler_status():
    """Get the status of the background scheduler and jobs."""
    if not scheduler.running:
        return {"status": "stopped"}
    
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        })
    
    return {
        "status": "running",
        "jobs": jobs
    }

@app.post("/scheduler/run-now")
async def run_scheduler_now():
    """Trigger the fetch and save job to run immediately."""
    try:
        scheduler.add_job(
            func=run_scheduler_fetch_and_save,
            trigger="date",
            run_date=datetime.datetime.now() + datetime.timedelta(seconds=1),
            id="manual_run_job",
            name="Manual fetch and save run",
            replace_existing=True
        )
        
        return {"message": "Fetch and save job scheduled to run immediately"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error scheduling immediate run: {str(e)}")

@app.get("/")
async def root():
    return {"message": "Google Accounts API is running"}

@app.get("/google-accounts")
async def get_google_accounts():
    query = """
    SELECT 
        ga.account_id,
        MAX(ct.refresh_token) AS token,
        MAX(ga.manager_account) AS manager_account  
    FROM 
        public.google_accounts AS ga 
    LEFT JOIN 
        connector_tokens AS ct 
    ON 
        ct.id=ga."connectorTokenId" 
    WHERE 
        ga.deleted_at IS NULL 
    AND 
        ct.active_status='active' 
    GROUP BY 
        ga.account_id
    """
    
    try:
        results = execute_query(query)
        return {"data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        
@app.get("/google-accounts/{account_id}/access-token")
async def get_access_token(account_id: str):
    """Generate a Google Ads API access token for a specific account using the refresh token."""
    try:
        # First get the refresh token for the specified account
        query = f"""
        SELECT 
            ga.account_id,
            MAX(ct.refresh_token) AS token
        FROM 
            public.google_accounts AS ga 
        LEFT JOIN 
            connector_tokens AS ct 
        ON 
            ct.id=ga."connectorTokenId" 
        WHERE 
            ga.deleted_at IS NULL 
        AND 
            ct.active_status='active'
        AND
            ga.account_id = '{account_id}'
        GROUP BY 
            ga.account_id
        """
        
        results = execute_query(query)
        if not results or len(results) == 0:
            raise HTTPException(status_code=404, detail=f"No account found with ID: {account_id}")
        
        refresh_token = results[0]["token"]
        if not refresh_token:
            raise HTTPException(status_code=404, detail=f"No refresh token found for account ID: {account_id}")
        
        # Use the refresh token to get a new access token
        payload = json.dumps({
            "grant_type": "refresh_token",
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token
        })
        
        headers = {
            'Content-Type': 'text/plain'
        }
        
        response = requests.post(
            url="https://www.googleapis.com/oauth2/v3/token",
            headers=headers,
            data=payload
        )
            
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Google OAuth error: {response.text}"
            )
                
        token_data = response.json()
        return token_data
    
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error generating access token: {str(e)}")

@app.get("/all-accounts-access-tokens")
async def get_all_access_tokens():
    """Generate Google Ads API access tokens for all accounts."""
    try:
        # Get all active accounts with refresh tokens
        query = """
        SELECT 
            ga.account_id,
            MAX(ct.refresh_token) AS token
        FROM 
            public.google_accounts AS ga 
        LEFT JOIN 
            connector_tokens AS ct 
        ON 
            ct.id=ga."connectorTokenId" 
        WHERE 
            ga.deleted_at IS NULL 
        AND 
            ct.active_status='active'
        AND
            ct.refresh_token IS NOT NULL
        GROUP BY 
            ga.account_id
        """
        
        accounts = execute_query(query)
        if not accounts or len(accounts) == 0:
            raise HTTPException(status_code=404, detail="No accounts found with valid refresh tokens")
        
        result = []
        
        # Get access token for each account
        for account in accounts:
            account_id = account["account_id"]
            refresh_token = account["token"]
            
            try:
                # Use the refresh token to get a new access token
                payload = json.dumps({
                    "grant_type": "refresh_token",
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "refresh_token": refresh_token
                })
                
                headers = {
                    'Content-Type': 'text/plain'
                }
                
                response = requests.post(
                    url="https://www.googleapis.com/oauth2/v3/token",
                    headers=headers,
                    data=payload
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    # Only include account_id and access_token in the result
                    result.append({
                        "account_id": account_id,
                        "access_token": token_data["access_token"]
                    })
                else:
                    # Add error info to result but continue with other accounts
                    result.append({
                        "account_id": account_id,
                        "error": f"Failed to get token: {response.text}"
                    })
            except Exception as e:
                # Add error info to result but continue with other accounts
                result.append({
                    "account_id": account_id,
                    "error": f"Exception: {str(e)}"
                })
        
        return {"data": result}
    
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error generating access tokens: {str(e)}")

# Add a simplified version that returns only account_id and access_token
@app.get("/account-tokens")
async def get_account_tokens():
    """Get only account_id and access_token for all accounts."""
    try:
        # Get all active accounts with refresh tokens
        query = """
        SELECT 
            ga.account_id,
            MAX(ct.refresh_token) AS token
        FROM 
            public.google_accounts AS ga 
        LEFT JOIN 
            connector_tokens AS ct 
        ON 
            ct.id=ga."connectorTokenId" 
        WHERE 
            ga.deleted_at IS NULL 
        AND 
            ct.active_status='active'
        AND
            ct.refresh_token IS NOT NULL
        GROUP BY 
            ga.account_id
        """
        
        accounts = execute_query(query)
        if not accounts or len(accounts) == 0:
            raise HTTPException(status_code=404, detail="No accounts found with valid refresh tokens")
        
        result = []
        
        # Get access token for each account
        for account in accounts:
            account_id = account["account_id"]
            refresh_token = account["token"]
            
            try:
                # Use the refresh token to get a new access token
                payload = json.dumps({
                    "grant_type": "refresh_token",
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "refresh_token": refresh_token
                })
                
                headers = {
                    'Content-Type': 'text/plain'
                }
                
                response = requests.post(
                    url="https://www.googleapis.com/oauth2/v3/token",
                    headers=headers,
                    data=payload
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    # Only include account_id and access_token in the result
                    result.append({
                        "account_id": account_id,
                        "access_token": token_data["access_token"]
                    })
            except Exception:
                # Skip accounts with errors
                continue
        
        return {"data": result}
    
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error generating access tokens: {str(e)}")

@app.post("/generate-access-token")
async def generate_access_token(token_request: TokenRequest):
    """Generate a Google Ads API access token from a refresh token provided in the request."""
    try:
        payload = json.dumps({
            "grant_type": "refresh_token",
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": token_request.refresh_token
        })
        
        headers = {
            'Content-Type': 'text/plain'
        }
        
        response = requests.post(
            url="https://www.googleapis.com/oauth2/v3/token",
            headers=headers,
            data=payload
        )
            
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Google OAuth error: {response.text}"
            )
                
        token_data = response.json()
        return token_data
    
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error generating access token: {str(e)}")

@app.post("/get-token")
async def get_token(refresh_token: str = Body(..., embed=True)):
    """Simple endpoint that just gets a token using the provided refresh token."""
    payload = json.dumps({
        "grant_type": "refresh_token",
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token
    })
    
    headers = {
        'Content-Type': 'text/plain'
    }
    
    response = requests.post(
        url="https://www.googleapis.com/oauth2/v3/token",
        headers=headers,
        data=payload
    )
    
    return response.json()

@app.get("/campaign-conversion-goals")
async def get_campaign_conversion_goals():
    """Fetch campaign conversion goals from Google Ads API for all accounts."""
    try:
        # First get all access tokens
        accounts_with_tokens = get_account_tokens_internal()
        
        all_results = []
        
        # For each account, make the Google Ads API call
        for account in accounts_with_tokens:
            account_id = account["account_id"]
            access_token = account["access_token"]
            
            try:
                # Call Google Ads API
                url = f"https://googleads.googleapis.com/v17/customers/{account_id}/googleAds:searchStream"
                
                query = """
                SELECT 
                    campaign_conversion_goal.campaign, 
                    campaign_conversion_goal.category, 
                    campaign_conversion_goal.origin, 
                    campaign.app_campaign_setting.bidding_strategy_goal_type, 
                    campaign_conversion_goal.biddable, 
                    campaign_conversion_goal.resource_name, 
                    campaign.id, 
                    customer.id 
                FROM campaign_conversion_goal
                """
                
                payload = json.dumps({
                    "query": query
                })
                
                headers = {
                    'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                }
                
                response = requests.post(url, headers=headers, data=payload)
                
                if response.status_code == 200:
                    # Process the response
                    data = response.json()
                    
                    # Process the data in the required format
                    if isinstance(data, list) and len(data) > 0 and "results" in data[0]:
                        results = data[0]["results"]
                        for item in results:
                            formatted_item = {
                                "account_id": item.get("customer", {}).get("id", account_id),
                                "campaign_id": item.get("campaign", {}).get("id"),
                                "category": item.get("campaignConversionGoal", {}).get("category"),
                                "origin": item.get("campaignConversionGoal", {}).get("origin"),
                                "biddable": item.get("campaignConversionGoal", {}).get("biddable")
                            }
                            all_results.append(formatted_item)
                else:
                    # Add error information but continue with other accounts
                    all_results.append({
                        "account_id": account_id,
                        "error": f"Google Ads API error: {response.status_code}, {response.text}"
                    })
            except Exception as e:
                # Add error information but continue with other accounts
                all_results.append({
                    "account_id": account_id,
                    "error": f"Exception: {str(e)}"
                })
        
        return {"data": all_results}
    
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error fetching campaign conversion goals: {str(e)}")

@app.post("/campaign-conversion-goals/{account_id}")
async def get_campaign_conversion_goals_for_account(account_id: str, gaql_request: GaqlRequest = None):
    """Fetch campaign conversion goals from Google Ads API for a specific account."""
    try:
        # Get access token for this account
        endpoint = f"/google-accounts/{account_id}/access-token"
        
        # Get the refresh token for the specified account
        query = f"""
        SELECT 
            ga.account_id,
            MAX(ct.refresh_token) AS token
        FROM 
            public.google_accounts AS ga 
        LEFT JOIN 
            connector_tokens AS ct 
        ON 
            ct.id=ga."connectorTokenId" 
        WHERE 
            ga.deleted_at IS NULL 
        AND 
            ct.active_status='active'
        AND
            ga.account_id = '{account_id}'
        GROUP BY 
            ga.account_id
        """
        
        results = execute_query(query)
        if not results or len(results) == 0:
            raise HTTPException(status_code=404, detail=f"No account found with ID: {account_id}")
        
        refresh_token = results[0]["token"]
        if not refresh_token:
            raise HTTPException(status_code=404, detail=f"No refresh token found for account ID: {account_id}")
        
        # Use the refresh token to get a new access token
        payload = json.dumps({
            "grant_type": "refresh_token",
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token
        })
        
        headers = {
            'Content-Type': 'text/plain'
        }
        
        token_response = requests.post(
            url="https://www.googleapis.com/oauth2/v3/token",
            headers=headers,
            data=payload
        )
            
        if token_response.status_code != 200:
            raise HTTPException(
                status_code=token_response.status_code, 
                detail=f"Google OAuth error: {token_response.text}"
            )
                
        token_data = token_response.json()
        access_token = token_data["access_token"]
        
        # Call Google Ads API
        url = f"https://googleads.googleapis.com/v17/customers/{account_id}/googleAds:searchStream"
        
        # Use provided query or default query
        if gaql_request and gaql_request.query:
            query = gaql_request.query
        else:
            query = """
            SELECT 
                campaign_conversion_goal.campaign, 
                campaign_conversion_goal.category, 
                campaign_conversion_goal.origin, 
                campaign.app_campaign_setting.bidding_strategy_goal_type, 
                campaign_conversion_goal.biddable, 
                campaign_conversion_goal.resource_name, 
                campaign.id, 
                customer.id 
            FROM campaign_conversion_goal
            """
        
        payload = json.dumps({
            "query": query
        })
        
        headers = {
            'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(url, headers=headers, data=payload)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Google Ads API error: {response.text}"
            )
        
        # Process the response
        data = response.json()
        all_results = []
        
        # Process the data in the required format
        if isinstance(data, list) and len(data) > 0 and "results" in data[0]:
            results = data[0]["results"]
            for item in results:
                formatted_item = {
                    "account_id": item.get("customer", {}).get("id", account_id),
                    "campaign_id": item.get("campaign", {}).get("id"),
                    "category": item.get("campaignConversionGoal", {}).get("category"),
                    "origin": item.get("campaignConversionGoal", {}).get("origin"),
                    "biddable": item.get("campaignConversionGoal", {}).get("biddable")
                }
                all_results.append(formatted_item)
        
        return {"data": all_results}
    
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error fetching campaign conversion goals: {str(e)}")

# Internal function to get account tokens - not exposed as an API endpoint
def get_account_tokens_internal():
    """Get only account_id and access_token for all accounts (internal function)."""
    # Get all active accounts with refresh tokens
    query = """
    SELECT 
        ga.account_id,
        MAX(ct.refresh_token) AS token
    FROM 
        public.google_accounts AS ga 
    LEFT JOIN 
        connector_tokens AS ct 
    ON 
        ct.id=ga."connectorTokenId" 
    WHERE 
        ga.deleted_at IS NULL 
    AND 
        ct.active_status='active'
    AND
        ct.refresh_token IS NOT NULL
    GROUP BY 
        ga.account_id
    """
    
    accounts = execute_query(query)
    if not accounts or len(accounts) == 0:
        raise HTTPException(status_code=404, detail="No accounts found with valid refresh tokens")
    
    result = []
    
    # Get access token for each account
    for account in accounts:
        account_id = account["account_id"]
        refresh_token = account["token"]
        
        try:
            # Use the refresh token to get a new access token
            payload = json.dumps({
                "grant_type": "refresh_token",
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "refresh_token": refresh_token
            })
            
            headers = {
                'Content-Type': 'text/plain'
            }
            
            response = requests.post(
                url="https://www.googleapis.com/oauth2/v3/token",
                headers=headers,
                data=payload
            )
            
            if response.status_code == 200:
                token_data = response.json()
                # Only include account_id and access_token in the result
                result.append({
                    "account_id": account_id,
                    "access_token": token_data["access_token"]
                })
        except Exception:
            # Skip accounts with errors
            continue
    
    return result 

@app.get("/sync-campaign-conversion-goals")
async def sync_campaign_conversion_goals(background_tasks: BackgroundTasks):
    """Fetch campaign conversion goals data and insert into the database in the background."""
    background_tasks.add_task(fetch_and_insert_conversion_goals)
    return {"message": "Synchronization of campaign conversion goals started in the background."}

@app.post("/insert-campaign-conversion-goals")
async def insert_campaign_conversion_goals(data: List[Dict[str, Any]]):
    """Insert campaign conversion goals data into the database."""
    try:
        inserted_count = bulk_insert_conversion_goals(data)
        return {"message": f"Successfully inserted {inserted_count} campaign conversion goals records."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting data: {str(e)}")

def fetch_and_insert_conversion_goals():
    """Fetch campaign conversion goals data and insert into the database."""
    try:
        # Get all accounts with tokens
        accounts_with_tokens = get_account_tokens_internal()
        
        all_results = []
        
        # For each account, make the Google Ads API call
        for account in accounts_with_tokens:
            account_id = account["account_id"]
            access_token = account["access_token"]
            
            try:
                # Call Google Ads API
                url = f"https://googleads.googleapis.com/v17/customers/{account_id}/googleAds:searchStream"
                
                query = """
                SELECT 
                    campaign_conversion_goal.campaign, 
                    campaign_conversion_goal.category, 
                    campaign_conversion_goal.origin, 
                    campaign.app_campaign_setting.bidding_strategy_goal_type, 
                    campaign_conversion_goal.biddable, 
                    campaign_conversion_goal.resource_name, 
                    campaign.id, 
                    customer.id 
                FROM campaign_conversion_goal
                """
                
                payload = json.dumps({
                    "query": query
                })
                
                headers = {
                    'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                }
                
                response = requests.post(url, headers=headers, data=payload)
                
                if response.status_code == 200:
                    # Process the response
                    data = response.json()
                    
                    # Process the data in the required format
                    if isinstance(data, list) and len(data) > 0 and "results" in data[0]:
                        results = data[0]["results"]
                        for item in results:
                            formatted_item = {
                                "account_id": item.get("customer", {}).get("id", account_id),
                                "campaign_id": item.get("campaign", {}).get("id"),
                                "category": item.get("campaignConversionGoal", {}).get("category"),
                                "origin": item.get("campaignConversionGoal", {}).get("origin"),
                                "biddable": item.get("campaignConversionGoal", {}).get("biddable")
                            }
                            all_results.append(formatted_item)
            except Exception as e:
                print(f"Error fetching data for account {account_id}: {str(e)}")
                continue
        
        # Insert all results into the database
        if all_results:
            inserted_count = bulk_insert_conversion_goals(all_results)
            print(f"Successfully inserted {inserted_count} records and refreshed continuous aggregate.")
        else:
            print("No data to insert.")
    
    except Exception as e:
        print(f"Error in fetch_and_insert_conversion_goals: {str(e)}")

def bulk_insert_conversion_goals(data):
    """
    Bulk insert campaign conversion goals data into the database.
    Returns the number of inserted records.
    """
    if not data:
        return 0
    
    # Load environment variables
    load_dotenv()
    
    # Get database connection parameters from .env file
    DB_URL = os.getenv("DATABASE_URL")
    
    # Add current run time to all records
    run_time = datetime.datetime.now()
    for item in data:
        item["run_time"] = run_time
    
    # Connect to the database
    conn = psycopg2.connect(DB_URL)
    inserted_count = 0
    
    try:
        with conn.cursor() as cursor:
            # Create the table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.google_campaign_conversion_goals (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT,
                    campaign_id TEXT,
                    category TEXT,
                    origin TEXT,
                    biddable BOOLEAN,
                    run_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Prepare data for bulk insertion
            insert_query = """
                INSERT INTO public.google_campaign_conversion_goals
                (account_id, campaign_id, category, origin, biddable, run_time)
                VALUES %s
            """
            
            # Convert data to tuples for executemany
            values = [(
                item.get("account_id"),
                item.get("campaign_id"),
                item.get("category"),
                item.get("origin"),
                item.get("biddable"),
                item.get("run_time")
            ) for item in data]
            
            # Use psycopg2.extras.execute_values for efficient bulk insertion
            psycopg2.extras.execute_values(cursor, insert_query, values)
            
            # Commit the transaction for the insert
            conn.commit()
            
            inserted_count = len(data)
    
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        raise e
    
    # Now, outside of the transaction, refresh the continuous aggregate
    # Create a separate connection for this since we need autocommit
    try:
        # Set autocommit mode for this connection
        refresh_conn = psycopg2.connect(DB_URL)
        refresh_conn.autocommit = True
        
        with refresh_conn.cursor() as refresh_cursor:
            refresh_cursor.execute("CALL refresh_continuous_aggregate('google_campaign_conversion_goals_mv', NULL, NULL);")
            
        refresh_conn.close()
    except Exception as e:
        print(f"Warning: Failed to refresh continuous aggregate: {str(e)}")
    
    finally:
        # Close the main connection
        conn.close()
    
    return inserted_count

@app.post("/fetch-and-save-campaign-conversion-goals")
async def fetch_and_save_campaign_conversion_goals():
    """
    Fetch campaign conversion goals from Google Ads API for all accounts and save them to the database.
    Returns the fetched data along with insertion status.
    """
    try:
        # Get all accounts with tokens
        accounts_with_tokens = get_account_tokens_internal()
        
        all_results = []
        
        # For each account, make the Google Ads API call
        for account in accounts_with_tokens:
            account_id = account["account_id"]
            access_token = account["access_token"]
            
            try:
                # Call Google Ads API
                url = f"https://googleads.googleapis.com/v17/customers/{account_id}/googleAds:searchStream"
                
                query = """
                SELECT 
                    campaign_conversion_goal.campaign, 
                    campaign_conversion_goal.category, 
                    campaign_conversion_goal.origin, 
                    campaign.app_campaign_setting.bidding_strategy_goal_type, 
                    campaign_conversion_goal.biddable, 
                    campaign_conversion_goal.resource_name, 
                    campaign.id, 
                    customer.id 
                FROM campaign_conversion_goal
                """
                
                payload = json.dumps({
                    "query": query
                })
                
                headers = {
                    'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                }
                
                response = requests.post(url, headers=headers, data=payload)
                
                if response.status_code == 200:
                    # Process the response
                    data = response.json()
                    
                    # Process the data in the required format
                    if isinstance(data, list) and len(data) > 0 and "results" in data[0]:
                        results = data[0]["results"]
                        for item in results:
                            formatted_item = {
                                "account_id": item.get("customer", {}).get("id", account_id),
                                "campaign_id": item.get("campaign", {}).get("id"),
                                "category": item.get("campaignConversionGoal", {}).get("category"),
                                "origin": item.get("campaignConversionGoal", {}).get("origin"),
                                "biddable": item.get("campaignConversionGoal", {}).get("biddable")
                            }
                            all_results.append(formatted_item)
                else:
                    # Add error information but continue with other accounts
                    print(f"Error fetching data for account {account_id}: {response.status_code}, {response.text}")
            except Exception as e:
                print(f"Error fetching data for account {account_id}: {str(e)}")
                continue
        
        # Insert all results into the database
        insertion_result = {"inserted_count": 0, "success": False, "continuous_aggregate_refreshed": False}
        if all_results:
            try:
                inserted_count = bulk_insert_conversion_goals(all_results)
                insertion_result = {
                    "inserted_count": inserted_count,
                    "success": True,
                    "continuous_aggregate_refreshed": True
                }
            except Exception as e:
                insertion_result = {
                    "error": str(e),
                    "success": False,
                    "continuous_aggregate_refreshed": False
                }
        
        return {
            "data": all_results,
            "insertion_result": insertion_result,
            "total_accounts_processed": len(accounts_with_tokens),
            "total_records_retrieved": len(all_results)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching and saving campaign conversion goals: {str(e)}")

@app.post("/fetch-and-save-campaign-conversion-goals/{account_id}")
async def fetch_and_save_campaign_conversion_goals_for_account(account_id: str):
    """
    Fetch campaign conversion goals from Google Ads API for a specific account ID and save them to the database.
    Returns the fetched data along with insertion status.
    """
    try:
        # Get the access token for this specific account
        access_token = None
        
        # First get the refresh token for the specified account
        query = f"""
        SELECT 
            ga.account_id,
            MAX(ct.refresh_token) AS token
        FROM 
            public.google_accounts AS ga 
        LEFT JOIN 
            connector_tokens AS ct 
        ON 
            ct.id=ga."connectorTokenId" 
        WHERE 
            ga.deleted_at IS NULL 
        AND 
            ct.active_status='active'
        AND
            ga.account_id = '{account_id}'
        GROUP BY 
            ga.account_id
        """
        
        results = execute_query(query)
        if not results or len(results) == 0:
            raise HTTPException(status_code=404, detail=f"No account found with ID: {account_id}")
        
        refresh_token = results[0]["token"]
        if not refresh_token:
            raise HTTPException(status_code=404, detail=f"No refresh token found for account ID: {account_id}")
        
        # Get a fresh access token
        payload = json.dumps({
            "grant_type": "refresh_token",
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token
        })
        
        headers = {
            'Content-Type': 'text/plain'
        }
        
        token_response = requests.post(
            url="https://www.googleapis.com/oauth2/v3/token",
            headers=headers,
            data=payload
        )
            
        if token_response.status_code != 200:
            raise HTTPException(
                status_code=token_response.status_code, 
                detail=f"Google OAuth error: {token_response.text}"
            )
                
        token_data = token_response.json()
        access_token = token_data["access_token"]
        
        # Now fetch the campaign conversion goals for this account
        all_results = []
        
        try:
            # Call Google Ads API
            url = f"https://googleads.googleapis.com/v17/customers/{account_id}/googleAds:searchStream"
            
            query = """
            SELECT 
                campaign_conversion_goal.campaign, 
                campaign_conversion_goal.category, 
                campaign_conversion_goal.origin, 
                campaign.app_campaign_setting.bidding_strategy_goal_type, 
                campaign_conversion_goal.biddable, 
                campaign_conversion_goal.resource_name, 
                campaign.id, 
                customer.id 
            FROM campaign_conversion_goal
            """
            
            payload = json.dumps({
                "query": query
            })
            
            headers = {
                'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.post(url, headers=headers, data=payload)
            
            if response.status_code == 200:
                # Process the response
                data = response.json()
                
                # Process the data in the required format
                if isinstance(data, list) and len(data) > 0 and "results" in data[0]:
                    results = data[0]["results"]
                    for item in results:
                        formatted_item = {
                            "account_id": item.get("customer", {}).get("id", account_id),
                            "campaign_id": item.get("campaign", {}).get("id"),
                            "category": item.get("campaignConversionGoal", {}).get("category"),
                            "origin": item.get("campaignConversionGoal", {}).get("origin"),
                            "biddable": item.get("campaignConversionGoal", {}).get("biddable")
                        }
                        all_results.append(formatted_item)
                else:
                    return {
                        "message": f"No results found in the API response for account {account_id}",
                        "raw_response": data
                    }
            else:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Google Ads API error: {response.text}"
                )
        except Exception as e:
            raise HTTPException(
                status_code=500, 
                detail=f"Error fetching data from Google Ads API for account {account_id}: {str(e)}"
            )
        
        # Insert the fetched data into the database
        insertion_result = {"inserted_count": 0, "success": False, "continuous_aggregate_refreshed": False}
        if all_results:
            try:
                inserted_count = bulk_insert_conversion_goals(all_results)
                insertion_result = {
                    "inserted_count": inserted_count,
                    "success": True,
                    "continuous_aggregate_refreshed": True
                }
            except Exception as e:
                insertion_result = {
                    "error": str(e),
                    "success": False,
                    "continuous_aggregate_refreshed": False
                }
        
        return {
            "account_id": account_id,
            "data": all_results,
            "insertion_result": insertion_result,
            "total_records_retrieved": len(all_results)
        }
    
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error processing account {account_id}: {str(e)}") 