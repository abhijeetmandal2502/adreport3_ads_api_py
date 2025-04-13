# Google Accounts API

A FastAPI application that connects to a PostgreSQL database to retrieve Google Accounts information and Google Ads data.

## Setup

1. Create a `.env` file with the required variables:
```
DATABASE_URL=postgresql://username:password@host/database
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
GOOGLE_ADS_DEVELOPER_TOKEN=your_google_ads_developer_token
```

2. Activate your virtual environment:
```
source .venv/bin/activate
```

3. Install the required dependencies:
```
pip install -r requirements.txt
```

## Development

Run the application in development mode with:
```
python run.py
```

## Production

For production deployment, use the provided script:
```
./start_production.sh
```

This script:
- Sets up the virtual environment if needed
- Installs dependencies
- Runs the application with Gunicorn and Uvicorn workers
- Configures optimal worker processes based on available CPU cores

## API Endpoints

- `GET /`: Check if the API is running
- `GET /google-accounts`: Retrieve Google accounts data with tokens and manager accounts
- `GET /google-accounts/{account_id}/access-token`: Get a Google Ads API access token for a specific account
- `GET /all-accounts-access-tokens`: Get access tokens for all accounts
- `GET /account-tokens`: Get simplified account_id and access_token pairs
- `GET /campaign-conversion-goals`: Get campaign conversion goals data
- `POST /fetch-and-save-campaign-conversion-goals`: Fetch campaign conversion goals and save to database
- `POST /fetch-and-save-campaign-conversion-goals/{account_id}`: Fetch goals for specific account
- `GET /scheduler/status`: Check the status of the scheduler
- `POST /scheduler/run-now`: Manually trigger the scheduler

## Scheduled Tasks

The application includes a background scheduler that runs every 12 hours to:
- Fetch campaign conversion goals data from the Google Ads API
- Save the data to the PostgreSQL database

## Security Notes

- Never commit sensitive information like API keys or database credentials
- Always use environment variables for sensitive information
- The `.env` file is excluded from Git in the `.gitignore` file

## API Documentation

FastAPI provides automatic interactive documentation:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc 