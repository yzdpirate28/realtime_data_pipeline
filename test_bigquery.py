#!/usr/bin/env python3
"""
Pandas-free BigQuery authentication test
This version avoids all pandas dependencies by using direct REST API calls
"""

import os
import sys
import json
import requests
from urllib.parse import urlencode

def check_environment():
    """Check if required environment variables and files exist"""
    print("="*50)
    print("Checking Environment Configuration")
    print("="*50)
    
    # Check credentials file
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', './secrets/bigquery-key.json')
    print(f"Credentials file: {credentials_path}")
    
    if not os.path.exists(credentials_path):
        print(f"❌ ERROR: Credentials file not found at {credentials_path}")
        return False
    
    print(f"✅ Credentials file found")
    
    # Try to read and validate JSON
    try:
        with open(credentials_path, 'r') as f:
            creds = json.load(f)
        
        if 'project_id' in creds:
            print(f"✅ Service account project: {creds['project_id']}")
            return creds
        else:
            print("❌ ERROR: Invalid service account key - no project_id found")
            return False
            
    except json.JSONDecodeError:
        print("❌ ERROR: Service account key is not valid JSON")
        return False
    except Exception as e:
        print(f"❌ ERROR reading credentials file: {e}")
        return False

def get_access_token(credentials):
    """Get Google Cloud access token using service account credentials"""
    import time
    import base64
    import hashlib
    import hmac
    from urllib.parse import urlencode
    
    try:
        # JWT header
        header = {
            "alg": "RS256",
            "typ": "JWT"
        }
        
        # JWT payload
        now = int(time.time())
        payload = {
            "iss": credentials["client_email"],
            "scope": "https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/cloud-platform",
            "aud": "https://oauth2.googleapis.com/token",
            "exp": now + 3600,
            "iat": now
        }
        
        # Encode header and payload
        header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip('=')
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')
        
        # Create signature
        message = f"{header_b64}.{payload_b64}"
        
        # Import RSA for signing
        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa, padding
            import base64
            
            # Load private key
            private_key_data = credentials["private_key"]
            private_key = serialization.load_pem_private_key(
                private_key_data.encode(),
                password=None
            )
            
            # Sign the message
            signature = private_key.sign(
                message.encode(),
                padding.PKCS1v15(),
                hashes.SHA256()
            )
            
            signature_b64 = base64.urlsafe_b64encode(signature).decode().rstrip('=')
            
        except ImportError:
            print("❌ cryptography library not found. Installing...")
            print("Please run: pip install cryptography")
            return None
        
        # Create JWT
        jwt_token = f"{message}.{signature_b64}"
        
        # Exchange JWT for access token
        token_url = "https://oauth2.googleapis.com/token"
        token_data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_token
        }
        
        response = requests.post(token_url, data=token_data)
        
        if response.status_code == 200:
            token_info = response.json()
            return token_info.get("access_token")
        else:
            print(f"❌ Token request failed: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error getting access token: {e}")
        return None

def test_bigquery_with_rest_api(credentials, access_token):
    """Test BigQuery access using REST API instead of client library"""
    print("\n" + "="*50)
    print("Testing BigQuery Access (REST API)")
    print("="*50)
    
    project_id = credentials["project_id"]
    base_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # List datasets
        response = requests.get(f"{base_url}/datasets", headers=headers)
        
        if response.status_code == 200:
            datasets = response.json()
            dataset_count = len(datasets.get("datasets", []))
            print(f"✅ BigQuery API access successful")
            print(f"   Project: {project_id}")
            print(f"   Found {dataset_count} datasets")
            return True
        else:
            print(f"❌ BigQuery API request failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ BigQuery API test failed: {e}")
        return False

def test_storage_with_rest_api(credentials, access_token):
    """Test Cloud Storage access using REST API"""
    print("\n" + "="*50)
    print("Testing Cloud Storage Access (REST API)")  
    print("="*50)
    
    project_id = credentials["project_id"]
    base_url = f"https://storage.googleapis.com/storage/v1/b"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # List buckets
        params = {"project": project_id}
        response = requests.get(base_url, headers=headers, params=params)
        
        if response.status_code == 200:
            buckets = response.json()
            bucket_count = len(buckets.get("items", []))
            print(f"✅ Cloud Storage API access successful")
            print(f"   Project: {project_id}")
            print(f"   Found {bucket_count} buckets")
            
            # Check temp bucket
            temp_bucket = os.environ.get('BQ_TEMP_BUCKET')
            if temp_bucket:
                bucket_response = requests.get(f"{base_url}/{temp_bucket}", headers=headers)
                if bucket_response.status_code == 200:
                    print(f"✅ Temp bucket '{temp_bucket}' is accessible")
                    return True
                else:
                    print(f"❌ Cannot access temp bucket '{temp_bucket}'")
                    print(f"   Status: {bucket_response.status_code}")
                    return False
            else:
                print("⚠️  No temp bucket specified")
                return True
                
        else:
            print(f"❌ Storage API request failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Storage API test failed: {e}")
        return False

def create_dataset_with_rest_api(credentials, access_token):
    """Create BigQuery dataset using REST API if it doesn't exist"""
    print("\n" + "="*50)
    print("Checking/Creating BigQuery Dataset (REST API)")
    print("="*50)
    
    project_id = credentials["project_id"]
    dataset_id = os.environ.get('BQ_DATASET_ID', 'streaming_data')
    
    base_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Check if dataset exists
        response = requests.get(f"{base_url}/datasets/{dataset_id}", headers=headers)
        
        if response.status_code == 200:
            print(f"✅ Dataset '{dataset_id}' already exists")
            return True
        elif response.status_code == 404:
            # Dataset doesn't exist, create it
            print(f"Creating dataset '{dataset_id}'...")
            
            dataset_config = {
                "datasetReference": {
                    "datasetId": dataset_id,
                    "projectId": project_id
                },
                "location": "US"
            }
            
            create_response = requests.post(
                f"{base_url}/datasets",
                headers=headers,
                data=json.dumps(dataset_config)
            )
            
            if create_response.status_code == 200:
                print(f"✅ Dataset '{dataset_id}' created successfully")
                return True
            else:
                print(f"❌ Failed to create dataset: {create_response.status_code}")
                print(f"   Response: {create_response.text}")
                return False
        else:
            print(f"❌ Error checking dataset: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Dataset creation failed: {e}")
        return False

def main():
    """Main test function using REST APIs only"""
    print("Pandas-Free BigQuery Configuration Test")
    print("This script tests BigQuery using REST APIs to avoid pandas dependencies")
    print()
    
    # Check environment and get credentials
    credentials = check_environment()
    if not credentials:
        print("\n❌ Environment check failed. Please fix the issues above.")
        sys.exit(1)
    
    # Check environment variables
    project_id = os.environ.get('BQ_PROJECT_ID')
    temp_bucket = os.environ.get('BQ_TEMP_BUCKET')
    dataset_id = os.environ.get('BQ_DATASET_ID', 'streaming_data')
    table_id = os.environ.get('BQ_TABLE_ID', 'aggregated_metrics')
    
    print(f"\nEnvironment Variables:")
    print(f"  BQ_PROJECT_ID: {project_id}")
    print(f"  BQ_DATASET_ID: {dataset_id}")
    print(f"  BQ_TABLE_ID: {table_id}")
    print(f"  BQ_TEMP_BUCKET: {temp_bucket}")
    
    if not project_id or not temp_bucket:
        print("❌ Missing required environment variables")
        sys.exit(1)
    
    print("✅ Environment variables configured")
    
    # Get access token
    print("\nGetting access token...")
    access_token = get_access_token(credentials)
    if not access_token:
        print("❌ Failed to get access token")
        sys.exit(1)
    
    print("✅ Access token obtained")
    
    # Test BigQuery
    if not test_bigquery_with_rest_api(credentials, access_token):
        print("\n❌ BigQuery test failed")
        sys.exit(1)
    
    # Test Storage
    if not test_storage_with_rest_api(credentials, access_token):
        print("\n❌ Storage test failed")
        sys.exit(1)
    
    # Create dataset
    if not create_dataset_with_rest_api(credentials, access_token):
        print("\n❌ Dataset creation failed")
        sys.exit(1)
    
    print("\n" + "="*50)
    print("🎉 ALL TESTS PASSED!")
    print("="*50)
    print("Your BigQuery configuration is working correctly!")
    print("The Spark application should now be able to connect to BigQuery.")

if __name__ == "__main__":
    main()