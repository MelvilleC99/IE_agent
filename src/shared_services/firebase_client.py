import sys
import os

# Determine the path to the src directory by going one level up from shared_services
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now you can import settings from config
from config.settings import FIREBASE_CONFIG

import firebase_admin
from firebase_admin import credentials, firestore

# Only initialize once
if not firebase_admin._apps:
    # Define the exact path to your credentials
    cred_path = os.path.join(os.path.dirname(project_root), "secrets/firebase-service-account.json")
    
    # Check if the file exists
    if not os.path.exists(cred_path):
        raise ValueError(f"Firebase credentials not found at: {cred_path}")
    
    # Initialize with credentials
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    print(f"Firebase initialized with credentials from: {cred_path}")

# Get Firestore client
db = firestore.client()