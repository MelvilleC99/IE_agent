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
    cred = credentials.Certificate("firebase-service-account.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()
