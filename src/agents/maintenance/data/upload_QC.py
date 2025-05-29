import json
from supabase import create_client, Client

# Replace these with your Supabase project URL and anon key
SUPABASE_URL = "https://xzubquphybaggsxuakjb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inh6dWJxdXBoeWJhZ2dzeHVha2piIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NzIwOTk2NCwiZXhwIjoyMDYyNzg1OTY0fQ.YeIoezqCJwg7iKZa9Lt7x6Mk9day3fWDOzmL1DZDYBY"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def convert_keys_to_lowercase(record):
    new_rec = {k.lower(): v for k, v in record.items()}
    if 'status' not in new_rec or new_rec['status'] is None:
        new_rec['status'] = 'open'
    return new_rec

def upload_json_to_table(json_file_path, table_name):
    with open(json_file_path, "r") as f:
        records = json.load(f)

    records = [convert_keys_to_lowercase(rec) for rec in records]

    chunk_size = 100
    for i in range(0, len(records), chunk_size):
        batch = records[i:i+chunk_size]
        response = supabase.table(table_name).insert(batch).execute()

        # Check if response has status_code attribute and handle accordingly
        if hasattr(response, 'status_code'):
            if response.status_code != 201:
                print(f"Error inserting batch starting at {i}: {response.data}")
            else:
                print(f"Inserted batch {i//chunk_size + 1} successfully.")
        else:
            # If no status_code, print full response for debugging
            print(f"Response from Supabase for batch starting at {i}: {response}")

if __name__ == "__main__":
    upload_json_to_table("reworks.json", "reworks")
    upload_json_to_table("rejects.json", "rejects")