import base64
import hashlib
import time
import jwt  # PyJWT
import requests
from cryptography.hazmat.primitives import serialization
import os
import uuid
from cryptography.hazmat.primitives._serialization import Encoding, PublicFormat
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()

# === CONFIGURATION ===
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")  # e.g., 'xy12345.us-east-1'
USER = os.getenv("SNOWFLAKE_USER")
PRIVATE_KEY_PATH = '../keypairs/rsa_private_key.pem'
QUERY = "SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE()"
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
ROLE = os.getenv("SNOWFLAKE_ROLE")

QUALIFIED_USERNAME = ACCOUNT.split('-')[0].upper() + '.' + USER.upper()
print(QUALIFIED_USERNAME)

# === LOAD PRIVATE KEY ===
with open(PRIVATE_KEY_PATH, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )

# Get the raw bytes of public key.
public_key_raw = private_key.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)

# Get the sha256 hash of the raw bytes.
sha256hash = hashlib.sha256()
sha256hash.update(public_key_raw)

# Base64-encode the value and prepend the prefix 'SHA256:'.
public_key_fp = 'SHA256:' + base64.b64encode(sha256hash.digest()).decode('utf-8')

print(public_key_fp)

print(f"{QUALIFIED_USERNAME}.{public_key_fp}")

# === GENERATE JWT TOKEN ===
now = datetime.now(timezone.utc)
jwt_payload = {
    "iss": f"{QUALIFIED_USERNAME}.{public_key_fp}",
    "sub": QUALIFIED_USERNAME,
    "iat": now,
    "exp": now + timedelta(minutes = 5)
}

token = jwt.encode(jwt_payload, private_key, algorithm="RS256")

if isinstance(token, bytes):
    token = token.decode('utf-8')

# === EXECUTE QUERY ===
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
    'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
    'Accept': 'application/json',
    'User-Agent': 'myApplicationName/1.0',
}

request_id = str(uuid.uuid4())
query_url = f"https://{ACCOUNT}.snowflakecomputing.com/api/v2/statements"

query_payload = {
    "statement": QUERY,
    "timeout": 60,
    "database": DATABASE,
    "schema": SCHEMA,
    "warehouse": WAREHOUSE,
    "role": ROLE
}

# Submit the query
response = requests.post(query_url, json=query_payload, headers=headers)
# if not response.ok:
#     print("Full response text:", response.text)
#     response.raise_for_status()
# response.raise_for_status()
resp_data = response.json()
print(resp_data)
handle = resp_data['statementHandle']
print(f"Submitted query with handle: {handle}")

# Poll for results
status_url = f"https://{ACCOUNT}.snowflakecomputing.com/api/v2/statements/{handle}"
while True:
    time.sleep(2)
    status_response = requests.get(status_url, headers=headers)
    status_response.raise_for_status()

    data = status_response.json()
    status = data['statementStatus']
    print(f"Query status: {status}")

    if status == 'SUCCESS':
        print("Query result:")
        print(data.get('data'))  # may include rows, depending on result size
        break
    elif status in ['FAILED_WITH_ERROR', 'ABORTED']:
        raise Exception(f"Query failed: {data}")