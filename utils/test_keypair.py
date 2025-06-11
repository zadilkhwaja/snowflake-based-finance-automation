from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
load_dotenv()


# Load your RSA private key
with open("../keypairs/rsa_private_key.pem", "rb") as f:
    pem_data = f.read()

# Load the key from PEM
private_key = serialization.load_pem_private_key(
    pem_data,
    password=None,  # Ensure the key is unencrypted
    backend=default_backend()
)

# Convert to DER format (PKCS#8, unencrypted)
der_private_key = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Build Snowpark session with RSA key auth
session = Session.builder.configs({
    "user": os.getenv("SNOWFLAKE_USER"),
    "private_key": der_private_key,
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE")  # Optional
}).create()

# Run a simple test query
df = session.sql("SELECT CURRENT_USER(), CURRENT_ROLE()")
df.show()
