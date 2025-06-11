from cryptography.hazmat.primitives import serialization
import base64

# Load PEM-formatted public key from file
with open("../keypairs/rsa_public_key.pem", "rb") as pem_file:
    public_key = serialization.load_pem_public_key(pem_file.read())

# Convert to DER (binary) format
der_bytes = public_key.public_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

# Encode in Base64 for Snowflake
b64_encoded_der = base64.b64encode(der_bytes).decode("utf-8")

print("Base64-encoded DER public key:\n")
print(b64_encoded_der)