from pathlib import Path
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048
)

pem_private = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Serialize public key
public_key = private_key.public_key()
pem_public = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

key_dir = Path("../keypairs")
key_dir.mkdir(exist_ok=True)

private_key_path = key_dir / "rsa_private_key.pem"
public_key_path = key_dir / "rsa_public_key.pem"

private_key_path.write_bytes(pem_private)
public_key_path.write_bytes(pem_public)

print(f"Saved private key to {private_key_path}")
print(f"Saved public key to {public_key_path}")