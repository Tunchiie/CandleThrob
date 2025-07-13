import oci
import base64

signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
secrets_client = oci.secrets.SecretsClient(config={}, signer=signer)

def get_secret(secret_id:str)-> str:
    """
    Fetch a secret from Oracle Vault using its OCID.
    
    Args:
        secret_id (str): The OCID of the secret to fetch.

    Returns:
        str: The decoded secret value.
    """
    try:
        secret_bundle = secrets_client.get_secret_bundle(secret_id=secret_id).data
        decoded_secret = base64.b64decode(
            secret_bundle.secret_bundle_content.content.encode("utf-8")
        ).decode("utf-8")
        return decoded_secret
    except Exception as e:
        raise RuntimeError(f"Failed to fetch secret: {e}") from e