from token_client import TokenClient

client = TokenClient()

# This will check the 8-hour logic automatically
token = client.get_token()
print(f"Token: {token}")

# This will ignore the cache and force a new one right now
forced_token = client.get_token(force=True)
print(f"Forced Token: {forced_token}")
