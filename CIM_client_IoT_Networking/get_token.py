from token_client import TokenClient

def main():
    client = TokenClient()
    token = client.get_token()
    print("=== Access Token ===")
    print(token)

if __name__ == "__main__":
    main()
