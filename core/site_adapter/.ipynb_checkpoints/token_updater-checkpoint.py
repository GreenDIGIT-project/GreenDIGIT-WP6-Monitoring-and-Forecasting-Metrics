import os
import sys
import requests
import time
from dotenv import load_dotenv, set_key

# Ορίζουμε το ίδιο path με το site_adapter.py
ENV_PATH = '/home/greendigit/app/greendigit-l3-iot-uth-deployment/.env'

def fetch_slices_token():
    """
    Διαβάζει τα credentials από το .env, καλεί το auth API του SLICES, 
    και αποθηκεύει το νέο token πίσω στο .env.
    """
    load_dotenv(ENV_PATH)
    
    # Διαβάζουμε τα credentials. (Θα πρέπει να τα προσθέσεις στο .env αν δεν υπάρχουν)
    email = os.getenv("API_EMAIL")
    password = os.getenv("API_PASSWORD")
    site_id = os.getenv("API_SITE_ID", "SLICES-GR-UTH")

    if not email or not password:
        print("❌ Σφάλμα: Λείπουν τα credentials. Πρόσθεσε τα API_EMAIL και API_PASSWORD στο .env αρχείο σου.")
        return

    url = "https://gd3.lab.uvalight.net/auth/token"
    
    # Οι παράμετροι του GET request (querystring)
    params = {
        "email": email,
        "password": password,
        "site_id": site_id,
        "role": "site_admin"
    }
    
    headers = {
        "accept": "application/json"
    }

    print(f"🔄 [SLICES] Γίνεται προσπάθεια λήψης νέου token για τον χρήστη {email}...")
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # Ανάλογα με το πώς επιστρέφει το API το token 
            # (συνήθως είναι dictionary της μορφής {"access_token": "..."})
            if isinstance(data, dict):
                new_token = data.get("access_token") or data.get("token")
            else:
                new_token = data # Αν το επιστρέφει ως σκέτο string

            if not new_token:
                print(f"❌ [SLICES] Αποτυχία! Το token δεν βρέθηκε στη δομή της απάντησης. Raw response: {data}")
                return

            # Η μαγική εντολή που κάνει update το .env αρχείο
            set_key(ENV_PATH, "API_TOKEN", str(new_token))
            
            print(f"✅ [SLICES] Επιτυχία! Το νέο token λήφθηκε και αποθηκεύτηκε με ασφάλεια στο αρχείο .env")
        else:
            print(f"❌ [SLICES] Σφάλμα API (Κωδικός {response.status_code}): {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ [SLICES] Αποτυχία σύνδεσης στο Auth API: {e}")

def fetch_wattnet_token():
    """
    Διαβάζει τα credentials του Wattnet από το .env, καλεί το auth API του,
    και αποθηκεύει το νέο token πίσω στο .env ως WATTNET_TOKEN.
    """
    load_dotenv(ENV_PATH)
    
    wattnet_email = os.getenv("API_EMAIL")
    wattnet_pass = os.getenv("WATTNET_PASSWORD")
    
    if not wattnet_email or not wattnet_pass:
        print("⚠️ [WATTNET] Προειδοποίηση: Λείπουν τα WATTNET_EMAIL και WATTNET_PASSWORD από το .env. Η ανανέωση ακυρώνεται.")
        return

    url = "https://api.wattnet.eu/token-request/get_token" 
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        "email": wattnet_email,
        "password": wattnet_pass
    }

    print(f"🔄 [WATTNET] Προσπάθεια λήψης νέου token...")
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        
        if response.status_code == 200:
            resp_data = response.json()
            new_token = resp_data.get("access_token")

            if new_token:
                set_key(ENV_PATH, "WATTNET_TOKEN", str(new_token))
                print(f"✅ [WATTNET] Επιτυχία! Το νέο token αποθηκεύτηκε στο .env")
            else:
                print(f"❌ [WATTNET] Αποτυχία! Το token δεν βρέθηκε στην απάντηση.")
        else:
            print(f"❌ [WATTNET] Σφάλμα API (Κωδικός {response.status_code}): {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ [WATTNET] Αποτυχία σύνδεσης στο Auth API: {e}")

if __name__ == "__main__":
    print("🚀 Ξεκινάει ο Token Updater. Θα ανανεώνει τα tokens (SLICES & WATTNET) αυτόματα κάθε 8 ώρες...")
    while True:
        fetch_slices_token()
        fetch_wattnet_token()
        
        # Αναμονή για 8 ώρες (8 ώρες * 3600 δευτερόλεπτα)
        print(f"[{time.strftime('%H:%M:%S')}] ⏳ Αναμονή 8 ώρες για την επόμενη ανανέωση...\n")
        time.sleep(8 * 3600)