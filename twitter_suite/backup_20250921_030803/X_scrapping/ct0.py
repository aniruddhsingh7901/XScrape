import requests
import time

# Input and output files
INPUT_FILE = "/home/im/Downloads/twitter/twitteracc.txt"
OUTPUT_FILE = "accounts_with_ct0.txt"

# Fake browser headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://twitter.com/",
    "Origin": "https://twitter.com"
}

def get_ct0(auth_token):
    session = requests.Session()
    session.cookies.set("auth_token", auth_token, domain=".twitter.com")

    try:
        res = session.get("https://twitter.com/home", headers=HEADERS, timeout=15)
        
        # Extract ct0 manually from the response cookies
        for cookie in session.cookies:
            if cookie.name == "ct0":
                return cookie.value
        
        print(f"[!] No ct0 found in cookies for token starting {auth_token[:6]}")
        return None

    except Exception as e:
        print(f"Error fetching ct0 for auth_token {auth_token[:5]}...: {e}")
        return None

def process_accounts():
    with open(INPUT_FILE, "r") as infile, open(OUTPUT_FILE, "w") as outfile:
        for line in infile:
            parts = line.strip().split(":")
            if len(parts) != 5:
                print(f"Skipping malformed line: {line}")
                continue

            username, password, email, email_password, auth_token = parts
            ct0 = get_ct0(auth_token)

            if ct0:
                # You can customize the output format below:
                output_line = f"{username}:{password}:{email}:{email_password}:{auth_token}:{ct0}\n"
                print(f"[+] Success: {username} => ct0: {ct0}")
                outfile.write(output_line)
            else:
                print(f"[-] Failed: {username} => no ct0 found")

            time.sleep(1)  # avoid hitting rate limits

if __name__ == "__main__":
    process_accounts()


