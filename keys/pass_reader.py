import subprocess
import os 

ENCRYPTED_FILE = "keys/encrypted_password.bin"
PASSPHRASE = "salt_is_salt"

def get_passcode(encrypted_file: str = ENCRYPTED_FILE, passphrase: str = PASSPHRASE) -> str: 
    command = [
        "openssl", "enc", "-aes-256-cbc", "-d", "-pbkdf2",
        "-k", passphrase,
        "-in", encrypted_file
    ]
    
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        decrypted_password = result.stdout.strip()
        return decrypted_password
    except subprocess.CalledProcessError as e:
        print("Error decrypting password:", e.stderr)
        return None