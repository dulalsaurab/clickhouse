## get_password()

IMPORTANT: Encrypting and Fetching password almost secretely. 

We use out of band openssl to encrypt clickhouse password

use this command to encrypt your password: `echo -n "<your-clickhouse-password>" | openssl enc -aes-256-cbc -pbkdf2 -k "<some-secret>" -out encrypted_password.bin`

And read it using openssl from python (this is done my pass_reader.py) 
