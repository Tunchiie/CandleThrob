import os 
import oracledb
from CandleThrob.utils.vault import get_secret

try:
    
    oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient")
    print("Connecting with", get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaa6qj7ezmhqtuqxjn7f54xbpfalsszppuhoo6zilyxzra"))


    ORACLE_USER = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaa6qj7ezmhqtuqxjn7f54xbpfalsszppuhoo6zilyxzra")
    ORACLE_PASSWORD = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaf63vh4v5e3ozokfp32cyk2ct6qjo5eb6z5mutkoppl4q")
    ORACLE_DSN = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyacow4ffwcgtu6uwsw5ujw5w64eeashh5ndeldxzy3y52q")
    
    print("STEP 0  ➜  Initializing Oracle DB connection")
    print("🔐 User:", ORACLE_USER)
    print("🔐 Password:", "********")  # Do not print sensitive information
    print("STEP 1  ➜  calling oracledb.connect()")
    print("🔐 DSN:", ORACLE_DSN)
    print("🔐 Wallet files:", os.listdir("C:\\Users\\Tunchiie\\Downloads\\Wallet_candleThroblake"))
    print("🔐 lib dir exists?", os.path.exists("C:\\Users\\Tunchiie\\Downloads\\Wallet_candleThroblake"))


    conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN,
        config_dir="C:\\Users\\Tunchiie\\Downloads\\Wallet_candleThroblake",
        wallet_location="C:\\Users\\Tunchiie\\Downloads\\Wallet_candleThroblake",
        wallet_password="Sukidakara18"
    )
    
    print("STEP 2  ➜  oracledb.connect() completed successfully")
    print("Connected to Oracle DB successfully.")
    print("Success! DB version:", conn.version)
    
    conn.close()

except Exception as e:
    print("❌ Error occurred:", e)