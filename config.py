from keys.pass_reader import get_passcode
CH_HOST = "<clickhouse-host>"  # Replace with your ClickHouse host
CH_PORT = 8123    
CH_USER = "<username>" 
CH_PASSWORD = get_passcode()
CH_DATABASE = "eis_cs" 
CH_TABLE = "CountingSystemXY" 
BATCH_SIZE = 100000
