kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}
jdbc_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "username": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "my_url": "jdbc:mysql://217.61.57.46:3306/msolonin",
    "driver": "com.mysql.jdbc.Driver",
    "athlete_enriched_results": "athlete_enriched_results"
}

MY_NAME = "msolonin"
ATHLETE_TOPIC_NAME = f"athletes_{MY_NAME}"
OUTPUT_TOPIC_NAME = f"output_{MY_NAME}"
GOLD = "gold"
SILVER = "silver"
BRONZE = "bronze"
