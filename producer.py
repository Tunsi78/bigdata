from kafka import *
import datetime as dt
import numpy as np
import uuid
from time import sleep

import json

def generateur_transaction():
    type_ = ['achat', 'remboursement', 'transfert']
    methodes_ = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']

    current_time = dt.datetime.now().isoformat()
    transaction_data = {"id_transaction": str(uuid.uuid4()),
        "type_transaction": np.random.choice(type_),
        "montant": round(np.random.uniform(10.0, 1e5), 2),
        "devise": "USD",
        "date": current_time,
        "moyen_paiement": np.random.choice(methodes_)}

    return transaction_data

def gen_producteur():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],  
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    # Vérifier la connexion en envoyant un message vide
    try:
        producer.send('transactions', value={"test": "ping"})
        producer.flush()
        print("Connexion à Kafka réussie")
    except Exception as e:
        print(f"Erreur de connexion à Kafka : {e}")
        exit(1)

    return producer

def send_transactions(producteur, topic):
    try:
        while True:
            transaction = generateur_transaction()
            future = producteur.send(topic, value=transaction)

            try:
                future.get(timeout=5) 
                print(f"Transaction envoyée : {transaction}")
            except Exception as e:
                print(f"Erreur d'envoi : {e}")

            sleep(1.5)
    except KeyboardInterrupt:
        print("Arrêt demandé par l'utilisateur.")
    finally:
        producteur.close()
        print("Producteur Kafka fermé.")


        
print("Création du producteur : ")
producteur_ = gen_producteur()
print("Début de l'envoie de donnée à Kafka : \n")
send_transactions(producteur = producteur_,
                    topic = 'transactions')
