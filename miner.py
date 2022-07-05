import pandas as pd
import pika, sys, os
import threading
import random
import string
from hashlib import sha1
import time

global arquivo 
arquivo = 'banco-de-dados.csv'

def getTransactionID():
    try:
        df = pd.read_csv(arquivo)
    except:
        df = None
    transactionID = 0
        
    if(df is None):
        lista = {"TransactionID":[0], "Challenge":[random.randint(1,5)], "Seed":[" "], "Winner": [-1]}
        df = pd.DataFrame(lista)
    else:
        tam = len(df.iloc[:, 0])
        if(df.iloc[tam-1, 3]):
            return df.iloc[tam-1, 0]
        else:
            transactionID = df.iloc[(tam-1), 0]+1
            lista = {"TransactionID":[0], "Challenge":[random.randint(1,5)], "Seed":[" "], "Winner": [-1]}
            transaction = pd.DataFrame(lista)

            df = pd.concat([df,transaction], ignore_index = True)
    
    df.to_csv(arquivo, index=False)
    
    return int(transactionID)

def getChallenge(transactionID):
    try:
        df = pd.read_csv(arquivo)
    except:
        return -1
    transaction = df.query("TransactionID ==" + str(transactionID))

    if(transaction.empty==False):
        return transaction["Challenge"].values[0]
    else:
        return -1
def setChallenge(transictionID, challenger):
    
    return
def main():
    qtd_usuarios = 2
    chairman = None
    usuarios, election = [], []
    id = time.time()
    def callback(ch, method, properties, body):
        if(len(usuarios) != qtd_usuarios):
            usuarios.append(body.decode())
            
            #Sala completa
            if(len(usuarios) == qtd_usuarios):
                channel.basic_publish(exchange = '', routing_key = 'ppd/election', body = str(random.randint(0,9)))
                
    def callback2(ch, method, properties, body):
        if(len(election) != qtd_usuarios):
            election.append(int(body.decode()))
        if(len(election) == qtd_usuarios):
            chairman = sum(election)%qtd_usuarios
            # verifica se o proprio usuario é o prefeito e publica o challenger gerado
            if(usuarios[chairman] == id):
                trasactionID    = getTransactionID() # Cria a transação
                challenger      = getChallenge(trasactionID)
                channel.basic_publish(exchange = '', routing_key = 'ppd/challenge', body = str(challenger))
                
    def callback3(ch, method, properties, body):
        challenger      = int(body.decode()) # Pega challenger anunciado 
        trasactionID    = getTransactionID() # Cria a transação
        setChallenge(trasactionID, challenger)
        
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost'))
    channel = connection.channel()

    # Verifica se a lista esta completa
    channel.queue_declare(queue = 'ppd/WRoom')      # assina/publica - Sala de Espera
    channel.queue_declare(queue = 'ppd/election')   # assina/publica - Eleção do presidente
    channel.queue_declare(queue = 'ppd/challenge')  # assina/publica
    
    #channel.queue_declare(queue = 'ppd/seed')       #publica
    #channel.queue_declare(queue = 'ppd/result')     #assina
    
    # Fila de Espera
    channel.basic_publish(exchange = '', routing_key = 'ppd/WRoom', body = str(id))
    channel.basic_consume(queue = 'ppd/WRoom' , on_message_callback = callback, auto_ack = True)
    
    # Eleição
    channel.basic_consume(queue = 'ppd/election' , on_message_callback = callback2, auto_ack = True)
    
    # Challenger
    channel.basic_consume(queue = 'ppd/election' , on_message_callback = callback3, auto_ack = True)
        
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

#def verificaSEED(hash, challenger):
#def submitChallenge(transactionID, ClientID, seed):