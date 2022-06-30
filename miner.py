from pandas import concat
import pika, sys, os
import threading
import random
import string
from hashlib import sha1
import time

global arquivo 
arquivo = 'banco-de-dados.csv'

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
        print(election)
        if(len(election) == qtd_usuarios):
            chairman = sum(election)%qtd_usuarios
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost'))
    channel = connection.channel()

    # Verifica se a lista esta completa
    channel.queue_declare(queue = 'ppd/WRoom')  # assina/publica
    channel.queue_declare(queue = 'ppd/election')  # assina/publica
    
    channel.basic_publish(exchange = '', routing_key = 'ppd/WRoom', body = str(id))
    channel.basic_consume(queue = 'ppd/WRoom' , on_message_callback = callback, auto_ack = True)
    
    channel.basic_consume(queue = 'ppd/election' , on_message_callback = callback2, auto_ack = True)
    
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

#def getTransactionID():
#def getChallenge(transactionID):
#def verificaSEED(hash, challenger):
#def submitChallenge(transactionID, ClientID, seed):