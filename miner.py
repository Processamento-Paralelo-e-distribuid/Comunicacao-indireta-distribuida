from numpy import char
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
def setChallenge(challenger):
    try:
        df = pd.read_csv(arquivo)
    except:
        return -1
    
    transactionID = getTransactionID()
    trasition = df.query("TransactionID == "+str(transactionID))    
    
    if(trasition.empty == True):
        return -1
    
    trasition.loc[transactionID,"Challenge"] = challenger
    df.iloc[transactionID,:] = trasition.iloc[0,:]
    
    df.to_csv(arquivo, index=False)
    
def main():
    qtd_usuarios = 3
    chairman = None
    id = time.time()
    cadastro = None
    usuarios, election = [], []
    def callback(ch, method, properties, body):
        temp = body.decode()
        if(len(usuarios)<qtd_usuarios):
            try:
                if(usuarios.index(temp) >= 0):
                    pass
            except:
                usuarios.append(temp)
            channel.basic_publish(exchange = '', routing_key = 'ppd/WRoom', body = temp)
            
            if(len(usuarios)==qtd_usuarios):
                aux = str(id)+"/"+str(random.randint(0,qtd_usuarios-1))
                print(aux)
                channel.basic_publish(exchange = '', routing_key = 'ppd/election', body = aux)
        else:
            try:
                if(usuarios.index(temp) >= 0):
                    channel.basic_publish(exchange = '', routing_key = 'ppd/WRoom', body = temp)
            except:
                pass
            
            try:
                if(usuarios.index(str(id)) >= 0):
                    channel.basic_publish(exchange = '', routing_key = 'ppd/WRoom', body = temp)
            except:
                exit(0)
            
    def callback2(ch, method, properties, body):
        temp = body.decode().split("/")
        if(len(usuarios) == qtd_usuarios):
            try:
                if(usuarios.index(temp[0]) >= 0 and len(election) < qtd_usuarios):
                    try:
                        if(election.index("/".join(temp))>= 0):
                            pass
                    except:
                        election.append("/".join(temp))
                    
                    if(len(election) == qtd_usuarios):
                        chairman = 0
                        for voto in election:
                            chairman = chairman + int(voto[1])
                        chairman = chairman%qtd_usuarios
                        print(chairman)
                        # verifica se o proprio usuario é o prefeito e publica o challenger gerado
                        if(usuarios[chairman] == str(id)):
                            trasactionID    = getTransactionID() # Cria a transação
                            challenger      = getChallenge(trasactionID)
                            channel.basic_publish(exchange = '', routing_key = 'ppd/challenge', body = str(challenger))
                    channel.basic_publish(exchange = '', routing_key = 'ppd/election', body = "/".join(temp))
                else:
                    channel.basic_publish(exchange = '', routing_key = 'ppd/election', body = "/".join(temp))
            except:
                pass
        else:
            channel.basic_publish(exchange = '', routing_key = 'ppd/election', body = "/".join(temp))
            
    def callback3(ch, method, properties, body):
        challenger = int(body.decode()) # Pega challenger anunciado
        setChallenge(challenger)
        
        seed = []

        # Buscar, localmente, uma seed (semente) que solucione o desafio proposto
        flag = True

        def verificaSEED(hash, challenger):
            for i in range(0,40):
                ini_string = hash[i]
                scale = 16
                res = bin(int(ini_string, scale)).zfill(4)
                res = str(res)
                for k in range(len(res)):
                    if(res[k] == "b"):
                        res = "0"*(4-len(res[k+1:]))+res[k+1:]
                        break

                for j in range (0, 4):
                    if(challenger == 0):
                        if(res[j] != "0"):
                            return 1
                        else:
                            return -1
                    if(res[j] == "0"):
                        challenger = challenger - 1
                    else:
                        return -1
            return -1

        def random_generator(size=6, n=1, chars=string.printable): # Gera string aleatória
            random.seed(n)
            return ''.join(random.choice(chars) for _ in range(size))

        def getSeed(challenger, seed, size): # Gera seed
            n = 0
            while(flag):
                seedTemp = random_generator(size, n)
                texto = str(seedTemp).encode('utf-8')
                hash = sha1(texto).hexdigest()
                
                if(verificaSEED(hash, challenger) == 1):
                    seed.append(seedTemp)
                    break
                n = n + 1

        multThread = []

        for i in range(1,challenger*2+1):
            thread = threading.Thread(target=getSeed, args=(challenger, seed, i, ))
            multThread.append(thread)
            thread.start()
            
            if(len(seed) > 0):
                flag = False
                break   

        while(True):
            if(len(seed) != 0):
                break

        flag = False

        # Verifica se todas as threads acabaram 
        for thread in multThread:
            thread.join()
            
        #enviar resposta para server
        cod_seed = str(id)+'/'+str(seed[0])
        channel.basic_publish(exchange = '', routing_key = 'ppd/seed', body = cod_seed)
        
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost'))
    channel = connection.channel()

    # Verifica se a lista esta completa
    channel.queue_declare(queue = 'ppd/WRoom')      # assina/publica - Sala de Espera
    channel.queue_declare(queue = 'ppd/election')   # assina/publica - Eleção do presidente
    channel.queue_declare(queue = 'ppd/challenge')  # assina/publica
    channel.queue_declare(queue = 'ppd/seed')       # assina/publica
    
    channel.queue_declare(queue = 'ppd/result')     # assina
    
    # Sala de Espera
    print(id)
    channel.basic_consume(queue = 'ppd/WRoom' , on_message_callback = callback, auto_ack = True)
    channel.basic_publish(exchange = '', routing_key = 'ppd/WRoom', body = str(id))
    
    # Eleição
    channel.basic_consume(queue = 'ppd/election' , on_message_callback = callback2, auto_ack = True)
    
    # Challenger
    #channel.basic_consume(queue = 'ppd/challenge' , on_message_callback = callback3, auto_ack = True)
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