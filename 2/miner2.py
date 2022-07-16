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
        lista = {"TransactionID":[0], "Challenge":[random.randint(1,7)], "Seed":[" "], "Winner": [-1]}
        df = pd.DataFrame(lista)
    else:
        tam = len(df.iloc[:, 0])
        if(df.iloc[tam-1, 3] == -1):
            return df.iloc[tam-1, 0]
        else:
            transactionID = df.iloc[(tam-1), 0]+1
            lista = {"TransactionID":transactionID, "Challenge":[random.randint(1,7)], "Seed":[" "], "Winner": [-1]}
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

def main():
    qtd_usuarios = 2
    id = time.time()
    
    usuarios, eleitos, votacao = [], [], []
    
    numero = "2"
    
    def callback(ch, method, properties, body):
        if(len(usuarios) != qtd_usuarios):
            usuarios.append(body.decode())

            #Sala completa
            if(len(usuarios) == qtd_usuarios):
                channel.basic_publish(exchange = 'Election', routing_key = '', body = str(random.randint(0,qtd_usuarios-1)))
                print(usuarios)

    def callback2(ch, method, properties, body):
        if(len(eleitos) != qtd_usuarios):
            eleitos.append(int(body.decode()))
        if(len(eleitos) == qtd_usuarios):
            chairman = sum(eleitos)%qtd_usuarios
            print(chairman)
            # verifica se o proprio usuario é o prefeito e publica o challenger gerado
            if(usuarios[chairman] == str(id)):
                trasactionID    = getTransactionID() # Cria a transação
                challenger      = getChallenge(trasactionID)
                channel.basic_publish(exchange = 'Challenge', routing_key = '', body = str(challenger)+'/'+str(trasactionID))
            eleitos.clear()
                
    def callback3(ch, method, properties, body):
        def setChallenge(challenger):
            transactionID = getTransactionID()
            try:
                df = pd.read_csv(arquivo)
            except:
                return -1
            
            trasition = df.query("TransactionID == "+str(transactionID))    
            
            if(trasition.empty == True):
                return -1
            
            trasition.loc[transactionID,"Challenge"] = challenger
            df.iloc[transactionID,:] = trasition.iloc[0,:]
            
            df.to_csv(arquivo, index=False)
        
        temp = body.decode().split("/")
        challenger      = int(temp[0]) # Pega challenger anunciado
        trasictionID    = int(temp[1])
        setChallenge(challenger)

        seed = []

        # Buscar, localmente, uma seed (semente) que solucione o desafio proposto
        flag = True

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
        cod_seed = str(id)+'/'+str(seed[0]+'/'+str(trasictionID))
        print(cod_seed)
        channel.basic_publish(exchange = 'Seed', routing_key = '', body = cod_seed)
        
    def callback4(ch, method, properties, body):
        def submitChallenge(seed):
            try:
                df = pd.read_csv(arquivo)
            except:
                return -1
            
            transactionID = getTransactionID() 
            trasition = df.query("TransactionID == "+str(transactionID))    
            
            if(trasition.empty == True):
                return -1
            elif(trasition["Winner"].values != -1):
                return 0

            texto = str(seed).encode('utf-8')
            hash = sha1(texto).hexdigest()
            challenge = trasition["Challenge"].values[0]

            if(verificaSEED(hash, challenge) == 1):
                return 1
            else:
                return 0
        
        lista = body.decode().split("/")
        
        try:
            df = pd.read_csv(arquivo)
        except:
            return -1 
        
        aux = df.query("TransactionID ==" + lista[2])
        if(aux["Winner"].values[0] == -1):
            result = False                       # Erro, não resolve desafio ou desafio solucionado
            if(submitChallenge(lista[1]) == 1):  # Resolve desafio
                result = True
            channel.basic_publish(exchange = 'Result', routing_key = '', body = body.decode()+"/"+str(result))
        
    def callback5(ch, method, properties, body):
        def verificaVotacao(votacao):
            count = 0
            for voto in votacao:
                if(voto[2]):
                    count = count + 1
            if(count >= qtd_usuarios/2):
                return 1
            return 0
            
        if(len(votacao) != qtd_usuarios):
            votacao.append(body.decode().split("/"))
        if(len(votacao) == qtd_usuarios):
            print(votacao)
            chairman = 0
            if(verificaVotacao(votacao)):
                try:
                    df = pd.read_csv(arquivo)
                except:
                    return -1 
                
                aux = df.query("TransactionID ==" + votacao[0][2])
                if(aux["Winner"].values[0] == -1):
                    transactionID = getTransactionID()
                    
                    trasition = df.query("TransactionID == "+str(transactionID))  
                    
                    trasition.loc[transactionID,"Seed"]   = votacao[0][1]
                    trasition.loc[transactionID,"Winner"] = float(votacao[0][0])
                    
                    df.iloc[transactionID,:] = trasition.iloc[0,:]
                    
                    df.to_csv(arquivo, index=False)
                    chairman = usuarios.index(votacao[0][0])
                else:
                    return
            
            print(chairman)
            channel.queue_purge('ppd/result/'+numero)
            if(usuarios[chairman] == str(id)):
                trasactionID    = getTransactionID()
                challenger      = getChallenge(trasactionID)
                channel.basic_publish(exchange = 'Challenge', routing_key = '', body = str(challenger)+'/'+str(trasactionID))
            
            votacao.clear()
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost'))
    channel = connection.channel()

    print(id)

    # Verifica se a lista esta completa
    channel.exchange_declare(exchange='WRoom', exchange_type='fanout')
    room = channel.queue_declare(queue = 'ppd/WRoom/'+numero)      # assina/publica - Sala de Espera
    channel.queue_bind(exchange='WRoom', queue=room.method.queue)

    channel.exchange_declare(exchange='Election', exchange_type='fanout')
    election = channel.queue_declare(queue = 'ppd/election/'+numero)      # assina/publica - Eleção do presidente
    channel.queue_bind(exchange='Election', queue=election.method.queue)

    channel.exchange_declare(exchange='Challenge', exchange_type='fanout')
    challenge = channel.queue_declare(queue = 'ppd/challenge/'+numero)     # assina/publica - Desafio da transição atual
    channel.queue_bind(exchange='Challenge', queue=challenge.method.queue)

    channel.exchange_declare(exchange='Seed', exchange_type='fanout')
    seed = channel.queue_declare(queue = 'ppd/seed/'+numero)     # assina/publica - Verificação da seed que resolve desafio
    channel.queue_bind(exchange='Seed', queue=seed.method.queue)

    channel.exchange_declare(exchange='Result', exchange_type='fanout')
    result = channel.queue_declare(queue = 'ppd/result/'+numero)     # assina/publica - Lista de votação na seed que soluciona o desafio
    channel.queue_bind(exchange='Result', queue=result.method.queue)
    
    # Fila de Espera
    channel.basic_publish(exchange = 'WRoom', routing_key = '', body = str(id))
    channel.basic_consume(queue = 'ppd/WRoom/'+numero , on_message_callback = callback, auto_ack = True)
    
    # Eleição
    channel.basic_consume(queue = 'ppd/election/'+numero , on_message_callback = callback2, auto_ack = True)
    
    # Challenger
    channel.basic_consume(queue = 'ppd/challenge/'+numero , on_message_callback = callback3, auto_ack = True)
    
    # Seed
    channel.basic_consume(queue = 'ppd/seed/'+numero , on_message_callback = callback4, auto_ack = True)
    
    # Resultado
    channel.basic_consume(queue = 'ppd/result/'+numero , on_message_callback = callback5, auto_ack = True)
    
    
    channel.start_consuming()

if __name__ == '__main__':
    try:
        file = 'banco-de-dados.csv'
        if(os.path.exists(file) and os.path.isfile(file)): 
            os.remove(file)     
        main()
    except KeyboardInterrupt:
        print('Interrupted')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

#def submitChallenge(transactionID, ClientID, seed):