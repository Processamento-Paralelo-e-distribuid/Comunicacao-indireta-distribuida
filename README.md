# Comunicacao-indireta-distribuida

## Objetivos
- Experimentar a implementação de sistemas de comunicação indireta por meio de
middleware Publish/Subscribe (Pub/Sub) com Filas de Mensagens;
- Realizar eleição em sistemas distribuídos por meio da troca de mensagens entre os
participantes do sistema;
- Realizar votação sobre o estado de transações distribuídas por meio da troca de mensagens
entre os participantes do sistema.

## Desenvolvimento

O código foi elaborado em Python e separado em 3 seguimentos:
- Data center local
- Votação
- Eleição

### Data center local
Como existe uma comunicação indireta entre os participantes, a uma necessidade de se armazenar todas as informações referentes ao andamento das transações. Contudo, cada usuário armazena de forma local uma tabela contendo informações de transações atuais e transações que já se foram finalizadas, nesta tabela temos como informações o ID da transação, o challenger associado a tal transação, a seed que soluciona o desafio caso já tenha sido solucionado e o id do usuário que soluciona o desafio.

obs: o ID do usuário foi implementado de tal forma que seja o timestap do exato momento em que o usuário se candidata a entrar no sistema.

### Votação
A votação está relacionada a dois momentos, no primeiro momento e preciso ser decidido por todos os usuários do sistema, qual usuário entre eles que sera eleito e sera responsável por divulgar o primeiro challenger para se iniciar o sistema de transações, esse momento sera melhor explicado posteriormente. 
  Já o segundo momento se dá, no instante em que um usuário envia uma seed que possivelmente soluciona o desafio, no exato minuto que um usuário envia a seed, o sistema entra em votação em que todos os usuários recebem a seed e fazem uma verificação local para checar ser a seed enviada realmente soluciona o desafio, após esta verificação o usuario envia True caso ele concorde com a seed e false caso contrario para uma fila de votação, onde nele todos os votos chegaram e caso a maioria disser que a seed é valida o usuário ao qual a seed foi enviada e dito como vencedor e se torna o novo eleito, responsável por enviar a próxima seed.  

### Eleição
A eleição e um passo primordial no código, cada usuário envia seu id se cadastrando no sistema, posteriormente quando o último usuário se cadastra se inicia a votação para se saber quem sera o primeiro eleito a divulgar o challenger para todos os outros usuários, esta eleição e feita com cada usuário enviando para uma fila de eleição com o seu ID um número ente 0 e n-1, sendo n a quantidade de usuários do sistema, e armazenando em seu próprio código a lista de números gerados pelos outros usuários, quando a lista bate a quantidade de usuários n e o id de cada envio corresponder a um usuário valido não repetido, o usuário de forma local soma todos os números e o divide por n de for a obter um índice de 0 à n-1 correspondendo ao índice do id da lista de usuários, contudo caso o usuário veja que o id na tabela de usuários correspondente ou índice encontrado for o dele ele automaticamente entende que se tornou o eleito, gerando e enviando o challenger da próxima votação.

## Autores
| [<img src="https://avatars.githubusercontent.com/u/56831082?v=4" width=115><br><sub>Arthur Coelho Estevão</sub>](https://github.com/arthurcoelho442) |  [<img src="https://avatars.githubusercontent.com/u/56406192?v=4" width=115><br><sub>Milena da Silva Mantovanelli</sub>](https://github.com/Milena0899) |  [<img src="https://avatars.githubusercontent.com/u/53350761?v=4" width=115><br><sub>Mayke Wallace</sub>](https://github.com/Nitrox0Af) |
| :---: | :---: | :---: |
