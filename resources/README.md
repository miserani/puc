## Intalação dos recursos de infra estrutura para aula

### Guia de Uso

#### Máquina Virtua - Azure
##### Criação da Máquina Virtual:

Acessar a VM via ssh:
    Para acessar a VM via ssh, execute o comando abaixo no terminal:
    - Depois de ter baixado a chave privada, execute o comando no terminal: ```chmod 600 {my_key}.pem```
    - O comando ssh: ```ssh -i {my_key}.pem {user}@{ip_publico}```


##### Como utilizar o Makefile

Depois de acessar a VM com o comando ssh, vamos configurar a máquina para rodar os serviços necessários para a aula. Para
isso, vamos executar os comando abaixo no terminal da VM:
```bash
    1. sudo apt-get update
    2. git clone https://github.com/miserani/puc.git
    3. cd puc/
    4. git fetch
    5. git checkout -f puc-pre-2023
    6. git pull origin puc-pre-2023 
    7. cd puc-pre-2023/
    8. cd recursos/
    9. sudo apt install make
    10. Acessar a pasta puc/puc-pre-2023/recursos/ e executar o comando ```make docker-install```
    11. sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    12. sudo chmod +x /usr/local/bin/docker-compose
    13. docker-compose --version
```

Para acessar o Makefile, deve navegar até a pasta ```puc/puc-pre-2023/recursos/``` e executar o comando ```make help```.
Este Makefile fornece comandos para gerenciar e instalar várias tecnologias. Abaixo estão as instruções de uso para cada seção.

##### Docker

- Instalação do Docker e Docker Compose no Ubuntu: ```make docker-install```
- Construir imagem do Postgres com PostGIS: ```make docker-postgres-build```  
- Iniciar o serviço Postgres: ```make docker-postgres-up```   
- Parar o serviço Postgres: ```make docker-postgres-down```   
- Popular o banco de dados Postgres com eventos históricos: ```make docker-postgres-populate```
- Iniciar o serviço Kafka (defina o IP conforme necessário): ```make docker-kafka-up IP=seu_endereco_ip```    
- Parar o serviço Kafka: ```make docker-kafka-down``` 
- Ajuda para comandos do Dockr: ```make help-docker```    

MongoDB Community Edition

- Instalação no Ubuntu: ```make mongodb-install-ubuntu```
- Instalação no macOS: ```make mongodb-install-mac```
- Iniciar o serviço MongoDB: ```make mongodb-run```
- Parar o serviço MongoDB: ```make mongodb-stop```
- Desinstalação no Ubuntu: ```make mongodb-uninstall-ubuntu```
- Desinstalação no macOS: ```make mongodb-uninstall-mac```
- Ajuda para comandos do MongoDB: ```make help-mongodb```


JupyterLab com Spark, Pandas, Kafka e outras bibliotecas

- Construir imagem Docker para o JupyterLab: ```make jupyter-build```
- Iniciar o JupyterLab: ```make jupyter-up```
- Parar o JupyterLab: ```make jupyter-down```
- Ajuda para comandos do JupyterLab: ```make help-jupyter```

Start Events API

- Iniciar o serviço de eventos: ```make api-start```
- Finalizar o serviço de eventos: ```make api-end```
- Ajuda para comandos da API: ```make help-api-events```

Para ver uma lista de todas as tecnologias disponíveis e seus comandos associados: ```make help```

##### Como usar o script de eventos credit-events.py

Configuração do Ambiente:
- Certifique-se de ter todas as bibliotecas necessárias instaladas. Você pode fazer isso usando ```pip install -r requirements.txt```. 
Dentro da pasta ```puc/puc-pre-2023/recursos/```.
- Configure as variáveis de ambiente para os URIs do MongoDB, PostgreSQL e Kafka, se necessário.

Execução:
- Deve acessar a pasta ```puc/puc-pre-2023/recursos/``` e executar o comando ```python3 -m credit-events```. 
- Use o argumento ```--db``` para especificar o tipo de banco de dados. As opções são "mongo", "postgres", "kafka" e "API".
  - Ex.: ```python3 -m credit-events --db mongo```
- Use o argumento ```--threads``` para especificar o número de threads.
  - Ex.: ```python3 -m credit-events --db postgres --threads 5```
- Use o argumento ```--records``` para especificar o número de registros por thread.
  - Ex.: ```python3 -m credit-events --db kafka --threads 5 --records 1000```
- Use o argumento ```--msgs_per_second``` para especificar o número de mensagens por segundo para Kafka (se estiver usando Kafka).
  - Ex.: ```python3 -m credit-events --db kafka --threads 5 --records 1000 --msgs_per_second 100```

##### Como subir a API de enventos

Acessar a pasta ```puc/puc-pre-2023/recursos/``` e executar o comando ```make api-start IP=endereco_VM_privado ou publico```
Se for rodar o script na máquina local, ip publico da VM, caso executar na própria VM e utilizar o ip privado.

##### Como subir o banco de dados postgres e popular com eventos históricos
Acessar a pasta ```puc/puc-pre-2023/recursos/``` e executar o comando ```make docker-postgres-build```, caso ainda
não tenha realizado o _build_ da imagem do postgres. Depois executar o comando ```make docker-postgres-up``` para subir o
banco de dados. Depois executar o comando ```make docker-postgres-populate``` para popular o banco de dados com os eventos. 
Para acessar os dados, utilizar o notebook ```puc/puc-pre-2023/notebooks/postgres_read_events.ipynb```

##### Como subir o banco de dados mongo e popular com eventos históricos
Acessar a pasta ```puc/puc-pre-2023/recursos/``` e executar o comando ```make mongodb-run```, caso ainda
não tenha realizado o _build_ da imagem do mongo. Depois executar o comando ```make mongodb-run``` para subir o
banco de dados. Depois executar o comando ```python3 -m credit-events --db mongo``` para popular o banco de dados com os eventos.


##### Acessar o jupyterLab
Acessar via browser o endereço ```http://endereco_VM_publico:8888```

##### Acessar o Kowl
Acessar via browser o endereço ```http://endereco_VM_publico:7777```

##### Acessar API
Acessar via browser o endereço ```http://endereco_VM_publico:8000/events?num_records=100```

#### Referências
https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#creating-a-virtual-environment