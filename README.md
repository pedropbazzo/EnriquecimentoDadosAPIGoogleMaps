## Problema


Há um arquivo de texto ("data_points.txt") em anexo à este repositório, contendo uma lista de coordenadas geográficas obtidas a partir do GPS de um dispositivo móvel. Necessita-se que esse arquivo seja processado e enriquecido com informações disponíveis na internet. O objetivo do enriquecimento é descobrir os endereços correspondentes às coordenadas. Para essa finalidade deve ser utilizada alguma API aberta, como por exemplo: _Google Maps_.

## Requisitos

1. A solução deve ser implementada em Python;
2. A solução como um todo deve poder ser executada em ambiente Linux;
3. Todas as bibliotecas, produtos ou serviços utilizados devem ser open source;
4. As configurações de conexão com banco de dados devem ser parametrizadas;
5. Caso necessário, um script DDL deve ser fornecido e devidamente documentado;

## Insumos

Os dados necessários para o desenvolvimento da solução estão disponíveis no arquivo "data_points.txt"

O arquivo consiste de uma série de coordenadas geográficas compostas por:

- Latitude
- Longitude
- Distância

**OBS: Em todas as linhas, os três atributos são apresentados em dois formatos:**

- Em graus, minutos e segundos
- Em número decimal

Exemplo:

> Latitude: 30°02′59″S   -30.04982864  
> Longitude: 51°12′05″W   -51.20150245  
> Distance: 2.2959 km  Bearing: 137.352°  

## Resultados

O dataframe final deve ser salvo em um banco de dados relacional e precisa conter as seguintes informações:

latitude|longitude|rua|numero|bairro|cidade|cep|estado|pais|endereço completo
--------|---------|---|------|------|------|---|------|----|-----------------

# Lógica da solução desenvolvida

O desenvolvimento da rotina visa buscar o conteúdo de um arquivo de entrada, com o nome padrão esperado de "data_points.txt", encontrando suas coordenada geográficas (latitude e longitude). Para cada coordenada de entrada, busca enriquecer uma base de dados de endereços através de consultas na API do Google Maps. Na rotina, foram tratados eventos de falhas na aquisição e decodificação do arquivo JSON que é retornado pela API, assim como há uma checagem para cada uma das coordenadas (latitude/longitude) indicadas no arquivo se estas já existem ou não no banco de dados; se existem, não os insere para não gerar duplicidade. Se não encontrar um correspondente no banco de dados, insere o novo registro na tabela. As chaves para essa verificação são: latitude e longitude.

## Requerimentos de ambiente

- S.O.: Linux
- Interpretador Python 3.6 ou versão mais recente.
- SGBD relacional da sua preferência (os testes de execuções foram efetuados em um banco de dados MySQL).
- Acesso ao SGBD com permissões para criar objetos DDL.
- Ter uma conta no Google, onde deverá ser ativado o aplicativo Google Maps para utilização da API. A API do Google servirá como fonte para requisições de consulta a geolocalizções. Link: https://console.developers.google.com

## Instalação

1. - Abrir o terminal de linha de comando do Linux
2. - Acessar seu "home": cd /home/your_username/
3. - Criar a pasta "Projetos": mkdir /home/your_username/Projetos/
4. - Acessar a pasta recém criada: cd /home/your_username/Projetos/
5. - Clonar o projeto "Enriquecimento de Dados" deste repositório: git clone https://github.com/daniellj/EnriquecimentoDadosAPIGoogle
6. - Acessar a pasta criada pelo Git clone: cd /home/your_username/Projetos/EnriquecimentoDadosAPIGoogle
7. - Executar o script DDL no SGBD para criação dos objetos de banco de dados. O script está alocado em: "/home/your_username/Projetos/EnriquecimentoDadosAPIGoogle/bin/CreateObjectsGeolocalizacao.sql"
8. - Instalar as dependências do projeto (Python Libraries): via terminal do Linux, acessar a pasta onde se encontra o arquivo "requirements.txt" (cd /home/your_username/Projetos/EnriquecimentoDadosAPIGoogle/). Após, digite/execute o comando à seguir: pip install -r requirements.txt
9. - Abrir o script "EnriquecimentoDados-APIGoogle.py", alocado em "/home/your_username/Projetos/EnriquecimentoDadosAPIGoogle/bin/", com um editor de texto da sua preferência (indico o Notepad++). Após, editar os valores das seguintes variáveis (LINHA - indica a linha que está a posição da variável para edição!):

#### Environment Config. - ajustar esses valores!
LINHA 25 - filepath = '/home/your_username/Projetos/EnriquecimentoDadosAPIGoogle/data/' # folder where the "data_points.txt" file is located<br />
LINHA 26 - APIKEYGOOGLEMAPS = '' # your api key (Google Maps)<br />
LINHA 28 - DB_HOST = '' # SGBD HostName or IP_Address<br />
LINHA 29 - DB_PORT = '' # SGBD default Port Number

10. - Salve as alterações do script ("EnriquecimentoDados-APIGoogle.py").

Projeto implantado com sucesso.

## Execução

1. - Acessar a pasta onde se encontra o script .PY: cd /home/your_username/Projetos/EnriquecimentoDadosAPIGoogle/bin/
2. - Dar permissão de execução ao script "EnriquecimentoDados-APIGoogle.py": chmod +x EnriquecimentoDados-APIGoogle.py
3. - Executar o script "EnriquecimentoDados-APIGoogle.py" com o interpretador Python via linha de comando no terminal do Linux: python EnriquecimentoDados-APIGoogle.py

Após a execução e finalização do processo de integração, aparacerá uma mensagem em tela: **"Processo de carga finalizado com sucesso!"**. Após isso, acesse o banco de dados e execute a query "SELECT * FROM geolocalizacao.geolocalizacao order by latitude, longitude". Espera-se que ocorram cargas incrementais e nunca tenham registros repetidos, por mais que se repita "N" vezes o processo com o mesmo arquivo da carga inicial ("data_points.txt").
