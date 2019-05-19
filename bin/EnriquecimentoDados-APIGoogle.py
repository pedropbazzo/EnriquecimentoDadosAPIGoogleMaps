#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#from conexao_bd import string_bd
from io import open
from os import listdir, system, remove
from os.path import isfile, join
import time
import requests
import json
import pandas as pd
import threading
from sqlalchemy import create_engine

### Default Values - não modificar esses valores!
filename = "data_points"
extension = "txt"
TABLENAME = 'geolocalizacao'
DB_USER = 'geolocalizacao'
DB_PASS = 'Geolx!177'
DB_NAME = 'geolocalizacao'
compare=['latitude', 'longitude']

### Environment Config. - ajustar esses valores!
filepath = '/home/your_username/Projetos/EnriquecimentoDadosAPIGoogle/data/' # folder where the "data_points.txt" file is located
APIKEYGOOGLEMAPS = '' # your api key (Google Maps)
#APIKEYGOOGLEMAPS = 'AIzaSyAgaYMWeeU4aM1MaTVb40uuNBtbZNo8PLQ' # apikey for my personal google account! Needed change this value!
DB_HOST = '' # SGBD HostName or IP_Address
DB_PORT = '' # SGBD default Port Number

def files_to_process(path, name, ext):
    '''
    Procura em uma determinada pasta do S.O. por arquivos com um nome + extensão padrão.
    Retorna uma lista com os arquivos encontrados.
    '''
    files_find = []

    for file in os.listdir(path):
        if (os.path.isfile(os.path.join(path, file))) and (file == name + '.' + ext):
            files_find.append(files)
    return (files_find)

def complete_getdate():
    '''
    Função que retorna o getdate() em formato string, com a máscara YYYYMMDD_HHMMSS.
    '''
    time_str = time.strftime("%Y%m%d_%H%M%S")
    return (str(time_str))

def create_coordenate_lists(filepath_source, filename_source, ext_source):
    '''
    Input: necessita de valores de entrada:
        - filepath_source = pasta onde se localiza o arquivo de entrada.
        - filename_source = nome do arquivo à ser processado.
        - ext_source = extensão do arquivo.
    Output: Retorna uma tupla com duas listas: latitude e longitude.
    '''
    file_source = filepath_source + filename_source + '.' + ext_source
    file_temp = filepath_source + filename_source + '_' + complete_getdate() + '.tmp'
    # print(file_source)
    # print(file_temp)

    try:
        # abrindo os arquivos de origem e temporário
        file_src = open(file_source, 'r')
        file_tmp = open(file_temp, 'w+')

        # escrevendo o conteúdo do arquivo de origem no temporário
        for line_src in file_src.readlines():
            file_tmp.write(line_src)

        # retornando a linha inicial do arquivo de origem
        file_tmp.seek(0, 0)

        # gerando uma lista de elemtos à partir de valores do arquivo temporário
        lista_inicial = file_tmp.readlines()

        # criando uma lista para cada uma das coordenadas: Latitude e Longitude
        tamanho = len(lista_inicial)
        posicao = 0
        lista_latitude = []
        lista_longitude = []

        for item in lista_inicial:
            if posicao <= tamanho:
                if 'Latitude' in item:
                    latitude = (item[23:len(item)])
                    lista_latitude.append(latitude.replace('\n', ''))
                if 'Longitude' in item:
                    longitude = (item[24:len(item)])
                    lista_longitude.append(longitude.replace('\n', ''))
            posicao = posicao + 1

    finally:
        print('Fechando o arquivo de origem', file_source)
        file_src.closed
        print('Fechando e excluindo o arquivo temporário', file_temp)
        file_tmp.closed
        remove(file_temp)
    return (lista_latitude, lista_longitude)

def recursive_geocode_googlemaps(latitude, longitude):
    '''
    Função que recebe como parâmetro a localização geográfica no formato de geolocalização (latitude e longitude),
    e retorna as informações básicas de um endereço em um DataFrame Pandas:
    - latitude
    - longitude
    - rua
    - numero
    - bairro
    - cidade
    - cep
    - estado
    - pais
    - enderecocompleto
    '''
    sensor = 'true'
    result_type = 'street_address|street_number|country|administrative_area_level_1|administrative_area_level_2|postal_code' # 'street_address|street_number|country|administrative_area_level_1|administrative_area_level_2|postal_code'
    language = 'pt' #en = inglês
    location_type='ROOFTOP'
    apikey = APIKEYGOOGLEMAPS

    base = "https://maps.googleapis.com/maps/api/geocode/json?"
    params = "latlng={lat},{lon}&sensor={sen}&result_type={result_type}&language={language}&location_type={location_type}".format(
        lat=latitude,
        lon=longitude,
        sen=sensor,
        result_type=result_type,
        language=language,
        location_type=location_type
    )
    key = "&key={apikey}".format(apikey=apikey)
    url = "{base}{params}{key}".format(base=base, params=params, key=key)
    
    #entregando a resposta da consulta para um objeto response
    response = requests.get(url)
    
    try:
        #transformando o conteúdo JSON para um objeto STRING(DICT) em Python
        data_str = str(response.json()).replace("'",'"')
    
        resp_dict = json.loads(data_str)

        #Buscando o resultado da consulta à API do Google Maps
        status = resp_dict['status']

        #Se o retorno da consulta for "OK", prossegue com o tratamento, senão encerra
        #if ("ZERO_RESULTS" in status or "OVER_QUERY_LIMIT" in status or "REQUEST_DENIED" in status or "INVALID_REQUEST" in status or "UNKNOWN_ERROR" in status):
            #print('Coordenada com problema. Latitude:', '|', latitude, ' Longitude:', longitude, '|', ' Status:', status)
        if ("OK" in status):
            #processo para inserir os valores coletados em um DataFrame Pandas...
            list_values = []
            cep_default = ''
            list_values.append(latitude) #latitude
            list_values.append(longitude) #longitude

            for item in resp_dict['results'][0]['address_components']:
                if 'route' in item['types']: #rua
                    list_values.append(item['long_name'])
                if 'street_number' in item['types']: #numero
                    list_values.append(item['long_name'])
                if 'sublocality_level_1' in item['types']: #bairro
                    list_values.append(item['long_name'])
                if 'administrative_area_level_2' in item['types']: #cidade
                    list_values.append(item['long_name'])
            if 'postal_code' not in item['types']: #cep
                list_values.append(cep_default)
            else:
                list_values.append(item['long_name'])
            for item in resp_dict['results'][0]['address_components']:
                if 'administrative_area_level_1' in item['types']: #estado
                    list_values.append(item['long_name'])
                if 'country' in item['types']: #pais
                    list_values.append(item['long_name'])
            list_values.append(resp_dict['results'][0]['formatted_address'])
            return(list_values)
        #address_tup = {(key, value) for (key, value) in zip(list_key, list_values)}    
        #transformando a Tupla em um Dicionário, para que possa ser importado no Pandas...
        #address_dict = dict(address_tup)
        #address_df = pd.DataFrame.from_dict([address_dict])

    except json.JSONDecodeError: # JSONDecodeError
        print('JSONDecodeError: decoding JSON has failed (Latitude:', latitude,'|', 'Longituide:', longitude,')')

def dataframe_input():
    '''
    Preparando o dataframe com base nas informações de endereços processadas pela chamada da função create_coordenate_lists().
    '''
    #chamando a função create_coordenate_lists() para ter as duas listas populadas: latitude e longitude.
    tup = create_coordenate_lists(filepath_source = filepath, filename_source = filename, ext_source = extension)
    
    address_values = []
    latitude = []
    longitude = []
    
    columns = ['latitude', 'longitude', 'numero', 'rua', 'bairro', 'cidade', 'estado', 'pais', 'cep', 'enderecocompleto']

    tamanho = len(tup[0])
    
    for item in tup[0]:
        latitude.append(item)
    #print(latitude)
    
    for item in tup[1]:
        longitude.append(item)
    #print(latitude)
    
    df_latitude = pd.DataFrame(latitude)
    df_longitude = pd.DataFrame(longitude)
    frames = [df_latitude, df_longitude]

    df = pd.concat(frames, axis=1)
    list_values = df.values.tolist()

    for item in list_values:
        latitude = float(item[0])
        longitude = float(item[1])
        address_values.append(recursive_geocode_googlemaps(latitude=latitude, longitude=longitude))

    #removendo valores "None"
    address_values = [x for x in address_values if x is not None]
    
    #concatenando os dados com as colunas, para formar o DataFrame
    df_address = (pd.DataFrame(data=address_values, columns=columns)).sort_values(by=compare, ascending=True)
    return(df_address)

def string_connection_bd(host, port, database, user, password):
    ''' SqlAlchemy Config. '''
    DB_TYPE = 'mysql'
    DB_DRIVER = 'pymysql'

    ''' Default Config. '''
    DB_HOST = host
    DB_PORT = port
    DB_NAME = database
    DB_USER = user
    DB_PASS = password
    #CHARSET = 'latin1'
    #CURSORCLASS = 'pymysql.cursors.Cursor'

    ''' SqlAlchemy Config. '''
    connection = '%s+%s://%s:%s@%s:%s/%s' % (DB_TYPE, DB_DRIVER, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
    return (connection)

def dataframe_output(df, engine, tablename, dup_cols=[], filter_continuous_col=None, filter_categorical_col=None):
    """
    Remove linhas de um dataframe que já exista dentro de uma tabela do banco de dados.
    
    Requerimentos:
        df : dataframe inicial que se deseja remover as linhas já existentes em relação ao conteúdo de uma tabela do BD.
        engine: SQLAlchemy engine object (ver documentação).
        tablename: nome da tabela base para checar o conteúdo já existente em relação ao dataframe inicial.
        dup_cols: lista ou tupla de nomes de colunas do dataframe inicial onde os valores serão comparados com os da tabela
                  do BD para remoção dos dados já existentes no dataframe.
    
    Opcional:
        filter_continuous_col: nome das colunas da tabela do BD para dados que sejam continuos, para uso de filtros do
                               tipo BETWEEEN min/max. Aceita dados das seguintes tipagens: : datetime, int, or float.
        filter_categorical_col : nome de colunas categóricas da tabela do BD, para uso de filtros do tipo WHERE =
                                "valor esperado". Aceita valores distintos dentro da avaliação da expressão IN ('valor'),
                                em relação à deteminada coluna de uma tabela do banco de dados.
    Retorno:
        Uma lista única de valores do dataframe onde os valores comparados com os da tabela do BD,
        não tem tem um correspondente. Resumindo, uma lista de valores que ainda não existem na tabela comparada do BD.
    """
    args = 'SELECT %s FROM %s.%s' %(', '.join(['{0}'.format(col) for col in dup_cols]), DB_NAME, tablename)
    args_contin_filter, args_cat_filter = None, None
    if filter_continuous_col is not None:
        if df[filter_continuous_col].dtype == 'datetime64[ns]':
            args_contin_filter = """ "%s" BETWEEN Convert(datetime, '%s') 
                                          AND Convert(datetime, '%s')""" %(filter_continuous_col, 
                              df[filter_continuous_col].min(), df[filter_continuous_col].max())

    if filter_categorical_col is not None:
        args_cat_filter = ' "%s" in(%s)' %(filter_categorical_col, 
                          ', '.join(["'{0}'".format(value) for value in df[filter_categorical_col].unique()]))

    if args_contin_filter and args_cat_filter:
        args += ' Where ' + args_contin_filter + ' AND' + args_cat_filter
    elif args_contin_filter:
        args += ' Where ' + args_contin_filter
    elif args_cat_filter:
        args += ' Where ' + args_cat_filter

    df.drop_duplicates(subset=dup_cols, keep='last', inplace=True) #inplace = descarta duplicadas no local
    
    #busca no banco de dados os registros para comparar com os dados de input...
    df_query_result = (pd.read_sql(con=engine, sql=args, columns=dup_cols)).sort_values(by=dup_cols, ascending=True)

    df = pd.merge(df, df_query_result, how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)
    return(df)

def insert_data_db(df, pool_size, *args, **kargs):
    """
    Extensão do método to_sql() da biblioteca PANDAS do Python, para inserções em tabelas de um BD usando thread.

    Requerimentos: 
        df : dataframe em PANDAS que se deseja inserir linhas em uma tabela de um banco de dados.
        POOL_SIZE : Configuração do parâmetro "max connection pool size" do sqlalchemy .
            Sugestão: setar valor < que a conexão do banco de dados.
    *args:
        Argumentos do método to_sql() para PANDAS.
        Argumentos requeridos de to_SQL():
            con : SqlAlchemy engine
            name : Nome da tabela do banco de dados que se deseja inserir as linhas carregadas pelo dataframe.

        Argumentos opcionais:
            'if_exists' : 'append' ou 'replace'.  Se a tabela já existe, usar a opção 'append'.
            'index' : True ou False. True se você quer escrever os valores dos índices para o banco de dados.

    Créditos para o código usando threading em Python, ao qual fiz adaptações:
        http://techyoubaji.blogspot.com/2015/10/speed-up-pandas-tosql-with.html
    """

    CHUNKSIZE = 500
    INITIAL_CHUNK = 100
    if len(df) > CHUNKSIZE:
        '''
        insere no banco de dados a quantidade de registros iniciais propostas pela variável INITIAL_CHUNK, somente se df for maior que
        o valor proposto pela variável CHUNKSIZE.
        '''
        df.iloc[:INITIAL_CHUNK, :].to_sql(*args, **kargs)
    else:
        '''
        caso a quantidade de registros do df for menor que o valor proposto pela variável CHUNKSIZE, então simplesmente insere no banco
        de dados os registros.
        '''
        df.to_sql(*args, **kargs)

    workers, i, j = [], 0, 1

    '''
    o iterador abaixo verifica quantos blocos de registros (valor de CHUNKSIZE) ele irá inserir por vez usando thread para melhora
    de performance nas operações de insert com grande volume de linhas.
    '''
    if ((df.shape[0] - INITIAL_CHUNK)//CHUNKSIZE)>0:
        for i in range((df.shape[0] - INITIAL_CHUNK)//CHUNKSIZE):
            t = threading.Thread(target=lambda: df.iloc[INITIAL_CHUNK+i*CHUNKSIZE:INITIAL_CHUNK+(i+1)*CHUNKSIZE].to_sql(*args, **kargs))
            t.start()
            workers.append(t)
            df.iloc[INITIAL_CHUNK+(i+1)*CHUNKSIZE:, :].to_sql(*args, **kargs)
            [t.join() for t in workers]
    elif (df.shape[0] - INITIAL_CHUNK)>0:
        '''
        caso a quantidade de blocos de registros (valor de CHUNKSIZE) seja insignificante (menor do que 1), então insere os registros no
        banco de dados a partir da posição INITIAL_CHUNK+i, onde j=1. Ou seja, insere o restante dos registros.
        '''
        df.iloc[INITIAL_CHUNK+j:].to_sql(*args, **kargs)

def main(filepath, filename, extension, DB_HOST, DB_PORT, DB_NAME, TABLENAME, DB_USER, DB_PASS):
    """
    Função que faz a chamada do processo de ETL proposto.
    """

    ###SqlAlchemy Config.###
    POOL_SIZE = 1000

    SQLALCHEMY_DATABASE_URI = string_connection_bd(host = DB_HOST, port = DB_PORT, database = DB_NAME, user = DB_USER, password = DB_PASS)
    ENGINE = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=POOL_SIZE, max_overflow=0)

    #chamando a função recursive_geocode_googlemaps(), onde para cada coordenada geográfica contida no objeto
    #tup (do tiplo TUPLA), é buscado o endereço completo junto a API do Google Maps e, junto a função insert_data_db,
    #é inserido no banco de dados os registros.

    #insert_data_db(df = dataframe_input(), pool_size=POOL_SIZE, con=ENGINE, name=TABLENAME, if_exists='append', index=False)
    
    #inserindo dados comparando se já existe no banco!
    df_input = dataframe_input()
    df_output = dataframe_output( df = df_input, engine=ENGINE, tablename=TABLENAME, dup_cols=compare)
    #print(df_output)

    insert_data_db( df = df_output, pool_size=POOL_SIZE, con=ENGINE, name=TABLENAME, if_exists='append', index=False)
    print('Processo de carga finalizado com sucesso!')

main(filepath = filepath, filename = filename, extension = extension, DB_HOST = DB_HOST, DB_PORT = DB_PORT, DB_NAME = DB_NAME, TABLENAME = TABLENAME, DB_USER = DB_USER, DB_PASS = DB_PASS)
