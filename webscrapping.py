import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time

while True:
    time.sleep(60)


    # URL para fazer o GET e baixar o arquivo Excel
    # URL da página que você deseja acessar
    url = 'https://www.janelaunicaportuaria.org.br/dte/relatorios/frmResultadoExcel.aspx'

    # Definir o cookie ASP.NET_SessionId
    cookies = {
        'ASP.NET_SessionId': 'v0hoebip3b4bgyeelffmvk1k'  # Substitua pelo valor correto
    }

    # Faz a requisição GET com o cookie definido
    response = requests.get(url, cookies=cookies)
    print(response.content)
    # Usa o BeautifulSoup para parsear o HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontra todas as tabelas na página
    tables = soup.find_all('table')

    # Variável para armazenar o HTML filtrado
    filtered_table_html = '<table>'

    # Itera sobre todas as tabelas procurando pela que contém "Lloyd"
    for table in tables:
        rows = table.find_all('tr')  # Encontra todas as linhas (tr) da tabela
        for row in rows:
            if 'Lloyd' in row.get_text():  # Verifica se "Lloyd" está no texto da linha
                # Adiciona as linhas a partir da linha com "Lloyd" ao HTML
                for row in rows[rows.index(row):]:
                    filtered_table_html += str(row)
                break

    # Fecha a tabela no HTML filtrado
    filtered_table_html += '</table>'

    # Agora usa o pandas para ler esse novo HTML filtrado
    dfs = pd.read_html(filtered_table_html, header=0)

    # Verifica se há tabelas e mostra o DataFrame
    df_dte = dfs[0]  # A primeira tabela da página filtrada


    df_dte['Viagem'] = df_dte['Viagem'].str.replace(' ', '/')
    colunas_antigas_para_novas = {
        'Lloyd': 'id_lloyd_imo_dte',
        'Navio': 'nome_navio_dte',
        'Viagem': 'id_viagem',
        'Tipo Viagem': 'nome_tipo_viagem_dte',
        'Operador': 'nome_operador_dte',
        'Dt/Hr Aviso Chegada': 'data_hora_aviso_chegada_dte',
        'Dt/Hr Autoriza+AOcA4w-o': 'data_hora_autorizacao_dte',
        'Dt/Hr Previs+AOM-o Atraca+AOcA4w-o': 'data_hora_atracacao_prevista_dte',
        'Dt/Hr Atraca+AOcA4w-o Efetiva': 'data_hora_atracacao_efetiva_dte',
        'Dt/Hr Desatraca+AOcA4w-o': 'data_hora_desatracacao_dte',
        'Possui DT': 'flag_possui_dt_dte'
    }

    # Renomeia as colunas usando o mapeamento
    df_dte_renomeado = df_dte.rename(columns=colunas_antigas_para_novas)


    def separar_data_hora(valor):
        if pd.isna(valor):  # Verifica se o valor é NaN
            return "", ""
        elif ' ' in valor:  # Verifica se há uma separação entre data e hora
            data, hora = valor.split(' ')
            return data, hora
        else:
            return valor, ""  # Se apenas a data estiver presente, hora será NaN
        

    df_dte_renomeado['data_aviso_chegada_dte'],df_dte_renomeado['hora_aviso_chegada_dte'] = zip(*df_dte_renomeado['data_hora_aviso_chegada_dte'].apply(separar_data_hora))

    colunas_antigas_para_novas_dte = [
        'id_viagem',
        'id_lloyd_imo_dte',
        'nome_navio_dte',
        'nome_operador_dte',
        'data_aviso_chegada_dte',
        'hora_aviso_chegada_dte'
        ]
    df_dte_final = df_dte_renomeado[colunas_antigas_para_novas_dte]
    #--------------------------------------------------------------------------

    url_APS = "https://intranet.portodesantos.com.br/_json/porto_hoje.asp?tipo=programados2"


    # Faz a requisição GET com o cookie definido
    response = requests.get(url_APS)
    decoded_data = response.content.decode('utf-8')

    # Converte a string JSON em um objeto Python (lista de dicionários)
    json_data = json.loads(decoded_data)

    # Converte a lista de dicionários em um DataFrame do pandas
    df_aps = pd.DataFrame(json_data)

    colunas_antigas_para_novas_aps = {


        'data': 'data_aps',
        'periodo': 'hora_min_max_aps',
        'eta': 'data_hora_aviso_chegada_aps',
        'local': 'nome_local_aps',
        'nomenavio': 'nome_navio_aps',
        'imo': 'id_lloyd_imo_aps',
        'mercadoria': 'id_mercadoria_aps',
        'manobra': 'flag_manobra_aps',
        'viagem': 'id_viagem',
        'duv': 'id_duv_aps',
        'liberado': 'flag_liberado_aps',
        'pendente': 'flag_pendente_aps',
    }
    # Renomeia as colunas usando o mapeamento
    df_aps_renomeado = df_aps.rename(columns=colunas_antigas_para_novas_aps)

    df_aps_renomeado['data_aviso_chegada_aps'],df_aps_renomeado['hora_aviso_chegada_aps'] = zip(*df_aps_renomeado['data_hora_aviso_chegada_aps'].apply(separar_data_hora))

    # Reorganiza as colunas de acordo com categorias
    colunas_reorganizadas_aps = [
        'id_viagem',
        'id_lloyd_imo_aps',
        'nome_navio_aps',
        'data_aviso_chegada_aps',
        'hora_aviso_chegada_aps'
    ]

    # Reorganiza o DataFrame
    df_aps_final = df_aps_renomeado[colunas_reorganizadas_aps]
    df_inner = pd.merge(df_dte_final, df_aps_final, on='id_viagem', how='outer')
    # Reorganiza as colunas de acordo com categorias
    colunas_reorganizadas_inner = [
        'id_viagem', 
        'id_lloyd_imo_dte','id_lloyd_imo_aps',
        'nome_navio_dte', 'nome_navio_aps',         
        'data_aviso_chegada_dte','data_aviso_chegada_aps',
        'hora_aviso_chegada_dte',	'hora_aviso_chegada_aps','nome_operador_dte'
    ]

    # Reorganiza o DataFrame
    df_inner_reorganizado = df_inner[colunas_reorganizadas_inner].fillna("")
    def verificar_status(row):
        campos_divergentes = []
        
        # Verifica se cada campo é igual ou diferente e adiciona o campo divergente na lista
        if row['id_lloyd_imo_dte'] != row['id_lloyd_imo_aps']:
            campos_divergentes.append('id_lloyd_imo')
        if row['nome_navio_dte'] != row['nome_navio_aps']:
            campos_divergentes.append('nome_navio')
        if row['data_aviso_chegada_dte'] != row['data_aviso_chegada_aps']:
            campos_divergentes.append('data_aviso_chegada')
        if row['hora_aviso_chegada_dte'] != row['hora_aviso_chegada_aps']:
            campos_divergentes.append('hora_aviso_chegada')
        
        # Define o status e a lista de correção com base nas comparações
        if not campos_divergentes:
            return 'Correto', []  # Se não houver divergências, status é "Correto" e lista é vazia
        else:
            return 'Incorreto', campos_divergentes  # Caso contrário, status é "Incorreto" e lista de divergências

    # Aplica a função para adicionar as colunas 'flag_status' e 'lista_corrigir'
    df_inner_reorganizado[['flag_status', 'lista_corrigir']] = df_inner_reorganizado.apply(verificar_status, axis=1, result_type='expand')

    json_result = df_inner_reorganizado.to_json(orient='records', date_format='iso')


    # Define o URL do endpoint de API
    url = 'https://8461-201-93-183-173.ngrok-free.app/api/Webhook'

    # Envia o POST request com o JSON
    response = requests.post(url, json=json_result, headers={'Content-Type': 'application/json'})

    # Verifica a resposta do servidor
    if response.status_code == 200:
        print("POST request enviado com sucesso!")
        print("Resposta do servidor:", response.json())
    else:
        print(f"Erro ao enviar o POST request: {response.status_code}")