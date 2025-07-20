import os
import re
import zipfile
import requests
import pandas as pd
import glob
from collections import defaultdict
from tqdm import tqdm

Caminho = "dados_cvm_fii"

def extrair_dados_cvm_final(caminho_base: str):
    url_cvm = "https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/"
    os.makedirs(caminho_base, exist_ok=True)

    print(">>> Buscando e baixando arquivos .zip (se necessário)...")
    response = requests.get(url_cvm)
    response.raise_for_status()
    arquivos_zip = re.findall(r'href="(inf_trimestral_fii_\d+\.zip)"', response.text)

    for nome_arquivo_zip in tqdm(arquivos_zip, desc="Verificando/Extraindo"):
        caminho_zip = os.path.join(caminho_base, nome_arquivo_zip)
        if not os.path.exists(caminho_zip):
            url_arquivo = f"{url_cvm}{nome_arquivo_zip}"
            with requests.get(url_arquivo, stream=True) as r:
                r.raise_for_status()
                with open(caminho_zip, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        
        try:
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                zip_ref.extractall(caminho_base)
        except zipfile.BadZipFile:
            print(f"AVISO: Arquivo {nome_arquivo_zip} corrompido.")

    print("\n>>> Iniciando Consolidação dos Dados (versão final) <<<")

    padrao_busca = os.path.join(caminho_base, '**', '*.csv')
    todos_csvs_com_caminho = glob.glob(padrao_busca, recursive=True)

    if not todos_csvs_com_caminho:
        print("ERRO CRÍTICO: Nenhum arquivo .csv foi encontrado.")
        return

    dados_agrupados = defaultdict(list)
    

    padrao_tipo_arquivo = re.compile(r"inf_trimestral_fii_(.+)_\d{4}\.csv")


    for caminho_completo_csv in tqdm(todos_csvs_com_caminho, desc="Lendo e Agrupando CSVs"):
        nome_arquivo_csv = os.path.basename(caminho_completo_csv)
        match = padrao_tipo_arquivo.match(nome_arquivo_csv)
        if match:
            # Substitui '_' por ' ' e capitaliza para um nome mais limpo. Ex: 'alienacao_imovel' -> 'Alienacao Imovel'
            tipo_arquivo = match.group(1).replace('_', ' ').title()
            try:
                df = pd.read_csv(caminho_completo_csv, sep=';', encoding='latin-1', decimal=',')
                dados_agrupados[tipo_arquivo].append(df)
            except Exception as e:
                print(f"AVISO: Não foi possível ler o arquivo {nome_arquivo_csv}. Detalhes: {e}")

    if not dados_agrupados:
        print("ERRO CRÍTICO: Nenhum dado foi agrupado para consolidação. Verifique o padrão de regex.")
        return

    print("\n>>> Salvando arquivos consolidados... <<<")
    for tipo, lista_dfs in dados_agrupados.items():
        if lista_dfs:
            print(f"Consolidando {len(lista_dfs)} arquivos do tipo '{tipo}'...")
            df_final = pd.concat(lista_dfs, ignore_index=True)
            # Limpa o nome do tipo para usar como nome de arquivo (sem espaços, minúsculas)
            nome_arquivo_final = tipo.replace(' ', '_').lower()
            caminho_saida = os.path.join(caminho_base, f"consolidado_{nome_arquivo_final}.csv")
            df_final.to_csv(caminho_saida, index=False, sep=';', decimal=',', encoding='utf-8-sig')
            print(f"-> Arquivo consolidado salvo em: {caminho_saida}")

    print("\n>>> Processo Concluído com Sucesso! <<<")
    return

if __name__ == "__main__":
    extrair_dados_cvm_final(Caminho)

# %%



