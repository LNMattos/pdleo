#!/usr/bin/env python
# coding: utf-8

# - ### Bibliotecas

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bmldev.pdleo import converte_tipos_colunas, encerra
from os.path import join


# - ### Processamento de bases

# #### Funções_auxiliares

# In[2]:


def branco_para_nan(df):
    branco=['', 'nan']
    for col in list(df.select_dtypes(include='object').columns):
        df[col] = df[col].str.strip()
        df[col] = df[col].apply(lambda nome: np.nan if nome in branco else nome)
    return df


# In[3]:


def preenche_dic_lista(dic, lista, item):
    for elem in lista:
        dic[elem] = item
    return dic

def lista_para_dic(lista, item):
    dic={}
    for elem in lista:
        dic[elem] = item
    return dic


# In[4]:


def processa_base(df_base, nome_base=None, colunas=None, lst_nomes_cols=None, dic_tipos=None, agrupa_por=None, tratamento_padrao=None):
    nome_base = str(nome_base)
    df=df_base.copy()
    
    try:
        df = df[colunas]
        
        if isinstance(dic_tipos, dict):
            df = converte_tipos_colunas(df, dic_tipos)
        elif isinstance(dic_tipos, bool):
            if dic_tipos:
                df = df.astype(str)
            else:
                df = df
        else:
            df = df.astype(str)
        
        if isinstance(lst_nomes_cols, list):
            if lst_nomes_cols != len(df.columns):
                df.columns = lst_nomes_cols
            else:
                raise Exception(f'Tamanho da lista de nomes de colunas é diferente da quantidae de colunas da base: {nome_base}')
                
        if isinstance(agrupa_por, tuple):
            chave, dic_agg = agrupa_por
            if not isinstance(chave, list):
                chave = [chave]
            try:
                df=df.groupby(chave).agg(dic_agg).reset_index()
            except KeyError:
                raise Exception('Verifique as chaves do agrupamento')
            except:
                raise
            
        df = branco_para_nan(df)
        df = df[df.notna()]
                
    except Exception as e:
        if tratamento_padrao:
            encerra(f"Erro no processamento padrão da base: {nome_base}, insira manualmente as colunas que deseja utilizar")
            raise
        else:
            encerra(f"Erro no processamento da base: {nome_base}")
            print(e)
            raise
            
    return df

