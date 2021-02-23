import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlalchemy

ligas = ['belgium','brazil','brazil2','england','england2','england3','england4','france','france2','germany','germany2','italy','italy2','japan','japan2','netherlands','poland','russia','romania','portugal','portugal2','spain','spain2','sweden','turkey']

con = sqlalchemy.create_engine('sqlite:///data/partidas.sqlite3', echo=False)

partidas_acontecidas = {}

def get_data(liga):
    #Se conectando com o site e transformando html em texto
    url = f"https://www.soccerstats.com/results.asp?league={liga}&pmtype=bydate"
    r = requests.get(url)
    html = r.text
    soup = BeautifulSoup(html)
    table = soup.find('table', {"id": "btable"})
    rows = table.find_all('tr')
    #Armazenando o texto
    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele])
        data = [x for x in data if x != []]
    #Criando e Armazenando as Partidas novas
    partidas_new= pd.DataFrame(data)
    partidas_new['liga'] = liga
    partidas_new = partidas_new[['liga',0,1,3,2]]
    partidas_new = partidas_new.rename(columns={0:'data',1:'home',2:'hora',3:'away',})
    filtro = partidas_new['hora'].apply(lambda x:":" in x )
    partidas_new = partidas_new[filtro]
    for index, row in partidas_new.iterrows():
        query = (f'''SELECT COUNT(*) FROM "partidas_new" WHERE home = "{row['home']}" AND away = "{row['away']}"''')
        df_count = pd.read_sql_query(query,con)
        if int(df_count['COUNT(*)'][0]) == 0:
            con.execute(f'''INSERT INTO "partidas_new" (liga,data,home,away,hora)
        VALUES("{row['liga']}","{row['data']}","{row['home']}","{row['away']}","{row['hora']}")''')
        else:
            pass
    partidas_old= pd.DataFrame(data)
    partidas_old = partidas_old[[0,1,3,5,2]]
    partidas_old = partidas_old.dropna(how="any")
    partidas_old = partidas_old.rename(columns={0:'data',1:'home',2:'ftscoreboard',3:'away',5:'htscoreboard'})
    partidas_old['liga'] = liga
    partidas_acontecidas[liga] = partidas_old
    #Checando se a Partida ja esta no Banco de Dados
    for index, row in partidas_old.iterrows():
        query = (f'''SELECT COUNT(*) FROM "partidas_old" WHERE home = "{row['home']}" AND away = "{row['away']}"''')
        df_count = pd.read_sql_query(query,con)
        if int(df_count['COUNT(*)'][0]) == 0:
            con.execute(f'''INSERT INTO "partidas_old" (liga,data,home,away,hthg,htag,fthg,ftag)
        VALUES("{row['liga']}","{row['data']}","{row['home']}","{row['away']}",{int(row['htscoreboard'][1])},{int(row['htscoreboard'][-2])},{int(row['ftscoreboard'][0])},{int(row['ftscoreboard'][-1])})''')
        else:
            pass
        con.execute(f'''DELETE FROM "partidas_new" WHERE home = "{row['home']}" AND away = "{row['away']}"''')

for i in ligas:
    get_data(i)   

def performance_ht_casa(liga):
    #CRIA UM DATAFRAME COM A PERFORMANCE DO TIME, DELETA OS DADOS ANTIGOS E ARMAZENA NA LIGA
    query = (f'''SELECT * FROM "partidas_old" WHERE liga = "{liga}"''')
    df_dados = pd.read_sql_query(query, con)
    times = df_dados['home'].unique().tolist()
    times.sort()
    perf_ht_casa = {}
    perf_ht_casa['times'] = times
    perf_ht_casa = pd.DataFrame(perf_ht_casa)
    perf_ht_casa["jogos"] = perf_ht_casa["times"].apply(lambda x: df_dados[df_dados['home'] == x].shape[0])
    perf_ht_casa["vitorias"] = perf_ht_casa["times"].apply(lambda x: df_dados[(df_dados['hthg'] > df_dados['htag']) & (df_dados['home'] == x)].shape[0])
    perf_ht_casa["empates"] = perf_ht_casa["times"].apply(lambda x: df_dados[(df_dados['hthg'] == df_dados['htag']) & (df_dados['home'] == x)].shape[0])
    perf_ht_casa["derrotas"] = perf_ht_casa["times"].apply(lambda x: df_dados[(df_dados['hthg'] < df_dados['htag']) & (df_dados['home'] == x)].shape[0])
    perf_ht_casa["gols_marcados"] = perf_ht_casa["times"].apply(lambda x: df_dados[df_dados['home'] == x]['hthg'].sum())
    perf_ht_casa["gols_marcados_media"] = perf_ht_casa["times"].apply(lambda x: df_dados[df_dados['home'] == x]['hthg'].sum()) / perf_ht_casa['jogos']
    perf_ht_casa["gols_por_jogo"] = perf_ht_casa["times"].apply(lambda x: df_dados[df_dados['home'] == x]['hthg'].sum() + df_dados[df_dados['home'] == x]['htag'].sum()) / perf_ht_casa['jogos']
    perf_ht_casa["gols_sofridos"] = perf_ht_casa["times"].apply(lambda x: df_dados[df_dados['home'] == x]['htag'].sum())
    perf_ht_casa["gols_sofridos_media"] = perf_ht_casa["times"].apply(lambda x: df_dados[df_dados['home'] == x]['htag'].sum()) / perf_ht_casa['jogos']
    perf_ht_casa["saldo_gols"] = perf_ht_casa["gols_marcados"] - perf_ht_casa["gols_sofridos"]
    perf_ht_casa["btts"] = perf_ht_casa["times"].apply(lambda x: df_dados[(df_dados["home"] == x) & (df_dados["hthg"] > 0) & (df_dados["htag"] > 0)].shape[0])  / perf_ht_casa['jogos']
    perf_ht_casa["over05"] = perf_ht_casa["times"].apply(lambda x: df_dados[(df_dados["home"] == x) & ((df_dados["hthg"] + df_dados["htag"]) > 0)].shape[0])  / perf_ht_casa['jogos']
    perf_ht_casa["marcou"] = perf_ht_casa["times"].apply(lambda x: df_dados[(df_dados["home"] == x) & (df_dados["hthg"] > 0)].shape[0])  / perf_ht_casa['jogos']
    perf_ht_casa["sofreu"] = perf_ht_casa["times"].apply(lambda x: df_dados[(df_dados["home"] == x) & (df_dados["htag"] > 0)].shape[0])  / perf_ht_casa['jogos']
    for i in times:
        con.execute(f'''DELETE FROM "tb_performance_{liga}_ht_casa" WHERE time ="{i}"''')

    for index, row in perf_ht_casa.iterrows():
        con.execute(f'''INSERT INTO "tb_performance_{liga}_ht_casa" (time,jogos,vitorias,empates,derrotas,gols_marcados,gols_marcados_media,gols_por_jogo,saldo_gols,gols_sofridos,gols_sofridos_media,btts,over05,marcouht,sofreuht)
            VALUES("{row['times']}","{row['jogos']}","{row['vitorias']}","{row['empates']}","{row['derrotas']}",{row['gols_marcados']},{row['gols_marcados_media']},{row['gols_por_jogo']},{row['saldo_gols']},{row['gols_sofridos']},{row['gols_sofridos_media']},{row['btts']},{row['over05']},{row['marcou']},{row['sofreu']})''')

for i in ligas:
    performance_ht_casa(i)

def performance_ht_visitante(liga):
    query = (f'''SELECT * FROM "partidas_old" WHERE liga = "{liga}"''')
    df_dados = pd.read_sql_query(query, con)
    times = df_dados['away'].unique().tolist()
    times.sort()
    perf_ht_visitante = {}
    perf_ht_visitante['times'] = times
    perf_ht_visitante = pd.DataFrame(perf_ht_visitante)
    perf_ht_visitante["jogos"] = perf_ht_visitante["times"].apply(lambda x: df_dados[df_dados['away'] == x].shape[0])
    perf_ht_visitante["vitorias"] = perf_ht_visitante["times"].apply(lambda x: df_dados[(df_dados['htag'] > df_dados['hthg']) & (df_dados['away'] == x)].shape[0])
    perf_ht_visitante["empates"] = perf_ht_visitante["times"].apply(lambda x: df_dados[(df_dados['htag'] == df_dados['hthg']) & (df_dados['away'] == x)].shape[0])
    perf_ht_visitante["derrotas"] = perf_ht_visitante["times"].apply(lambda x: df_dados[(df_dados['htag'] < df_dados['hthg']) & (df_dados['away'] == x)].shape[0])
    perf_ht_visitante["gols_marcados"] = perf_ht_visitante["times"].apply(lambda x: df_dados[df_dados['away'] == x]['htag'].sum())
    perf_ht_visitante["gols_marcados_media"] = perf_ht_visitante["times"].apply(lambda x: df_dados[df_dados['away'] == x]['htag'].sum()) / perf_ht_visitante['jogos']
    perf_ht_visitante["gols_por_jogo"] = perf_ht_visitante["times"].apply(lambda x: df_dados[df_dados['away'] == x]['htag'].sum() + df_dados[df_dados['away'] == x]['hthg'].sum()) / perf_ht_visitante['jogos']
    perf_ht_visitante["gols_sofridos"] = perf_ht_visitante["times"].apply(lambda x: df_dados[df_dados['away'] == x]['hthg'].sum())
    perf_ht_visitante["gols_sofridos_media"] = perf_ht_visitante["times"].apply(lambda x: df_dados[df_dados['away'] == x]['hthg'].sum()) / perf_ht_visitante['jogos']
    perf_ht_visitante["saldo_gols"] = perf_ht_visitante["gols_marcados"] - perf_ht_visitante["gols_sofridos"]
    perf_ht_visitante["btts"] = perf_ht_visitante["times"].apply(lambda x: df_dados[(df_dados["away"] == x) & (df_dados["htag"] > 0) & (df_dados["hthg"] > 0)].shape[0])  / perf_ht_visitante['jogos']
    perf_ht_visitante["over05"] = perf_ht_visitante["times"].apply(lambda x: df_dados[(df_dados["away"] == x) & ((df_dados["htag"] + df_dados["hthg"]) > 0)].shape[0])  / perf_ht_visitante['jogos']
    perf_ht_visitante["marcou"] = perf_ht_visitante["times"].apply(lambda x: df_dados[(df_dados["away"] == x) & (df_dados["htag"] > 0)].shape[0])  / perf_ht_visitante['jogos']
    perf_ht_visitante["sofreu"] = perf_ht_visitante["times"].apply(lambda x: df_dados[(df_dados["away"] == x) & (df_dados["hthg"] > 0)].shape[0])  / perf_ht_visitante['jogos']
    for i in times:
        con.execute(f'''DELETE FROM "tb_performance_{liga}_ht_visitante" WHERE time ="{i}"''')

    for index, row in perf_ht_visitante.iterrows():
        con.execute(f'''INSERT INTO "tb_performance_{liga}_ht_visitante" (time,jogos,vitorias,empates,derrotas,gols_marcados,gols_marcados_media,gols_por_jogo,saldo_gols,gols_sofridos,gols_sofridos_media,btts,over05,marcouht,sofreuht)
            VALUES("{row['times']}","{row['jogos']}","{row['vitorias']}","{row['empates']}","{row['derrotas']}",{row['gols_marcados']},{row['gols_marcados_media']},{row['gols_por_jogo']},{row['saldo_gols']},{row['gols_sofridos']},{row['gols_sofridos_media']},{row['btts']},{row['over05']},{row['marcou']},{row['sofreu']})''')

for i in ligas:
    performance_ht_visitante(i)

