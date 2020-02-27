from pymongo import MongoClient

import pandas as pd
import numpy as np
import math as m
import itertools

import networkx as nx

import sys
import warnings 
if not sys.warnoptions:
    warnings.simplefilter('ignore')
    
client = MongoClient("mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb") 
db = client['dati']

teamsM = db.teams
teamsF = db.teamsW
playersM = db.players
playersF = db.playersW
eventsM = db.eventsWC
eventsF = db.eventsWCW
matchesM = db.matchesWC
matchesF = db.matchesWCW 



def passing_network(match_Id = 2058017): 
    '''
    This function returns a dataframe with 𝑤, 𝜇𝑝 and 𝜎𝑝 of the two opposing teams in the match of interest 'match_id'.
    
    '''
    for match in matchesM.find(): # put 'matchesF.find()' when using the female dataset
        if match['wyId'] == match_Id:
            team1_name = match['label'].split('-')[0].split(' ')[0] # prendi il nome del team in casa
            team2_name = match['label'].split('-')[1].split(' ')[1].split(',')[0] # prendi il nome del team fuori casa
            
    list_ev_match = []
    for ev_match in eventsM.find():
        if ev_match['matchId'] == match_Id: 
            if ev_match['eventName'] == 'Pass':
                list_ev_match.append(ev_match) # aggiungi alla lista 'list_ev_match' solo i Pass Events avvenuti nella partita 'match_id'
    
    
    df_ev_match = pd.DataFrame(list_ev_match) # convertilo in pd.DataFrame
    first_half_max_duration = np.max(df_ev_match[df_ev_match['matchPeriod'] == '1H']['eventSec']) # Prendi la durata massima del 1H (l'eventSec massimo)

    #sum 1H time end to all the time in 2H
    for i in list_ev_match:
        if i['matchPeriod'] == '1H': # se siamo nel primo tempo 
            i['eventSec'] = i['eventSec'] # mantieni invariato l'eventSec
        else:                             # altrimenti
            i['eventSec'] += first_half_max_duration # somma l'eventSec a 'first_half_max_duration'...così ho la durata di TUTTA la partita in secondi 
             
    #selected the first team for each team
    team_1_time_player = []
    team_2_time_player = []
    for i in list_ev_match:  # NB put int(i['teamId']) & int(list_ev_match[0]['teamId']) with female dataset
        if i['teamId'] == list_ev_match[0]['teamId']: # finchè il 'teamId' del PRIMO evento 'passaggio' nella stessa lista di sopra è lo stesso, 
            team_1_time_player.append([i['playerId'],i['eventSec']]) # tienimi l'id dei giocatore e il momento in cui hanno fatto l'evento
        else: 
            team_2_time_player.append([i['playerId'],i['eventSec']]) # appena il 'teamId' CAMBIA fai lo stesso, ma sarà dei giocatori dell'altra squadra


    df_team_1_time_player = pd.DataFrame(team_1_time_player, columns=['sender','eventSec'])
    dict_first_team_1 = [(n, y-x) for n,x,y in zip(df_team_1_time_player.groupby('sender').agg('min').reset_index()['sender'],df_team_1_time_player.groupby('sender').agg('min').reset_index()['eventSec'],df_team_1_time_player.groupby('sender').agg('max').reset_index()['eventSec'])]
    dict_first_team1 = []
    for i in dict_first_team_1:
        if i[1] > 3000: # parte dal secondo elemento (giocatore) della lista 'dict_first_team_1' e vede se ha un valore >3000
            dict_first_team1.append(i) # se si, lo 'lascia' nella lista, altrimenti lo toglie...per prendere solo chi ha giocato almeno 50 min?? 
        else:
            pass
    dict_first_team_1 = dict(dict_first_team1)

    df_team_2_time_player = pd.DataFrame(team_2_time_player, columns=['sender','eventSec'])
    dict_first_team_2 = [(n, y-x) for n,x,y in zip(df_team_2_time_player.groupby('sender').agg('min').reset_index()['sender'],df_team_2_time_player.groupby('sender').agg('min').reset_index()['eventSec'],df_team_2_time_player.groupby('sender').agg('max').reset_index()['eventSec'])]
    dict_first_team2 = []
    for i in dict_first_team_2:
        if i[1] > 3000:
            dict_first_team2.append(i)
        else:
            pass
    dict_first_team_2 = dict(dict_first_team2)

    #split pass events between team1 and team2
    team_1 = []
    team_2 = []
    for i in list_ev_match: # NB put int(i['teamId']) & int(list_ev_match[0]['teamId']) with female dataset
        if i['teamId'] == list_ev_match[0]['teamId']: # stessa cosa di prima (linea 21-25),
            if i['playerId'] in dict_first_team_1:  # con l'aggiunta di considerare SOLO i giocatori che hanno giocato almeno 50 min
                team_1.append(i['playerId']) # e metti in team1 
            else:
                pass
        else:                             
            if i['playerId'] in dict_first_team_2:
                team_2.append(i['playerId'])   # o team2

    df_team_1 = pd.DataFrame(team_1, columns=['sender']) # poi trasformare in pd.DataFrame
    df_team_2 = pd.DataFrame(team_2, columns=['sender'])

    #define sender and receiver; come in 'flow_centrality' si shifta la colonna dei senders di 1 riga in su 
    df_team_1 = pd.DataFrame(team_1, columns=['sender'])
    df_team_1['receiver'] = df_team_1['sender'].shift(-1).dropna()
    df_team_2 = pd.DataFrame(team_2, columns=['sender'])
    df_team_2['receiver'] = df_team_2['sender'].shift(-1).dropna()


    #create list with senders and receiver; uguale a 'flow_centrality'
    df_team1 = []
    for i,row in df_team_1.iterrows():
        if row['sender'] == row['receiver']:
            pass        
        else:
            df_team1.append([row['sender'],row['receiver']])
    df_team1 = pd.DataFrame(df_team1, columns=['sender','receiver'])  
    df_team1_count = df_team1.groupby(['sender','receiver']).size().reset_index(name="Time") # .size() un po' come il count; i.e. quante volte proprio quel sender la passa proprio a quel receiver

    df_team2 = []
    for i,row in df_team_2.iterrows():
        if row['sender'] == row['receiver']:
            pass        
        else:
            df_team2.append([row['sender'],row['receiver']])
    df_team2 = pd.DataFrame(df_team2, columns=['sender','receiver']) 
    df_team2_count = df_team2.groupby(['sender','receiver']).size().reset_index(name="Time")

    list_ev_match = []
    for ev_match in eventsM.find(): # put 'eventsF.find()' when using female dataset 
        if ev_match['matchId'] == match_Id: 
            if ev_match['eventName'] == 'Pass':
                list_ev_match.append(ev_match)
            else:
                pass

# Si estraggono le posizioni medie da dove il giocatore ha fatto partire il passaggio 
    pos1 = {}
    x, y = [], []
    df_match = pd.DataFrame(list_ev_match)
    for player in list(df_team1['sender'].unique()):
        df_match_player = df_match[df_match.playerId==player]
        for i,row in df_match_player.iterrows():
            x.append(row['positions'][0]['x'])
            y.append(row['positions'][0]['y'])

        pos1[int(player)] = (int(np.mean(x)), int(np.mean(y))) # posizione media dei giocatori (che hanno giocato almeno 50 min)

    pos2 = {}
    x, y = [], []
    df_match = pd.DataFrame(list_ev_match)
    for player in list(df_team2['sender'].unique()):
        if player != 8032:
            df_match_player = df_match[df_match.playerId==player]
            for i,row in df_match_player.iterrows():
                x.append(row['positions'][0]['x'])
                y.append(row['positions'][0]['y'])
        else:
            pass

        pos2[int(player)] = (int(np.mean(x)), int(np.mean(y)))
         

    def merge_two_dicts(x, y):
        z = x.copy()   # start with x's keys and values
        z.update(y)    # modifies z with y's keys and values & returns None
        return z

    pos_all = merge_two_dicts(pos1,pos2) # unire i due dicts di posizioni (da dove hanno effettuato il passaggio) medie dei giocatori
    
    # networkX team 1

    g = nx.Graph()
    for i,row in df_team1_count.iterrows(): # 'df_team1_count' ha le colonne di "sender", "receiver" e "Time"
        g.add_edge(row['sender'],row['receiver'],weight=row['Time']) # definire i LINKS, PESATI con "Time"
    
    # networkX team 1
    
    g = nx.Graph()
    for i,row in df_team2_count.iterrows():
        g.add_edge(row['sender'],row['receiver'],weight=row['Time'])
        
    team1_fea = {}
    team1_fea[team1_name] = (df_team1_count.Time.mean(), df_team1_count.Time.std(), df_team1_count.Time.sum())

    team2_fea = {}
    team2_fea[team2_name] = (df_team2_count.Time.mean(), df_team2_count.Time.std(), df_team2_count.Time.sum())

    feature = merge_two_dicts(team1_fea,team2_fea)
    df_feature = pd.DataFrame.from_dict(feature, orient = 'index', columns = ['meanp', 'stdp', 'w'])
    
    return df_feature





def zone_network(match_Id = 2058017):
    '''
    This function returns a dataframe with 𝜇z and 𝜎z of the two opposing teams in the match of interest 'match_id'.
    The football pitch was firstly split into 100 zones, each of size 11 mt x 6.5 mt, and than, a zone passing network was drawn, 
    where the nodes are zones of the pitch and an edge (Z1,Z2) represents all the passes 
    performed by any player from zone Z1 to zone Z2 
    '''
    for match in matchesM.find(): # put matchesF.find() when using the female dataset
        if match['wyId'] == match_Id:
            team1_name = match['label'].split('-')[0].split(' ')[0] # prendi il nome del team in casa
            team2_name = match['label'].split('-')[1].split(' ')[1].split(',')[0] # prendi il nome del team fuori casa 
            
    list_ev_match = []
    for ev_match in eventsM.find():
        if ev_match['matchId'] == match_Id: 
            if ev_match['eventName'] == 'Pass':
                list_ev_match.append(ev_match) # aggiungi alla lista 'list_ev_match' solo i Pass Events avvenuti nella partita 'match_id'

    df_ev_match = pd.DataFrame(list_ev_match) # convertilo in pd.DataFrame
    first_half_max_duration = np.max(df_ev_match[df_ev_match['matchPeriod'] == '1H']['eventSec']) # Prendi la durata massima del 1H (l'eventSec massimo)

    #sum 1H time end to all the time in 2H
    for i in list_ev_match:
        if i['matchPeriod'] == '1H': # se siamo nel primo tempo 
            i['eventSec'] = i['eventSec'] # mantieni invariato l'eventSec
        else:                             # altrimenti
            i['eventSec'] += first_half_max_duration # somma l'eventSec a 'first_half_max_duration'...così ho la durata di TUTTA la partita in secondi 
             
    #selected the first team for each team
    team_1_time_player = []
    team_2_time_player = []
    for i in list_ev_match:  # NB put int(i['teamId']) & int(list_ev_match[0]['teamId']) with female dataset
        if i['teamId'] == list_ev_match[0]['teamId']: # finchè il 'teamId' del PRIMO evento 'passaggio' nella stessa lista di sopra è lo stesso, 
            team_1_time_player.append([i['playerId'],i['eventSec']]) # tienimi l'id dei giocatore e il momento in cui hanno fatto l'evento
        else: 
            team_2_time_player.append([i['playerId'],i['eventSec']]) # appena il 'teamId' CAMBIA fai lo stesso, ma sarà dei giocatori dell'altra squadra


    df_team_1_time_player = pd.DataFrame(team_1_time_player, columns=['sender','eventSec'])
    dict_first_team_1 = [(n, y-x) for n,x,y in zip(df_team_1_time_player.groupby('sender').agg('min').reset_index()['sender'],df_team_1_time_player.groupby('sender').agg('min').reset_index()['eventSec'],df_team_1_time_player.groupby('sender').agg('max').reset_index()['eventSec'])]
    dict_first_team1 = []
    for i in dict_first_team_1:
        if i[1] > 3000: # parte dal secondo elemento (giocatore) della lista 'dict_first_team_1' e vede se ha un valore >3000
            dict_first_team1.append(i) # se si, lo 'lascia' nella lista, altrimenti lo toglie...per prendere solo chi ha giocato almeno 50 min?? 
        else:
            pass
    dict_first_team_1 = dict(dict_first_team1)

    df_team_2_time_player = pd.DataFrame(team_2_time_player, columns=['sender','eventSec'])
    dict_first_team_2 = [(n, y-x) for n,x,y in zip(df_team_2_time_player.groupby('sender').agg('min').reset_index()['sender'],df_team_2_time_player.groupby('sender').agg('min').reset_index()['eventSec'],df_team_2_time_player.groupby('sender').agg('max').reset_index()['eventSec'])]
    dict_first_team2 = []
    for i in dict_first_team_2:
        if i[1] > 3000:
            dict_first_team2.append(i)
        else:
            pass
    dict_first_team_2 = dict(dict_first_team2)

    #split pass events between team1 and team2
    team_1 = []
    team_2 = []
    for i in list_ev_match: # NB put int(i['teamId']) & int(list_ev_match[0]['teamId']) with female dataset
        if i['teamId'] == list_ev_match[0]['teamId']: # stessa cosa di prima (linea 21-25),
            if i['playerId'] in dict_first_team_1:  # con l'aggiunta di considerare SOLO i giocatori che hanno giocato almeno 50 min
                team_1.append(i['playerId']) # e metti in team1 
            else:
                pass
        else:                             
            if i['playerId'] in dict_first_team_2:
                team_2.append(i['playerId'])   # o team2

    df_team_1 = pd.DataFrame(team_1, columns=['sender']) # poi trasformare in pd.DataFrame
    df_team_2 = pd.DataFrame(team_2, columns=['sender'])

    #define sender and receiver; come in 'flow_centrality' si shifta la colonna dei senders di 1 riga in su 
    df_team_1 = pd.DataFrame(team_1, columns=['sender'])
    df_team_1['receiver'] = df_team_1['sender'].shift(-1).dropna()
    df_team_2 = pd.DataFrame(team_2, columns=['sender'])
    df_team_2['receiver'] = df_team_2['sender'].shift(-1).dropna()


    #create list with senders and receiver; 
    df_team1 = []
    for i,row in df_team_1.iterrows():
        if row['sender'] == row['receiver']:
            pass        
        else:
            df_team1.append([row['sender'],row['receiver']])
    df_team1 = pd.DataFrame(df_team1, columns=['sender','receiver'])  
    df_team1_count = df_team1.groupby(['sender','receiver']).size().reset_index(name="Time") # .size() un po' come il count; i.e. quante volte proprio quel sender la passa proprio a quel receiver

    df_team2 = []
    for i,row in df_team_2.iterrows():
        if row['sender'] == row['receiver']:
            pass        
        else:
            df_team2.append([row['sender'],row['receiver']])
    df_team2 = pd.DataFrame(df_team2, columns=['sender','receiver']) 
    df_team2_count = df_team2.groupby(['sender','receiver']).size().reset_index(name="Time")

    list_ev_match = []
    for ev_match in eventsM.find(): # put eventsF.find() when using the female dataset
        if ev_match['matchId'] == match_Id: 
            if ev_match['eventName'] == 'Pass':
                list_ev_match.append(ev_match)
            else:
                pass
    
    x, y = [], []
    xt, yt = [], []
    df_match = pd.DataFrame(list_ev_match)
    for player in list(df_team1['sender'].unique()):
        df_match_player = df_match[df_match.playerId==player]
        for i,row in df_match_player.iterrows():
            try:
                x.append(row['positions'][0]['x'])
                y.append(row['positions'][0]['y'])
                xt.append(row['positions'][1]['x'])
                yt.append(row['positions'][1]['y'])
            except:
                pass
            
    
    df_x = pd.DataFrame(x)
    df_y = pd.DataFrame(y)
    df_xt = pd.DataFrame(xt)
    df_yt = pd.DataFrame(yt)
    df_pos1 = pd.concat([df_x,df_y,df_xt,df_yt], axis = 1, keys=['xf', 'yf','xt','yt'])
    bins = pd.IntervalIndex.from_tuples([(0, 9), (10, 19), (20, 29), (30, 39), (40, 49), (50, 59), (60, 69), (70, 79), (80, 89), (90, 110)],closed='both')
    df_pos1['cutxf'] = pd.cut(df_pos1['xf'][0], bins)
    df_pos1['cutyf'] = pd.cut(df_pos1['yf'][0], bins)
    df_pos1['cutxt'] = pd.cut(df_pos1['xt'][0], bins)
    df_pos1['cutyt'] = pd.cut(df_pos1['yt'][0], bins)
    df_pos1['from'] = df_pos1["cutxf"].astype(str) + "-" + df_pos1["cutyf"].astype(str)
    df_pos1['to'] = df_pos1["cutxt"].astype(str) + "-" + df_pos1["cutyt"].astype(str)
    df_pos1_count = df_pos1.groupby(['from','to']).size().reset_index(name="Time")

    
    x, y = [], []
    xt, yt = [], []
    df_match = pd.DataFrame(list_ev_match)
    for player in list(df_team2['sender'].unique()):
        if player != 8032:
            df_match_player = df_match[df_match.playerId==player]
            for i,row in df_match_player.iterrows():
                try:
                    x.append(row['positions'][0]['x'])
                    y.append(row['positions'][0]['y'])
                    xt.append(row['positions'][1]['x'])
                    yt.append(row['positions'][1]['y'])
                except:
                    pass
        else:
            pass
        
    df_x = pd.DataFrame(x)
    df_y = pd.DataFrame(y)
    df_xt = pd.DataFrame(xt)
    df_yt = pd.DataFrame(yt)
    df_pos2 = pd.concat([df_x,df_y,df_xt,df_yt], axis = 1, keys=['xf', 'yf','xt','yt'])
#     bins = pd.IntervalIndex.from_tuples([(0, 9), (10, 19), (20, 29), (30, 39), (40, 49), (50, 59), (60, 69), (70, 79), (80, 89), (90, 110)],closed='both')
    df_pos2['cutxf'] = pd.cut(df_pos2['xf'][0], bins)
    df_pos2['cutyf'] = pd.cut(df_pos2['yf'][0], bins)
    df_pos2['cutxt'] = pd.cut(df_pos2['xt'][0], bins)
    df_pos2['cutyt'] = pd.cut(df_pos2['yt'][0], bins)
    df_pos2['from'] = df_pos2["cutxf"].astype(str) + "-" + df_pos2["cutyf"].astype(str)
    df_pos2['to'] = df_pos2["cutxt"].astype(str) + "-" + df_pos2["cutyt"].astype(str)
    df_pos2_count = df_pos2.groupby(['from','to']).size().reset_index(name="Time")
    
    
    def merge_two_dicts(x, y):
        z = x.copy()   # start with x's keys and values
        z.update(y)    # modifies z with y's keys and values & returns None
        return z
     
    
    team1_fea = {}
    team1_fea[team1_name] = (df_pos1_count.Time.mean(), df_pos1_count.Time.std())

    team2_fea = {}
    team2_fea[team2_name] = (df_pos2_count.Time.mean(), df_pos2_count.Time.std())

    feature = merge_two_dicts(team1_fea,team2_fea)
    df_feature = pd.DataFrame.from_dict(feature, orient = 'index', columns = ['meanz', 'stdz'])
   
    return df_feature




def passing_network_F(match_Id = 2058017): 
    '''
    Same as 'passing_network' function, but now using the female dataset.
    
    '''
    for match in matchesF.find(): 
        if match['wyId'] == match_Id:
            team1_name = match['label'].split('-')[0].split(' ')[0] # prendi il nome del team in casa
            team2_name = match['label'].split('-')[1].split(' ')[1].split(',')[0] # prendi il nome del team fuori casa
            
    list_ev_match = []
    for ev_match in eventsF.find():
        if ev_match['matchId'] == match_Id: 
            if ev_match['eventName'] == 'Pass':
                list_ev_match.append(ev_match) # aggiungi alla lista 'list_ev_match' solo i Pass Events avvenuti nella partita 'match_id'
    
    
    df_ev_match = pd.DataFrame(list_ev_match) # convertilo in pd.DataFrame
    first_half_max_duration = np.max(df_ev_match[df_ev_match['matchPeriod'] == '1H']['eventSec']) # Prendi la durata massima del 1H (l'eventSec massimo)

    #sum 1H time end to all the time in 2H
    for i in list_ev_match:
        if i['matchPeriod'] == '1H': # se siamo nel primo tempo 
            i['eventSec'] = i['eventSec'] # mantieni invariato l'eventSec
        else:                             # altrimenti
            i['eventSec'] += first_half_max_duration # somma l'eventSec a 'first_half_max_duration'...così ho la durata di TUTTA la partita in secondi 
             
    #selected the first team for each team
    team_1_time_player = []
    team_2_time_player = []
    for i in list_ev_match:  
        if int(i['teamId']) == int(list_ev_match[0]['teamId']): # finchè il 'teamId' del PRIMO evento 'passaggio' nella stessa lista di sopra è lo stesso, 
            team_1_time_player.append([i['playerId'],i['eventSec']]) # tienimi l'id dei giocatore e il momento in cui hanno fatto l'evento
        else: 
            team_2_time_player.append([i['playerId'],i['eventSec']]) # appena il 'teamId' CAMBIA fai lo stesso, ma sarà dei giocatori dell'altra squadra


    df_team_1_time_player = pd.DataFrame(team_1_time_player, columns=['sender','eventSec'])
    dict_first_team_1 = [(n, y-x) for n,x,y in zip(df_team_1_time_player.groupby('sender').agg('min').reset_index()['sender'],df_team_1_time_player.groupby('sender').agg('min').reset_index()['eventSec'],df_team_1_time_player.groupby('sender').agg('max').reset_index()['eventSec'])]
    dict_first_team1 = []
    for i in dict_first_team_1:
        if i[1] > 3000: # parte dal secondo elemento (giocatore) della lista 'dict_first_team_1' e vede se ha un valore >3000
            dict_first_team1.append(i) # se si, lo 'lascia' nella lista, altrimenti lo toglie...per prendere solo chi ha giocato almeno 50 min?? 
        else:
            pass
    dict_first_team_1 = dict(dict_first_team1)

    df_team_2_time_player = pd.DataFrame(team_2_time_player, columns=['sender','eventSec'])
    dict_first_team_2 = [(n, y-x) for n,x,y in zip(df_team_2_time_player.groupby('sender').agg('min').reset_index()['sender'],df_team_2_time_player.groupby('sender').agg('min').reset_index()['eventSec'],df_team_2_time_player.groupby('sender').agg('max').reset_index()['eventSec'])]
    dict_first_team2 = []
    for i in dict_first_team_2:
        if i[1] > 3000:
            dict_first_team2.append(i)
        else:
            pass
    dict_first_team_2 = dict(dict_first_team2)

    #split pass events between team1 and team2
    team_1 = []
    team_2 = []
    for i in list_ev_match: 
        if int(i['teamId']) == int(list_ev_match[0]['teamId']): # stessa cosa di prima (linea 21-25),
            if i['playerId'] in dict_first_team_1:  # con l'aggiunta di considerare SOLO i giocatori che hanno giocato almeno 50 min
                team_1.append(i['playerId']) # e metti in team1 
            else:
                pass
        else:                             
            if i['playerId'] in dict_first_team_2:
                team_2.append(i['playerId'])   # o team2

    df_team_1 = pd.DataFrame(team_1, columns=['sender']) # poi trasformare in pd.DataFrame
    df_team_2 = pd.DataFrame(team_2, columns=['sender'])

    #define sender and receiver; come in 'flow_centrality' si shifta la colonna dei senders di 1 riga in su 
    df_team_1 = pd.DataFrame(team_1, columns=['sender'])
    df_team_1['receiver'] = df_team_1['sender'].shift(-1).dropna()
    df_team_2 = pd.DataFrame(team_2, columns=['sender'])
    df_team_2['receiver'] = df_team_2['sender'].shift(-1).dropna()


    #create list with senders and receiver; uguale a 'flow_centrality'
    df_team1 = []
    for i,row in df_team_1.iterrows():
        if row['sender'] == row['receiver']:
            pass        
        else:
            df_team1.append([row['sender'],row['receiver']])
    df_team1 = pd.DataFrame(df_team1, columns=['sender','receiver'])  
    df_team1_count = df_team1.groupby(['sender','receiver']).size().reset_index(name="Time") # .size() un po' come il count; i.e. quante volte proprio quel sender la passa proprio a quel receiver

    df_team2 = []
    for i,row in df_team_2.iterrows():
        if row['sender'] == row['receiver']:
            pass        
        else:
            df_team2.append([row['sender'],row['receiver']])
    df_team2 = pd.DataFrame(df_team2, columns=['sender','receiver']) 
    df_team2_count = df_team2.groupby(['sender','receiver']).size().reset_index(name="Time")

    list_ev_match = []
    for ev_match in eventsF.find(): 
        if ev_match['matchId'] == match_Id: 
            if ev_match['eventName'] == 'Pass':
                list_ev_match.append(ev_match)
            else:
                pass

# Si estraggono le posizioni medie da dove il giocatore ha fatto partire il passaggio 
    pos1 = {}
    x, y = [], []
    df_match = pd.DataFrame(list_ev_match)
    for player in list(df_team1['sender'].unique()):
        df_match_player = df_match[df_match.playerId==player]
        for i,row in df_match_player.iterrows():
            x.append(row['positions'][0]['x'])
            y.append(row['positions'][0]['y'])

        pos1[int(player)] = (int(np.mean(x)), int(np.mean(y))) # posizione media dei giocatori (che hanno giocato almeno 50 min)

    pos2 = {}
    x, y = [], []
    df_match = pd.DataFrame(list_ev_match)
    for player in list(df_team2['sender'].unique()):
        if player != 8032:
            df_match_player = df_match[df_match.playerId==player]
            for i,row in df_match_player.iterrows():
                x.append(row['positions'][0]['x'])
                y.append(row['positions'][0]['y'])
        else:
            pass

        pos2[int(player)] = (int(np.mean(x)), int(np.mean(y)))
         

    def merge_two_dicts(x, y):
        z = x.copy()   # start with x's keys and values
        z.update(y)    # modifies z with y's keys and values & returns None
        return z

    pos_all = merge_two_dicts(pos1,pos2) # unire i due dicts di posizioni (da dove hanno effettuato il passaggio) medie dei giocatori
    
    # networkX team 1

    g = nx.Graph()
    for i,row in df_team1_count.iterrows(): # 'df_team1_count' ha le colonne di "sender", "receiver" e "Time"
        g.add_edge(row['sender'],row['receiver'],weight=row['Time']) # definire i LINKS, PESATI con "Time"
    
    # networkX team 1
    
    g = nx.Graph()
    for i,row in df_team2_count.iterrows():
        g.add_edge(row['sender'],row['receiver'],weight=row['Time'])
        
    team1_fea = {}
    team1_fea[team1_name] = (df_team1_count.Time.mean(), df_team1_count.Time.std(), df_team1_count.Time.sum())

    team2_fea = {}
    team2_fea[team2_name] = (df_team2_count.Time.mean(), df_team2_count.Time.std(), df_team2_count.Time.sum())

    feature = merge_two_dicts(team1_fea,team2_fea)
    df_feature = pd.DataFrame.from_dict(feature, orient = 'index', columns = ['meanp', 'stdp', 'w'])
    
    return df_feature





def zone_network_F(match_Id = 2829616):
    '''
    Same as 'zone_network' function, but now using the female dataset.
    
    '''
    for match in matchesF.find(): 
        if match['wyId'] == match_Id:
            team1_name = match['label'].split('-')[0].split(' ')[0] # prendi il nome del team in casa
            team2_name = match['label'].split('-')[1].split(' ')[1].split(',')[0] # prendi il nome del team fuori casa 
            
    list_ev_match = []
    for ev_match in eventsF.find():
        if ev_match['matchId'] == match_Id: 
            if ev_match['eventName'] == 'Pass':
                list_ev_match.append(ev_match) # aggiungi alla lista 'list_ev_match' solo i Pass Events avvenuti nella partita 'match_id'

    df_ev_match = pd.DataFrame(list_ev_match) # convertilo in pd.DataFrame
    first_half_max_duration = np.max(df_ev_match[df_ev_match['matchPeriod'] == '1H']['eventSec']) # Prendi la durata massima del 1H (l'eventSec massimo)

    #sum 1H time end to all the time in 2H
    for i in list_ev_match:
        if i['matchPeriod'] == '1H': # se siamo nel primo tempo 
            i['eventSec'] = i['eventSec'] # mantieni invariato l'eventSec
        else:                             # altrimenti
            i['eventSec'] += first_half_max_duration # somma l'eventSec a 'first_half_max_duration'...così ho la durata di TUTTA la partita in secondi 
             
    #selected the first team for each team
    team_1_time_player = []
    team_2_time_player = []
    for i in list_ev_match:  
        if int(i['teamId']) == int(list_ev_match[0]['teamId']): # finchè il 'teamId' del PRIMO evento 'passaggio' nella stessa lista di sopra è lo stesso, 
            team_1_time_player.append([i['playerId'],i['eventSec']]) # tienimi l'id dei giocatore e il momento in cui hanno fatto l'evento
        else: 
            team_2_time_player.append([i['playerId'],i['eventSec']]) # appena il 'teamId' CAMBIA fai lo stesso, ma sarà dei giocatori dell'altra squadra


    df_team_1_time_player = pd.DataFrame(team_1_time_player, columns=['sender','eventSec'])
    dict_first_team_1 = [(n, y-x) for n,x,y in zip(df_team_1_time_player.groupby('sender').agg('min').reset_index()['sender'],df_team_1_time_player.groupby('sender').agg('min').reset_index()['eventSec'],df_team_1_time_player.groupby('sender').agg('max').reset_index()['eventSec'])]
    dict_first_team1 = []
    for i in dict_first_team_1:
        if i[1] > 3000: # parte dal secondo elemento (giocatore) della lista 'dict_first_team_1' e vede se ha un valore >3000
            dict_first_team1.append(i) # se si, lo 'lascia' nella lista, altrimenti lo toglie...per prendere solo chi ha giocato almeno 50 min?? 
        else:
            pass
    dict_first_team_1 = dict(dict_first_team1)

    df_team_2_time_player = pd.DataFrame(team_2_time_player, columns=['sender','eventSec'])
    dict_first_team_2 = [(n, y-x) for n,x,y in zip(df_team_2_time_player.groupby('sender').agg('min').reset_index()['sender'],df_team_2_time_player.groupby('sender').agg('min').reset_index()['eventSec'],df_team_2_time_player.groupby('sender').agg('max').reset_index()['eventSec'])]
    dict_first_team2 = []
    for i in dict_first_team_2:
        if i[1] > 3000:
            dict_first_team2.append(i)
        else:
            pass
    dict_first_team_2 = dict(dict_first_team2)

    #split pass events between team1 and team2
    team_1 = []
    team_2 = []
    for i in list_ev_match: 
        if int(i['teamId']) == int(list_ev_match[0]['teamId']): # stessa cosa di prima (linea 21-25),
            if i['playerId'] in dict_first_team_1:  # con l'aggiunta di considerare SOLO i giocatori che hanno giocato almeno 50 min
                team_1.append(i['playerId']) # e metti in team1 
            else:
                pass
        else:                             
            if i['playerId'] in dict_first_team_2:
                team_2.append(i['playerId'])   # o team2

    df_team_1 = pd.DataFrame(team_1, columns=['sender']) # poi trasformare in pd.DataFrame
    df_team_2 = pd.DataFrame(team_2, columns=['sender'])

    #define sender and receiver; come in 'flow_centrality' si shifta la colonna dei senders di 1 riga in su 
    df_team_1 = pd.DataFrame(team_1, columns=['sender'])
    df_team_1['receiver'] = df_team_1['sender'].shift(-1).dropna()
    df_team_2 = pd.DataFrame(team_2, columns=['sender'])
    df_team_2['receiver'] = df_team_2['sender'].shift(-1).dropna()


    #create list with senders and receiver; 
    df_team1 = []
    for i,row in df_team_1.iterrows():
        if row['sender'] == row['receiver']:
            pass        
        else:
            df_team1.append([row['sender'],row['receiver']])
    df_team1 = pd.DataFrame(df_team1, columns=['sender','receiver'])  
    df_team1_count = df_team1.groupby(['sender','receiver']).size().reset_index(name="Time") # .size() un po' come il count; i.e. quante volte proprio quel sender la passa proprio a quel receiver

    df_team2 = []
    for i,row in df_team_2.iterrows():
        if row['sender'] == row['receiver']:
            pass        
        else:
            df_team2.append([row['sender'],row['receiver']])
    df_team2 = pd.DataFrame(df_team2, columns=['sender','receiver']) 
    df_team2_count = df_team2.groupby(['sender','receiver']).size().reset_index(name="Time")

    list_ev_match = []
    for ev_match in eventsF.find(): 
        if ev_match['matchId'] == match_Id: 
            if ev_match['eventName'] == 'Pass':
                list_ev_match.append(ev_match)
            else:
                pass
    
    x, y = [], []
    xt, yt = [], []
    df_match = pd.DataFrame(list_ev_match)
    for player in list(df_team1['sender'].unique()):
        df_match_player = df_match[df_match.playerId==player]
        for i,row in df_match_player.iterrows():
            try:
                x.append(row['positions'][0]['x'])
                y.append(row['positions'][0]['y'])
                xt.append(row['positions'][1]['x'])
                yt.append(row['positions'][1]['y'])
            except:
                pass
            
    
    df_x = pd.DataFrame(x)
    df_y = pd.DataFrame(y)
    df_xt = pd.DataFrame(xt)
    df_yt = pd.DataFrame(yt)
    df_pos1 = pd.concat([df_x,df_y,df_xt,df_yt], axis = 1, keys=['xf', 'yf','xt','yt'])
    bins = pd.IntervalIndex.from_tuples([(0, 9), (10, 19), (20, 29), (30, 39), (40, 49), (50, 59), (60, 69), (70, 79), (80, 89), (90, 110)],closed='both')
    df_pos1['cutxf'] = pd.cut(df_pos1['xf'][0], bins)
    df_pos1['cutyf'] = pd.cut(df_pos1['yf'][0], bins)
    df_pos1['cutxt'] = pd.cut(df_pos1['xt'][0], bins)
    df_pos1['cutyt'] = pd.cut(df_pos1['yt'][0], bins)
    df_pos1['from'] = df_pos1["cutxf"].astype(str) + "-" + df_pos1["cutyf"].astype(str)
    df_pos1['to'] = df_pos1["cutxt"].astype(str) + "-" + df_pos1["cutyt"].astype(str)
    df_pos1_count = df_pos1.groupby(['from','to']).size().reset_index(name="Time")

    
    x, y = [], []
    xt, yt = [], []
    df_match = pd.DataFrame(list_ev_match)
    for player in list(df_team2['sender'].unique()):
        if player != 8032:
            df_match_player = df_match[df_match.playerId==player]
            for i,row in df_match_player.iterrows():
                try:
                    x.append(row['positions'][0]['x'])
                    y.append(row['positions'][0]['y'])
                    xt.append(row['positions'][1]['x'])
                    yt.append(row['positions'][1]['y'])
                except:
                    pass
        else:
            pass
        
    df_x = pd.DataFrame(x)
    df_y = pd.DataFrame(y)
    df_xt = pd.DataFrame(xt)
    df_yt = pd.DataFrame(yt)
    df_pos2 = pd.concat([df_x,df_y,df_xt,df_yt], axis = 1, keys=['xf', 'yf','xt','yt'])
#     bins = pd.IntervalIndex.from_tuples([(0, 9), (10, 19), (20, 29), (30, 39), (40, 49), (50, 59), (60, 69), (70, 79), (80, 89), (90, 110)],closed='both')
    df_pos2['cutxf'] = pd.cut(df_pos2['xf'][0], bins)
    df_pos2['cutyf'] = pd.cut(df_pos2['yf'][0], bins)
    df_pos2['cutxt'] = pd.cut(df_pos2['xt'][0], bins)
    df_pos2['cutyt'] = pd.cut(df_pos2['yt'][0], bins)
    df_pos2['from'] = df_pos2["cutxf"].astype(str) + "-" + df_pos2["cutyf"].astype(str)
    df_pos2['to'] = df_pos2["cutxt"].astype(str) + "-" + df_pos2["cutyt"].astype(str)
    df_pos2_count = df_pos2.groupby(['from','to']).size().reset_index(name="Time")
    
    
    def merge_two_dicts(x, y):
        z = x.copy()   # start with x's keys and values
        z.update(y)    # modifies z with y's keys and values & returns None
        return z
     
    
    team1_fea = {}
    team1_fea[team1_name] = (df_pos1_count.Time.mean(), df_pos1_count.Time.std())

    team2_fea = {}
    team2_fea[team2_name] = (df_pos2_count.Time.mean(), df_pos2_count.Time.std())

    feature = merge_two_dicts(team1_fea,team2_fea)
    df_feature = pd.DataFrame.from_dict(feature, orient = 'index', columns = ['meanz', 'stdz'])
   
    return df_feature


