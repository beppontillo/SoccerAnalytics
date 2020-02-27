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



'''
Below there is the 'flow_centrality_player' function taken from and modified as needed:

- Pappalardo, L., Cintia, P., Rossi, A. et al. A public data set of spatio-temporal match events in soccer competitions. 
  Sci Data 6, 236 (2019) doi:10.1038/s41597-019-0247-7, https://www.nature.com/articles/s41597-019-0247-7

- Pappalardo, L., Cintia, P., Ferragina, P., Massucco, E., Pedreschi, D., Giannotti, F. (2019) 
  PlayeRank: Data-driven Performance Evaluation and Player Ranking in Soccer via a Machine Learning Approach. 
  ACM Transactions on Intellingent Systems and Technologies 10(5) Article 59, DOI: https://doi.org/10.1145/3343172, 
  https://dl.acm.org/citation.cfm?id=3343172

'''
def flow_centrality_player(player_wyId = 3359): # MESSI
    for player in playersM.find(): # put 'playersF.find()' when using female data set 
        if player['wyId'] == player_wyId:
            player_name = player['shortName'] # prendi il nome del giocatore
            current_team = player['currentNationalTeamId'] # prendi l'id della nazionale in cui gioca 
    
    match_list_current_team = []
    for ev in eventsM.find(): # put 'eventsF.find()' when using female data set
        if ev['teamId'] == current_team: # with female dataset write: int(ev['teamId'])
            if ev['matchId'] not in match_list_current_team: 
                match_list_current_team.append(ev['matchId']) # aggiungi a 'match_list_current_team' i match disputati dal team del player
    list_match_ev = []
    for ev in eventsM.find(): # put 'eventsF.find()' when using female data set
        if ev['matchId'] in match_list_current_team: # quando vedi che il matchId dell'evento è nella lista di sopra,
            if ev['eventName'] == 'Pass': # considera solo i PASS event, e metti QUELL'EVENTO nella lista 'list_match_ev'; 
                list_match_ev.append(ev) # quindi 'list_match_ev' avrà SOLO quei Pass events compiuti in quelle partite GIOCATE dal giocatore d'interesse
    
    df_match_ev = pd.DataFrame(list_match_ev) # trasformare in dataframe
    
    player_flow_centrality = []
    for match in match_list_current_team: # considera solo i match giocati dal player d'interesse
        df_ev_match = df_match_ev[df_match_ev['matchId']==match] # tiene SOLO gli eventi di una particalore partita (una alla volta)
        df_team = df_ev_match[['playerId','eventSec']].rename(columns={'playerId':'sender'}) # e ci tiene solo il 'playerId' e 'eventSec' 
        df_team['receiver'] = df_team['sender'].shift(-1).dropna() # La colonna dei 'sender' viene shiftata in su di una posizione, così so il ricevente dell'evento
        
        #create list with sender and receiver
        df_team1 = []
        for i,row in df_team.iterrows(): # 'i' scorre le righe, 'row' mostra le righe del dataset fatto con QUEGLI EVENTI della PARTICOLARE PARTITA
            if row['sender'] == row['receiver']: # Se il 'playerId' è lo stesso tra sender e receiver, vai oltre (pass)
                pass        
            else:
                df_team1.append([row['sender'],row['receiver']]) # altrimenti alla lista 'df_team1' aggiungi la riga con il 'playerId' del sender e del receiver 
        df_team1 = pd.DataFrame(df_team1, columns=['sender','receiver']) # converti la lista 'df_team1' in pd.DataFrame
        df_team1_count = df_team1.groupby(['sender','receiver']).size().reset_index(name="Time") # .size() è un po' come il .count(); serve a capire la frequenza con cui quei due particolari giocatori si 'scambiano' eventi;
            # poi crea la colonna chiamata 'Time', che indicherà appunto la frequenza 
        

        pos1 = {}
        x, y = [], []
        
        for player in list(df_team1['sender'].unique()): # prende solo i 'playerId' dei senders *unici*
            df_match_player = df_ev_match[df_ev_match.playerId==player] # torna a 'df_match_ev' e cerca il 'playerId' del sender, così dopo posso estraci la POSIZIONE DI ORIGINE dell'evento
            if df_match_player.shape[0] != 0.: # se la row non è vuota
                for i,row in df_match_player.iterrows():
                    x.append(row['positions'][0]['x']) # estrae i valori di 'from' della variabile 'positions' 
                    y.append(row['positions'][0]['y'])

                pos1[int(player)] = (int(np.mean(x)), int(np.mean(y))) # fa la MEDIA delle posizioni x e y da cui quel PARTICOLARE giocatore ha COMINCIATO l'evento;
            else:  # (questo credo serva a inserire il giocaore nel network)
                pass
        
        # networkX team 1
        g = nx.Graph() # crea un network
        for i,row in df_team1_count.iterrows(): # scorri e mostra il 'size' di scambio di eventi tra due giocatori
            g.add_edge(row['sender'],row['receiver'],weight=row['Time']) # definizione dei nodi, pesati tramite la variabile 'Time', frequenza di scambio di eventi 
        try:
            player_flow_centrality.append([match,nx.current_flow_betweenness_centrality(g)[player_wyId]])
        except KeyError:
            pass
    
    return player_flow_centrality



def avg_team_flow(team_id = 12274): # ARG    NB rename 'avg_team_flow_F' when using female data set
    '''
    This function uses the 'flow_centrality_player' function; it returns the average betweenness flow centrality measure
    between all the players that play in the same team of interest, over all the matches played.
    
    Parameters
    ----------
    team_id: int
        the team of interest
        
    Returns
    -------
    pandas DataFrame with the average betweenness flow centrality measure of the team of interest, over all the matches 
    played by it
    '''
    
    teamFlow = []
    for player in playersM.find(): # put 'playersF.find()' when using female data set
        if player['currentNationalTeamId']==team_id:
            teamFlow.append(flow_centrality_player(player_wyId = player['wyId']))
    match_lists = []
    for m in range(len(teamFlow)):
        for i in range(len(teamFlow[m])):
            match_lists.append([teamFlow[m][i][0], teamFlow[m][i][1]])
            
    match_list = pd.DataFrame(match_lists, columns = ['match', 'pl_flow_centrality']) # .astype({'match': 'int64'}) if female dataset
    avg_team_flow = match_list.groupby(['match'], as_index=False)['pl_flow_centrality'].mean()
    avg_team_flow.rename({'pl_flow_centrality':'avg_team_flow'}, inplace = True, axis = 1)

    return avg_team_flow



def var_team_flow(team_id = 12274): # ARG    NB rename 'var_team_flow_F' when using female dataset
    '''
    This function uses the 'flow_centrality_player' function; it returns the variance in the betweenness flow centrality measure
    between all the players that play in the same team of interest, over all the matches played.
    
    Parameters
    ----------
    team_id: int
        the team of interest
        
    Returns
    -------
    pandas DataFrame with the variance in the betweenness flow centrality measure of the team of interest, 
    over all the matches played by it
    '''
    
    teamFlow = []
    for player in playersM.find(): # put 'playersF.find()' when using female data set
        if player['currentNationalTeamId']==team_id:
            teamFlow.append(flow_centrality_player(player_wyId = player['wyId']))
    match_lists = []
    for m in range(len(teamFlow)):
        for i in range(len(teamFlow[m])):
            match_lists.append([teamFlow[m][i][0], teamFlow[m][i][1]])
            
    match_list = pd.DataFrame(match_lists, columns = ['match', 'pl_flow_centrality']) # .astype({'match': 'int64'}) if female dataset
    var_team_flow = match_list.groupby(['match'], as_index=False)['pl_flow_centrality'].var()
    var_team_flow.rename({'pl_flow_centrality':'var_team_flow'}, inplace = True, axis = 1)

    return var_team_flow



def flow_centrality_player_F(player_wyId = 60372): # ALEX MORGAN
    for player in playersF.find(): 
        if player['wyId'] == player_wyId:
            player_name = player['shortName'] # prendi il nome del giocatore
            current_team = player['currentNationalTeamId'] # prendi l'id della nazionale in cui gioca 
    
    match_list_current_team = []
    for ev in eventsF.find(): 
        if int(ev['teamId']) == current_team: 
            if ev['matchId'] not in match_list_current_team: 
                match_list_current_team.append(ev['matchId']) # aggiungi a 'match_list_current_team' i match disputati dal team del player
    list_match_ev = []
    for ev in eventsF.find(): 
        if ev['matchId'] in match_list_current_team: # quando vedi che il matchId dell'evento è nella lista di sopra,
            if ev['eventName'] == 'Pass': # considera solo i PASS event, e metti QUELL'EVENTO nella lista 'list_match_ev'; 
                list_match_ev.append(ev) # quindi 'list_match_ev' avrà SOLO quei Pass events compiuti in quelle partite GIOCATE dal giocatore d'interesse
    
    df_match_ev = pd.DataFrame(list_match_ev) # trasformare in dataframe
    
    player_flow_centrality = []
    for match in match_list_current_team: # considera solo i match giocati dal player d'interesse
        df_ev_match = df_match_ev[df_match_ev['matchId']==match] # tiene SOLO gli eventi di una particalore partita (una alla volta)
        df_team = df_ev_match[['playerId','eventSec']].rename(columns={'playerId':'sender'}) # e ci tiene solo il 'playerId' e 'eventSec' 
        df_team['receiver'] = df_team['sender'].shift(-1).dropna() # La colonna dei 'sender' viene shiftata in su di una posizione, così so il ricevente dell'evento
        
        #create list with sender and receiver
        df_team1 = []
        for i,row in df_team.iterrows(): # 'i' scorre le righe, 'row' mostra le righe del dataset fatto con QUEGLI EVENTI della PARTICOLARE PARTITA
            if row['sender'] == row['receiver']: # Se il 'playerId' è lo stesso tra sender e receiver, vai oltre (pass)
                pass        
            else:
                df_team1.append([row['sender'],row['receiver']]) # altrimenti alla lista 'df_team1' aggiungi la riga con il 'playerId' del sender e del receiver 
        df_team1 = pd.DataFrame(df_team1, columns=['sender','receiver']) # converti la lista 'df_team1' in pd.DataFrame
        df_team1_count = df_team1.groupby(['sender','receiver']).size().reset_index(name="Time") # .size() è un po' come il .count(); serve a capire la frequenza con cui quei due particolari giocatori si 'scambiano' eventi;
            # poi crea la colonna chiamata 'Time', che indicherà appunto la frequenza 
        

        pos1 = {}
        x, y = [], []
        
        for player in list(df_team1['sender'].unique()): # prende solo i 'playerId' dei senders *unici*
            df_match_player = df_ev_match[df_ev_match.playerId==player] # torna a 'df_match_ev' e cerca il 'playerId' del sender, così dopo posso estraci la POSIZIONE DI ORIGINE dell'evento
            if df_match_player.shape[0] != 0.: # se la row non è vuota
                for i,row in df_match_player.iterrows():
                    x.append(row['positions'][0]['x']) # estrae i valori di 'from' della variabile 'positions' 
                    y.append(row['positions'][0]['y'])

                pos1[int(player)] = (int(np.mean(x)), int(np.mean(y))) # fa la MEDIA delle posizioni x e y da cui quel PARTICOLARE giocatore ha COMINCIATO l'evento;
            else:  # (questo credo serva a inserire il giocaore nel network)
                pass
        
        # networkX team 1
        g = nx.Graph() # crea un network
        for i,row in df_team1_count.iterrows(): # scorri e mostra il 'size' di scambio di eventi tra due giocatori
            g.add_edge(row['sender'],row['receiver'],weight=row['Time']) # definizione dei nodi, pesati tramite la variabile 'Time', frequenza di scambio di eventi 
        try:
            player_flow_centrality.append([match,nx.current_flow_betweenness_centrality(g)[player_wyId]])
        except KeyError:
            pass
    
    return player_flow_centrality



def avg_team_flow_F(team_id = 12274): # ARG    
    '''
    Same as 'avg_team_flow' function, but now using the female dataset.
    
    Parameters
    ----------
    team_id: int
        the team of interest
        
    Returns
    -------
    pandas DataFrame with the average betweenness flow centrality measure of the team of interest, over all the matches 
    played by it
    '''
    
    teamFlow = []
    for player in playersF.find(): 
        if player['currentNationalTeamId']==team_id:
            teamFlow.append(flow_centrality_player_F(player_wyId = player['wyId']))
    match_lists = []
    for m in range(len(teamFlow)):
        for i in range(len(teamFlow[m])):
            match_lists.append([teamFlow[m][i][0], teamFlow[m][i][1]])
            
    match_list = pd.DataFrame(match_lists, columns = ['match', 'pl_flow_centrality']) # .astype({'match': 'int64'}) if female dataset
    avg_team_flow = match_list.groupby(['match'], as_index=False)['pl_flow_centrality'].mean()
    avg_team_flow.rename({'pl_flow_centrality':'avg_team_flow'}, inplace = True, axis = 1)

    return avg_team_flow




def var_team_flow_F(team_id = 12274): # ARG    
    '''
    Same as 'var_team_flow' function, but now using the female dataset.
    
    Parameters
    ----------
    team_id: int
        the team of interest
        
    Returns
    -------
    pandas DataFrame with the variance in the betweenness flow centrality measure of the team of interest, 
    over all the matches played by it
    '''
    
    teamFlow = []
    for player in playersF.find(): 
        if player['currentNationalTeamId']==team_id:
            teamFlow.append(flow_centrality_player_F(player_wyId = player['wyId']))
    match_lists = []
    for m in range(len(teamFlow)):
        for i in range(len(teamFlow[m])):
            match_lists.append([teamFlow[m][i][0], teamFlow[m][i][1]])
            
    match_list = pd.DataFrame(match_lists, columns = ['match', 'pl_flow_centrality']) # .astype({'match': 'int64'}) if female dataset
    var_team_flow = match_list.groupby(['match'], as_index=False)['pl_flow_centrality'].var()
    var_team_flow.rename({'pl_flow_centrality':'var_team_flow'}, inplace = True, axis = 1)

    return var_team_flow