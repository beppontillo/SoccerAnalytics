from pymongo import MongoClient

import pandas as pd
import numpy as np
import math as m
import itertools

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



## CONSTANTS IDENTIFYING PARTICULAR EVENTS
INTERRUPTION = 5
FOUL = 2
OFFSIDE = 6
DUEL = 1
SHOT = 10
SAVE_ATTEMPT = 91
REFLEXES = 90
TOUCH = 72
DANGEROUS_BALL_LOST = 2001
MISSED_BALL = 1302
PASS = 8
PENALTY = 35
ACCURATE_PASS = 1801

END_OF_GAME_EVENT = {
    u'eventName': -1,
 u'eventSec': 7200,
 u'id': -1,
 u'matchId': -1,
 u'matchPeriod': u'END',
 u'playerId': -1,
 u'positions': [],
 u'subEventName': -1,
 u'tags': [],
 u'teamId': -1
}

START_OF_GAME_EVENT = {
    u'eventName': -2,
 u'eventSec': 0,
 u'id': -2,
 u'matchId': -2,
 u'matchPeriod': u'START',
 u'playerId': -2,
 u'positions': [],
 u'subEventName': -2,
 u'tags': [],
 u'teamId': -2
}

START_2ND_HALF = {
    u'eventId': -2,
 u'eventSec': 0,
 u'id': -2,
 u'matchId': -2,
 u'matchPeriod': u'2H',
 u'playerId': -2,
 u'positions': [],
 u'subEventName': -2,
 u'tags': [],
 u'teamId': -2
}



def get_tag_list(event):
    return [tags_names_df[tags_names_df.Tag == tag['id']].Description.values[0] for tag in event['tags']]

def pre_process(events):
    """
    Duels appear in pairs in the streamflow: one event is by a team and the other by
    the opposing team. This can create
    """
    filtered_events, index, prev_event = [], 0, {'teamId': -1}
    
    while index < len(events) - 1:
        current_event, next_event = events[index], events[index + 1]
        
        # if it is a duel
        if current_event['eventId'] == DUEL: 
            
            if current_event['teamId'] == prev_event['teamId']:
                filtered_events.append(current_event)
            else:
                filtered_events.append(next_event)
            index += 1
            
        else:
            # if it is not a duel, just add the event to the list
            filtered_events.append(current_event)
            prev_event = current_event
            
        index += 1
    return filtered_events

def is_interruption(event, current_half):
    """
    Verify whether or not an event is a game interruption. A game interruption can be due to
    a ball out of the field, a whistle by the referee, a fouls, an offside, the end of the
    first half or the end of the game.
    
    Parameters
    ----------
    event: dict
        a dictionary describing the event
        
    current_half: str
        the current half of the match (1H = first half, 2H == second half)
        
    Returns
    -------
    True is the event is an interruption
    False otherwise
    """
    event_id, match_period = event['eventId'], event['matchPeriod']
    if event_id in [INTERRUPTION, FOUL, OFFSIDE] or match_period != current_half or event_id == -1:
        return True
    return False

def is_pass(event):
    return event['eventId'] == PASS

def is_accurate_pass(event):
    return ACCURATE_PASS in [tag['id'] for tag in event['tags']]

def is_shot(event):
    """
    Verify whether or not the event is a shot. Sometimes, a play action can continue
    after a shot if the team gains again the ball. We account for this case by looking
    at the next events of the game.
    
    Parameters
    ----------
    event: dict
        a dictionary describing the event
        
    Returns
    -------
    True is the event is a shot
    False otherwise
    """
    event_id = event['eventId']
    return event_id == 10
        
    
def is_save_attempt(event):
    return event['subEventId'] == SAVE_ATTEMPT

def is_reflexes(event):
    return event['subEventId'] == REFLEXES

def is_touch(event):
    return event['subEventId'] == TOUCH

def is_duel(event):
    return event['eventId'] == DUEL

def is_ball_lost(event, previous_event):
    #if DANGEROUS_BALL_LOST in tags or MISSED_BALL in tags:
    #    return True
    #if event['eventName'] == PASS:
    #    if 'Not accurate' in tags:
    #        return True
    if event['teamId'] != previous_event['teamId'] and previous_event['teamId'] != -2 and event['eventName'] != 1:
        return True
    
    return False

def is_penalty(event):
    return event['subEventId'] == PENALTY

def get_play_actions(db, match_id, verbose=False):
    """
    Given a list of events and a match_id to select events of a single match,
    it splits the events
    into possession phases using the following principle:
    
    - an action begins when a team gains ball possession
    - an action ends if one of two cases occurs:
    -- there is interruption of the match, due to: 1) end of first half or match; 2) ball 
    out of the field 3) offside 4) foul
    -- ball is played by the opposite team w.r.t. to the team who is owning the ball
    
    
    """
    try:
            
        events = list(filter(lambda x: x['matchId'] == match_id and x['matchPeriod'] in ['1H', '2H'],db))
        half_offset = {'2H' : max([x['eventSec'] for x in events if x['matchPeriod']=='1H']),
                      '1H':0}
        events = sorted(events, key = lambda x: x['eventSec'] + half_offset[x['matchPeriod']])
        first_secondhalf_evt = sorted(filter(lambda x: x['matchPeriod'] == '2H',events), key = lambda x: x['eventSec'])[0]
        ## add a fake event representing the start and end of the game and the second half start
        events.insert(0, START_OF_GAME_EVENT)
        events.insert(events.index(first_secondhalf_evt),START_2ND_HALF)
        events.append(END_OF_GAME_EVENT)

        play_actions = []

        time, index, current_action, current_half = 0.0, 1, [], '1H'
        previous_event = events[0]
        
        while index < len(events) - 2:

            current_event = events[index]
            # if the action stops by an game interruption
            if is_interruption(current_event, current_half):
                if current_event['eventId'] not in [-1,-2]: #delete fake events
                  current_action.append(current_event)
                play_actions.append(('interruption', current_action))
                current_action = []

            elif is_penalty(current_event):
                next_event = events[index + 1]

                if is_save_attempt(next_event) or is_reflexes(next_event):
                    index += 1
                    current_action.append(current_event)
                    
                    play_actions.append(('penalty', current_action))
                    current_action = []
                else:
                    current_action.append(current_event)

            elif is_shot(current_event):
                next_event = events[index + 1]

                if is_interruption(next_event, current_half):
                    index += 1
                    current_action.append(current_event)
                    if current_event['eventId'] not in [-1,-2]: #delete fake events
                      current_action.append(next_event)
                    play_actions.append(('shot', current_action))
                    current_action = []

                ## IF THERE IS A SAVE ATTEMPT OR REFLEXES; GO TOGETHER
                elif is_save_attempt(next_event) or is_reflexes(next_event):
                    index += 1
                    current_action.append(current_event)
                    current_action.append(next_event)
                    play_actions.append(('shot', current_action))
                    current_action = []

                else:
                    current_action.append(current_event)
                    play_actions.append(('shot', current_action))
                    current_action = []

            elif is_ball_lost(current_event, previous_event):

                current_action.append(current_event)
                play_actions.append(('ball lost', current_action))
                current_action = [current_event]

            else:
                current_action.append(current_event)

            time = current_event['eventSec']
            current_half = current_event['matchPeriod']
            index += 1

            if not is_duel(current_event):
                previous_event = current_event

        events.remove(START_OF_GAME_EVENT)
        events.remove(END_OF_GAME_EVENT)
        events.remove(START_2ND_HALF)

        return play_actions
    except TypeError:
        
        return []
    

def avg_LenPass(match_id = 2058017):
    '''
    This function uses the 'get_play_actions' function; it returns the average passes length (in mt) of a team 
    in a match of interest, in term of euclidean distance.
    
    Parameters
    ----------
    match_id: int
        the match of interest
        
    Returns
    -------
    pandas DataFrame with the average passes length (in mt) of a team in the match of interest
    
    '''
    phase = get_play_actions(eventsM.find(), match_id,verbose=False) # put 'eventsF.find()' with the female dataset
    
    from scipy.spatial.distance import euclidean

    def pass_length(phase):

        """
        average length of all the passes within the phase
        """
        pass_l = []
        for e in phase[1]:
            if e['eventId'] == 8 and len(e['positions'])>1:
                pass_l.append(euclidean([e['positions'][0]['x']*110/100,e['positions'][0]['y']*65/100],[e['positions'][1]['x']*110/100,e['positions'][1]['y']*65/100]))
        
  
        if len(pass_l)>0:
            return np.mean(pass_l),max(pass_l),e['matchId'],e['teamId'] # take the average pass length in that particular action of a game; add the matchId and the teamId
        else:
            pass
    
    len_pa = []
    for i in range(len(phase)):
        p = pass_length(phase[i])
        len_pa.append(p)
    
    avgP = pd.DataFrame(len_pa, columns = ['avgLenPass(mt)','maxLen','match','team'])
    avgP = avgP.groupby(by=['team','match'], as_index=False)['avgLenPass(mt)'].mean()
    
    return avgP





def avg_LenPass_F(match_id = 2829616):
    '''
    This function uses the 'get_play_actions' function; it returns the average passes length (in mt) of a team 
    in a match of interest, in term of euclidean distance.
    
    Parameters
    ----------
    match_id: int
        the match of interest
        
    Returns
    -------
    pandas DataFrame with the average passes length (in mt) of a team in the match of interest
    
    '''
    phase = get_play_actions(eventsF.find(), match_id,verbose=False) 
    
    from scipy.spatial.distance import euclidean

    def pass_length(phase):

        """
        average length of all the passes within the phase
        """
        pass_l = []
        for e in phase[1]:
            if e['eventId'] == 8 and len(e['positions'])>1:
                pass_l.append(euclidean([e['positions'][0]['x']*110/100,e['positions'][0]['y']*65/100],[e['positions'][1]['x']*110/100,e['positions'][1]['y']*65/100]))
        
  
        if len(pass_l)>0:
            return np.mean(pass_l),max(pass_l),e['matchId'],e['teamId'] # take the average pass length in that particular action of a game; add the matchId and the teamId
        else:
            pass
    
    len_pa = []
    for i in range(len(phase)):
        p = pass_length(phase[i])
        len_pa.append(p)
    
    avgP = pd.DataFrame(len_pa, columns = ['avgLenPass(mt)','maxLen','match','team'])
    avgP = avgP.groupby(by=['team','match'], as_index=False)['avgLenPass(mt)'].mean()
    
    return avgP




def avg_pass_rec_time(match_id = 2058017):
    ''' 
    This function uses the 'get_play_actions' function; it returns the average time (in seconds) elpased between 
    two consecutive passes produced by a team in a certain match of interest and how long before the same team regains the ball.
    
    Parameters
    ----------
    match_id: int
        the match of interest
        
    Returns
    -------
    pandas DataFrame with the average passing time value for each team in the match of interest 
    '''
    phase = get_play_actions(eventsM.find(), match_id,verbose=False) # put 'eventsF.find()' when using the female dataset
    
    """
    From the action split output, extract the only features needed
    """
    team_time = []

    for i in range(len(phase)):
        for j in range(len(phase[i][1])):
            try: 
                    team_time.append([phase[i][1][j]['matchId'],phase[i][1][j]['teamId'],phase[i][1][j+1]['teamId'],phase[i][1][j]['subEventName'],phase[i][1][j+1]['subEventName'],phase[i][1][j]['eventSec'],phase[i][1][j+1]['eventSec'],phase[i][1][j]['playerId'],phase[i][1][j+1]['playerId'],phase[i][1][j]['tags'],phase[i][1][j+1]['tags'],phase[i][1][j]['matchPeriod'],phase[i][1][j]['eventName'],phase[i][1][j+1]['eventName']])      
            except:
                pass
    times = pd.DataFrame(team_time, columns=['match','team', 'checkTeam','prevSubEv','nextSubEv','prevTime','nextTime','prevPlr','nextPlr','prevTags','nextTags','period','prevEv','nextEv'])
    
    '''
    Every time the receiver of a pass is the same that send the next pass, the function below 
    measures the time elpased between the two consecutive passes.
    '''
    diff = []

    for i,row in times[times['prevPlr']==times['nextPlr'].shift(1)].iterrows():
            if row['prevEv'] == 'Pass':
                diff.append([row['nextTime'] - row['prevTime'], row['team'], row['match'], row['prevTime'], row['nextTime'], row['period']]) 
    
    diff_ = pd.DataFrame(diff, columns = ['passTime', 'team', 'match', 'prevTime', 'nextTime','period'])
    avg_diff = diff_.groupby(by=['team'], as_index=False)['passTime'].mean()
    
    recoveryTime = []

    for i,row in diff_[diff_['team']!=diff_['team'].shift(1)].iterrows():
        recoveryTime.append([row['prevTime'], row['team'], row['period']])
        
    recTime = pd.DataFrame(recoveryTime, columns=['time', 'team', 'period'])
    recTime['recoveryTime'] = recTime.groupby(by=['period'])['time'].diff().fillna(0)
    avg_rec = recTime.groupby(by=['team'], as_index=False)['recoveryTime'].mean()
    
    df = pd.merge(avg_diff, avg_rec, on = ['team'], how='outer')

    return df




def avg_pass_rec_time_F(match_id = 2829616):
    ''' 
    Same of the 'get_play_actions' function, but now using the female data set.
        
    Parameters
    ----------
    match_id: int
        the match of interest
        
    Returns
    -------
    pandas DataFrame with the average passing time value for each team in the match of interest 
    '''
    phase = get_play_actions(eventsF.find(), match_id,verbose=False) 
    
    """
    From the action split output, extract the only features needed
    """
    team_time = []

    for i in range(len(phase)):
        for j in range(len(phase[i][1])):
            try: 
                    team_time.append([phase[i][1][j]['matchId'],phase[i][1][j]['teamId'],phase[i][1][j+1]['teamId'],phase[i][1][j]['subEventName'],phase[i][1][j+1]['subEventName'],phase[i][1][j]['eventSec'],phase[i][1][j+1]['eventSec'],phase[i][1][j]['playerId'],phase[i][1][j+1]['playerId'],phase[i][1][j]['tags'],phase[i][1][j+1]['tags'],phase[i][1][j]['matchPeriod'],phase[i][1][j]['eventName'],phase[i][1][j+1]['eventName']])      
            except:
                pass
    times = pd.DataFrame(team_time, columns=['match','team', 'checkTeam','prevSubEv','nextSubEv','prevTime','nextTime','prevPlr','nextPlr','prevTags','nextTags','period','prevEv','nextEv'])
    
    '''
    Every time the receiver of a pass is the same that send the next pass, the function below 
    measures the time elpased between the two consecutive passes.
    '''
    diff = []

    for i,row in times[times['prevPlr']==times['nextPlr'].shift(1)].iterrows():
            if row['prevEv'] == 'Pass':
                diff.append([row['nextTime'] - row['prevTime'], row['team'], row['match'], row['prevTime'], row['nextTime'], row['period']]) 
    
    diff_ = pd.DataFrame(diff, columns = ['passTime', 'team', 'match', 'prevTime', 'nextTime','period'])
    avg_diff = diff_.groupby(by=['team'], as_index=False)['passTime'].mean()
    
    recoveryTime = []

    for i,row in diff_[diff_['team']!=diff_['team'].shift(1)].iterrows():
        recoveryTime.append([row['prevTime'], row['team'], row['period']])
        
    recTime = pd.DataFrame(recoveryTime, columns=['time', 'team', 'period'])
    recTime['recoveryTime'] = recTime.groupby(by=['period'])['time'].diff().fillna(0)
    avg_rec = recTime.groupby(by=['team'], as_index=False)['recoveryTime'].mean()
    
    df = pd.merge(avg_diff, avg_rec, on = ['team'], how='outer')

    return df



def avg_duel_shot_time(match_id = 2058017):
    '''
    This function uses the 'get_play_actions' function; it returns the average time (in seconds) elapsed between the same team
    does two consecutive duel and shot events in a match of interest.
    
    Parameters
    ----------
    match_id: int
        the match of interest
        
    Returns
    -------
    pandas DataFrame with the average time (in seconds) between two duels and shots for each team in the match of interest 
    '''
    phase = get_play_actions(eventsM.find(), match_id,verbose=False) # put 'eventsF.find()' when using the female dataset
    
    """
    From the action split output, extract the only features needed
    """
    team_time = []

    for i in range(len(phase)):
        for j in range(len(phase[i][1])):
            try: 
                    team_time.append([phase[i][1][j]['matchId'],phase[i][1][j]['teamId'],phase[i][1][j+1]['teamId'],phase[i][1][j]['subEventName'],phase[i][1][j+1]['subEventName'],phase[i][1][j]['eventSec'],phase[i][1][j+1]['eventSec'],phase[i][1][j]['playerId'],phase[i][1][j+1]['playerId'],phase[i][1][j]['tags'],phase[i][1][j+1]['tags'],phase[i][1][j]['matchPeriod'],phase[i][1][j]['eventName'],phase[i][1][j+1]['eventName']])      
            except:
                pass
    times = pd.DataFrame(team_time, columns=['match','team', 'checkTeam','prevSubEv','nextSubEv','prevTime','nextTime','prevPlr','nextPlr','prevTags','nextTags','period','prevEv','nextEv'])
    #----
    
    duel = []
    shot = [] 

    for i,row in times.iterrows():
        if all([row['prevEv']=='Duel', row['nextEv']!='Duel']):
            duel.append([row['prevTime'], row['team'],row['prevSubEv'], row['period'],match_id])
    for i,row in times.iterrows():
        if row['nextEv']=='Shot':
            shot.append([row['nextTime'],row['checkTeam'],row['nextSubEv'], row['period'],match_id])
    
    duels = pd.DataFrame(duel, columns=['time', 'ContrastTeam', 'event','period','match'])
    shots = pd.DataFrame(shot, columns=['time', 'ContrastTeam', 'event','period','match'])
    
    duels['difDuel'] = duels.groupby(['ContrastTeam','period'])['time'].diff().fillna(0) # how much time elapsed between the same team does another 'duel' 
    duels = duels.groupby(by=['ContrastTeam','match'],as_index=False)['difDuel'].mean()

    shots['difShot'] = shots.groupby(['ContrastTeam','period'])['time'].diff().fillna(0) # how much time elapsed between the same team does another 'shot' 
    shots = shots.groupby(by=['ContrastTeam','match'],as_index=False)['difShot'].mean()
    
    duel_shot_df = pd.merge(duels, shots, on = ['ContrastTeam','match'], how='outer')
    
    return duel_shot_df




def avg_duel_shot_time_F(match_id = 2829616):
    '''
    Same of the 'avg_duel_shot_time' function, but now using the female data set.
    
    Parameters
    ----------
    match_id: int
        the match of interest
        
    Returns
    -------
    pandas DataFrame with the average time (in seconds) between two duels and shots for each team in the match of interest 
    '''
    phase = get_play_actions(eventsF.find(), match_id,verbose=False) 
    
    """
    From the action split output, extract the only features needed
    """
    team_time = []

    for i in range(len(phase)):
        for j in range(len(phase[i][1])):
            try: 
                    team_time.append([phase[i][1][j]['matchId'],phase[i][1][j]['teamId'],phase[i][1][j+1]['teamId'],phase[i][1][j]['subEventName'],phase[i][1][j+1]['subEventName'],phase[i][1][j]['eventSec'],phase[i][1][j+1]['eventSec'],phase[i][1][j]['playerId'],phase[i][1][j+1]['playerId'],phase[i][1][j]['tags'],phase[i][1][j+1]['tags'],phase[i][1][j]['matchPeriod'],phase[i][1][j]['eventName'],phase[i][1][j+1]['eventName']])      
            except:
                pass
    times = pd.DataFrame(team_time, columns=['match','team', 'checkTeam','prevSubEv','nextSubEv','prevTime','nextTime','prevPlr','nextPlr','prevTags','nextTags','period','prevEv','nextEv'])
    #----
    
    duel = []
    shot = [] 

    for i,row in times.iterrows():
        if all([row['prevEv']=='Duel', row['nextEv']!='Duel']):
            duel.append([row['prevTime'], row['team'],row['prevSubEv'], row['period'],match_id])
    for i,row in times.iterrows():
        if row['nextEv']=='Shot':
            shot.append([row['nextTime'],row['checkTeam'],row['nextSubEv'], row['period'],match_id])
    
    duels = pd.DataFrame(duel, columns=['time', 'ContrastTeam', 'event','period','match'])
    shots = pd.DataFrame(shot, columns=['time', 'ContrastTeam', 'event','period','match'])
    
    duels['difDuel'] = duels.groupby(['ContrastTeam','period'])['time'].diff().fillna(0) # how much time elapsed between the same team does another 'duel' 
    duels = duels.groupby(by=['ContrastTeam','match'],as_index=False)['difDuel'].mean()

    shots['difShot'] = shots.groupby(['ContrastTeam','period'])['time'].diff().fillna(0) # how much time elapsed between the same team does another 'shot' 
    shots = shots.groupby(by=['ContrastTeam','match'],as_index=False)['difShot'].mean()
    
    duel_shot_df = pd.merge(duels, shots, on = ['ContrastTeam','match'], how='outer')
    
    return duel_shot_df




'''NB just for the male World Cup matches!'''

def stop_time(match_id = 2058017):
    '''
    This function uses the 'get_play_actions' function; it returns the average time (in seconds) that a team stand stopped
    in a certain match of interest. It has been recorded the time elapsed: 1) between a foul and the next free kick; 
    2) between an Offside and the next free kick; 3) since an event occurrance that leads to a throw in; 4) since an event occurrance
    that leads to a goal kick; 5) since an event occurrance that leads to a Corner.
    
    Parameters
    ----------
    match_id: int
        the match of interest
        
    Returns
    -------
    pandas DataFrame with the average 'stop_time' value for each team in the match of interest 
    '''
    phases = get_play_actions(eventsM.find(), match_id, verbose=False) 
    
    stop = []
    for i in range(len(phases)):
        for j in range(len(phases[i][1])):
            try:
                if phases[i][1][j]['subEventName']  == 'Foul':
                    stop.append([phases[i][1][j]['eventSec'], phases[i+1][1][0]['eventSec'], phases[i][1][j]['teamId'],phases[i+1][1][0]['teamId'],phases[i][1][j]['matchPeriod']])   
            except:
                pass
            
            try:
                if phases[i][1][j+1]['subEventName']  == 'Throw in':
                    stop.append([phases[i][1][j]['eventSec'], phases[i][1][j+1]['eventSec'], phases[i][1][j]['teamId'],phases[i][1][j+1]['teamId'],phases[i][1][j]['matchPeriod'],phases[i][1][j]['subEventName']])
            except:
                pass 
        
            try:
                if phases[i][1][j+1]['subEventName']  == 'Goal kick':
                    stop.append([phases[i][1][j]['eventSec'], phases[i][1][j+1]['eventSec'], phases[i][1][j]['teamId'],phases[i][1][j+1]['teamId'],phases[i][1][j]['matchPeriod'],phases[i][1][j]['subEventName']])
            except:
                pass
        
            try:
                if phases[i][1][j+1]['subEventName']  == 'Corner':
                    stop2.append([phases[i][1][j]['eventSec'], phases[i][1][j+1]['eventSec'], phases[i][1][j]['teamId'],phases[i][1][j+1]['teamId'],phases[i][1][j]['matchPeriod'],phases[i][1][j]['subEventName']])
            except:
                pass
        
            try:
                if phases[i][1][j]['eventName']  == 'Offside':
                    stop.append([phases[i][1][j]['eventSec'], phases[i+1][1][0]['eventSec'], phases[i][1][j]['teamId'],phases[i+1][1][0]['teamId'],phases[i][1][j]['matchPeriod'],phases[i][1][j]['eventName']])
            except:
                pass
    
    df_phases = pd.DataFrame(stop, columns=['prevTime', 'nextTime','actTeam','beneficiaryTeam','period','preEvent'])
    df_phases['stopTime'] = df_phases['nextTime'] - df_phases['prevTime'] 
    
    df_phases = df_phases.groupby(by=['beneficiaryTeam'])['stopTime'].mean().reset_index(name = 'avg_stopTime')
    df_phases['match'] = match_id
    
    return df_phases   





def stop_timef(match_id = 2826535):
    '''
    This function generates the same output of the 'stop_time()' function, but it is adapted to the female data set, which
    contains the 'interruption' event that makes the algorithm easier.

    '''
    phases = get_play_actions(eventsF.find(), match_id, verbose=False) 
    
    stop = []
    for i in range(len(phases)):
        for j in range(len(phases[i][1])):
            try:
                if phases[i][1][j]['subEventName']  == 'Foul':
                    stop.append([phases[i][1][j]['eventSec'], phases[i+1][1][0]['eventSec'], phases[i][1][j]['teamId'],phases[i+1][1][0]['teamId'],phases[i][1][j]['matchPeriod'],phases[i][1][j]['subEventName'],phases[i+1][1][0]['eventName']])           
            except:
                pass
            
            try:
                if phases[i][1][j]['subEventName']  == 'Ball out of the field':
                    stop.append([phases[i][1][j]['eventSec'], phases[i+1][1][0]['eventSec'], phases[i][1][j]['teamId'],phases[i+1][1][0]['teamId'],phases[i+1][1][0]['matchPeriod'],phases[i][1][j]['subEventName'],phases[i+1][1][0]['subEventName']])
            except:
                pass
    
    
    df_phases = pd.DataFrame(stop, columns=['prevTime', 'nextTime','actTeam','beneficiaryTeam','period','prevEvent', 'nextEvent'])
    df_phases['stopTime'] = df_phases['nextTime'] - df_phases['prevTime'] 
    
    df_phases = df_phases.groupby(by=['beneficiaryTeam'])['stopTime'].mean().reset_index(name = 'avg_stopTime')
    df_phases['match'] = match_id
    
    return df_phases  