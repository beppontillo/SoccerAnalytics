### The THESIS_NOTEBOOK contains all the codes and the functions used in order to define the *technical features* of a football team and to compute the *machine learning classifiers* used to classify a team in a game as male or female.

#### How to use it:

- **1 STEP**: download the *soccer-logs* data of interest, collected from https://www.nature.com/articles/s41597-019-0247-7, into a folder (which we call *data*, for example);

- **2 STEP**: use *data_import.sh* file into the *Command prompt* to create the *MongoDB* **dati** database, where I storage the usefull data;

- **3 STEP**: obtain the *PlayeRank* functions using *git clone https://github.com/mesosbrodleto/playerank.git*; **NOTE** before computing the *players' ratings* in SECTION 6.3 of the THESIS_NOTEBOOK, you first need to define the *feature_weights.json* and the *role_matrix.json* files. These files are obtained by launching the codes at the *beginning* of SECTION 6.3 of the THESIS_NOTEBOOK and they use the information of *events*, *players* and *matches* into the *data* folder. 

After you do these preliminar phases, you can use the THESIS_NOTEBOOK!

This work uses data related to the Fifa Men's World Cup 2018, with 101,759 *events* from the 64 games played and 736 players, and to the Fifa Women’s World Cup 2019, with 71,636 *events* on the 44 matches available and 546 players.  

Again, all the data are storage in a **MongoDB** database which I called **dati** and they are presented in *json* format, i.e., characterized by *collections* and *documents*. 
A *collection* is characterized by *documents* that describe: a World Cup game, a team, a player or a soccer event, i.e., a certain action, such as a pass, a shot, a foul, a save attempt, and so on, made by a team's player in a match.


**NOTE** the **dati** database it's been created by me and contains all the usefull collections; they were imported through the 'mongoimport' command in the *Command prompt*. See *data_import.sh* file to create the **dati** database and import all the usefull *json* files/collections.


The **dati** database contains the following *collections*:
- teams (male football teams)
- teamsW (female football teams)
- players (male players)
- playersW (female players)
- eventsWC (events in the Russia 2018 World Cup championship)
- eventsWCW (events in the France 2019 World Cup championship)
- matchesWC (matches in the Russia 2018 World Cup championship)
- matchesWCW (matches in the France 2019 World Cup championship)


A document describing a **match**, for example, presents the identities of the two challenging teams through the variable *teamsData*, which contains the variable *formation* (list of sub-documents) with the description of the players on the bench, on the lineup and information on any substitutions. 
For each player the following features are described: *playerId*, *assists* (dummy), *goals*, *ownGoals* (dummy), *redCards* (dummy) e *yellowCards* (dummy). Finally, the document presents generic data on the match and its outcome (*winner*, *date*, *referees*, etc...).

Of particular importance are the documents describing an *event*, such as:

{'_id': ObjectId('5d8a3d05f89835179f701044'),
 'eventId': 8,
 'subEventName': 'High pass',
 'tags': [{'id': 1801}],
 'playerId': 139393,
 'positions': [{'y': 53, 'x': 35}, {'y': 19, 'x': 75}],
 'matchId': 2057954,
 'eventName': 'Pass',
 'teamId': 16521,
 'matchPeriod': '1H',
 'eventSec': 4.487814,
 'subEventId': 83,
 'id': 258612106}

Which describes a *High pass* made by the Arab player Abdullah Ibrahim Otayf during the game "Russia - Saudi Arabia", after about 4 seconds from the kick off.  

We decide to use **Python** as **MongoDB** driver, via the *pymongo* package. All the collections into **dati** are renamed as teamsM, teamsF, playersM, playersF, eventsM, eventsF, matchesM and matchesF, respectively.

**All** the function we use to create the different *technical features* of a football team are in the *.py* files: (i) match_intensity.py (ii) H_indicator.py (iii) flow_centrality.py . 

---

In Section <font color="red">6.3</font> we quantify the *performance ratings* of each player in a certain game, and then we aggregate them via **mean** and **standard deviation**, for each national team. The *ratings* are computed using the **PlayeRank** framework, taken from:

- Playerank -- Pappalardo, L., Cintia, P., Ferragina, P., Massucco, E., Pedreschi, D., & Giannotti, F. (2019). PlayeRank: data-driven performance evaluation and player ranking in soccer via a machine learning approach. ACM Transactions on Intelligent Systems and Technology (TIST), 10(5), 59.

- Public dataset -- Luca Pappalardo, Paolo Cintia, Alessio Rossi, Emanuele Massucco, Paolo Ferragina, Dino Pedreschi & Fosca Giannotti (2019). Nature Scientic Data.

To implement it, you **need** to use *json* files and to compute the *features' **weights*** and the **role_matrix** as descripted in the articles. They also need to be in *json* format. The **weights** are estimated via *SVM* and they indicate the *importance* of every *event*(in the winning goal) that occurred in a football match. While, they use a *k-means* algorithm to detect the **player roles**, based on the *average position* of them *with respect to their mates* on the field (center of performance).

To compute team players' ratings of a certain soccer league or tournament, you need the *json* files describing **matches**, **events** and **players** of that competition; for example, we use *matchesWcup_Women*, *eventsWcup_Women* and *playersF* to compute the performance ratings of the female players who have played in the France 2019 World Cup.  

Finally, we aggregate *ratings* via **mean** and **standard deviation**, for each national team and for every game. 
*Ratings' standard deviation* of a team quantifies **how important** the **indivual performance** of a player affects the **team collective performance**; for example in the match Portugal vs Spain (3-3, id=2057960), the *ratings'* standard deviation of Portugal is quite high, Rstd=0.22, due to the great *individual* performance of C. Ronaldo that scored 3 goals.  

---

In Chapter <font color=orange>II</font>, we implement the machine learning classifiers to classify a football team as male (*class 0*) or female (*class 1*), in a certain game. We use the models: Logistic classifier, Decision Tree, Random Forest and Adaboost classifier (and also a Baseline which assign always the most frequent class, i.e., male team). We carry out two different experiments:

- in the *first*, we select the *tuning parameters* of each method through the 'GridSearchCV' function, taken from the *sklearn* library, on the 20% of the total observations available, randomly drawn. There are chosen the parameters that *maximaze* the 5-fold CV accuracy. After, we validate the models via 10-fold CV, computing the *accuracy* and *F1-score*, on the remaining 80%.

**NOTE** Due to a *random component* present in each classifier, the choice of the best parameters may sometimes slightly differ from the choice of parameters presented in the notebook.

- in the *second* experiment, we split again the *training* set into *another training* set (70%), which we call *fitting* set, and into a *test* set (30%). This split is repeated *N times* (twenty times in our case) based on different *random state* values, and we fit the Decision Tree with the *best parameters* on the *fitting* set each time. Every time the Decision Tree is fitted, we take the *feature importance* measure for *each* variable, and we measure the Normalized Root-Mean-Square Error and the Kendall's τ coefficient between each pair of trees, to stabilize on *which* features the Decision Tree focuses on the most to classify a team in a game as male (*class 0*) or female (*class 1*).

**NOTE** If you want to focus on just a *subset* of variables, you can put *your list* of variables of interest instead of 'dref', and compute these two indices to evaluate the grade of concordance between every pair of trees, about the importance of *that specific variables* in the classification task. For example, we use the features: *ratings'* standard deviation, average pass velocity, ball possession recovery time and percentage of accurate passes.



