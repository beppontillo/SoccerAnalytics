mongoimport --db dati --collection teams --file data\teams.json --jsonArray
mongoimport --db dati --collection teamsW --file data\teamsF.json --jsonArray
mongoimport --db dati --collection players --file data\players.json --jsonArray
mongoimport --db dati --collection playersW --file data\playersF.json --jsonArray
mongoimport --db dati --collection eventsWC --file data\eventsWcup_Men\events_World_Cup.json --jsonArray
mongoimport --db dati --collection eventsWCW --file data\eventsWcup_Women\events_World_Cup.json --jsonArray
mongoimport --db dati --collection matchesWC --file data\matchesWcup_Men\matches_World_Cup.json --jsonArray
mongoimport --db dati --collection matchesWCW --file data\matchesWcup_Women\matches_World_Cup.json --jsonArray

