this should be a go service which connects via websocket to a bob node, you will find interface for
communicating with bob in the bob service at /Users/alexlucaci/Workspace/qubicbob
the service needs to connect to bob and listen for events of type qu_transfer, asset_issuance, asset_ownership_change and asset_possession_change
if bob or this service crashes it should be able to restart and continue listening for events from the last tick and/or event/logid it stopped processing
the events will be stored internally in a pebble db, but make sure that we create separate pebble dbs for each epoch as events having the following structure:
each event is part of a tick, and each tick is part of an epoch so when we are getting events for the next epoch we need to create another db
all epoch dbs should be stored in different folders for easy management and their reference should always be in memory
the pebble keys should have the following format: <tick_number>:<event_id>
in terms of exposing the information for the clients, we would use a grpc with grpc-rest-gateway, you can find reference folder/code structure implementation for grpc/handlers in our other similar services: /Users/alexlucaci/Workspace/qubic-archive-query-service/v2
we would want to expose the following endpoints:
/getStatus -> returns the tick intervals grouped by epochs for which the events were stored
/getEventsForTick -> returns all events for a given tick