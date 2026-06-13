# SmartPark Izmir Postman Files

Use these files to test the API layer during a demo.

## Import

1. Open Postman.
2. Import `SmartPark-Izmir.postman_collection.json`.
3. Import `SmartPark-Izmir.postman_environment.json`.
4. Select the `SmartPark Izmir - Local` environment.
5. Start the project:

```bash
docker compose up --build
```

6. Open `http://localhost` once to confirm Traefik and the frontend are reachable.

## Suggested Demo Flow

Run the requests in this order:

1. `01 - Frontend smoke check`
2. `Occupancy State / Get all lots`
3. `Occupancy State / Get nearby lots`
4. `Pricing and Routing / Get recommendations`
5. `Pricing and Routing / Get dynamic price for lot`
6. `Forecasting / Get forecast for lot`
7. `Analytics / Get analytics summary`
8. `Notifications / Get notification status`

The first occupancy or recommendation request automatically saves a usable `lot_id` into the environment.

## Notes

- If `/occupancy` returns an empty array, wait 30-60 seconds and run it again. The ingestion service must fetch data first.
- If `/forecast/{{lot_id}}` returns `404`, wait for a new occupancy event and retry.
- WebSocket updates can be tested manually in Postman with `ws://localhost/ws`.
- The plain `/health` endpoints are used internally by Docker health checks. Through Traefik, use the exposed routes such as `/notifications/health` and `/analytics/health`.
