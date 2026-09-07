# Railway backend — DXY Gold Paper Traders

This is the server-side foundation for the mobile Netlify dashboard.

## Railway setup from phone

1. Put this folder in a GitHub repository.
2. Railway → New Project → Deploy from GitHub repo.
3. Select the repository.
4. Railway detects the Dockerfile automatically.
5. Service → Variables → add:
   TWELVE_DATA_API_KEY = your new Twelve Data key
   START_BALANCE = 10000
   TD_SYMBOLS = XAU/USD,DXY
6. Deploy.
7. Settings → Networking → Generate Domain.
8. Open `https://YOUR-DOMAIN/health`.

The API key is read only from the Railway environment and is not stored in the frontend.

IMPORTANT: This backend is PAPER ONLY. It does not send broker orders.
