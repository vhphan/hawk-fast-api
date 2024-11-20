# How to start this app
## using gunicorn
```bash
gunicorn -c gunicorn_config.py main:app --daemon
```
The settings for gunicorn are in the `gunicorn_config.py` file.

### using passenger standalone
```bash
passenger start
```
The settings for passenger are in the `Passengerfile.json` file.