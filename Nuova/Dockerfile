# Usa un'immagine base Python
FROM python:3.8-slim

# Imposta la directory di lavoro nell'immagine
WORKDIR /app

# Copia il tuo codice Python e i file necessari nella directory di lavoro dell'immagine
COPY nuovaprova.py .
COPY requirements.txt .

# Installa le dipendenze del tuo script Python
RUN pip install --no-cache-dir -r requirements.txt

# Avvia il tuo script Python quando l'immagine viene eseguita
CMD ["python", "nuovaprova.py"]
