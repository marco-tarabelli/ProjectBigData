FROM python:3.8

ADD nuovaprova.py .

RUN  pip install kafka-python

# Avvia il tuo script Python quando l'immagine viene eseguita
CMD ["python", "nuovaprova.py"]
