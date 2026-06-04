from SpotiFLAC.core.models import TrackMetadata
import inspect

# Questo stampa quali sono i campi richiesti dal modello
print("Campi richiesti da TrackMetadata:")
print(TrackMetadata.model_fields.keys())

# Proviamo a creare l'oggetto
try:
    meta = TrackMetadata(
        id="test",
        title="Numb",
        artists="Linkin Park",
        album="Meteora",
        album_artist="Linkin Park",
        isrc="USWB10300185",
        duration_ms=187000
    )
    print("\nInizializzazione riuscita!")
except Exception as e:
    print(f"\nErrore di inizializzazione: {e}")