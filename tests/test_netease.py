from SpotiFLAC.providers.netease import NeteaseProvider
from SpotiFLAC.core.models import TrackMetadata

# Inizializza il provider
provider = NeteaseProvider()

# Crea metadati di prova con tutti i campi obbligatori
meta = TrackMetadata(
    id="test",
    title="Numb",
    artists="Linkin Park",
    album="Meteora",
    album_artist="Linkin Park",  # <--- Questo campo mancava!
    isrc="USWB10300185",        # <--- Aggiunto per evitare errori di validazione
    duration_ms=187000
)

# Esegui il download
result = provider.download_track(meta, output_dir="./test_download")

if result.success:
    print(f"Successo! File salvato in: {result.file_path}")
else:
    print(f"Fallito: {result.error}")
