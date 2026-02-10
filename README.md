<h1>SpotiFLAC Python Module</h1>
<p>Integrate SpotiFLAC directly into your Python projects. Perfect for building custom telegram bots, automation tools, or web interfaces. <br> Also available as a <a href="https://github.com/jelte1/SpotiFLAC-Command-Line-Interface">CLI tool</a> and <a href="https://github.com/afkarxyz/SpotiFLAC">GUI tool</a>.</p>
<a href="https://pypi.org/project/SpotiFLAC">Module in Pypi</a>
<h2>Function Signature</h2>
<pre>
from SpotiFLAC import SpotiFLAC

SpotiFLAC(
    url: str,
    output_dir: str,
    services: list = ["tidal", "deezer", "qobuz", "amazon"],
    filename_format: str = "{title} - {artist}",
    use_track_numbers: bool = False,
    use_artist_subfolders: bool = False,
    use_album_subfolders: bool = False,
    loop: int = None
)
</pre>

<h2>Parameters</h2>
<i>url (str)</i><br>
The Spotify URL (Track, Album, or Playlist) you want to download.<br><br>

<i>output_dir (str)</i><br>
The directory path where the files will be saved.<br><br>

<i>services (list)</i><br>
A list of strings specifying which services to use and the priority order. <br>
Options: <code>"tidal"</code>, <code>"qobuz"</code>, <code>"deezer"</code>, <code>"amazon"</code>.<br>
Example: <code>["qobuz", "tidal"]</code>.<br><br>

<i>filename_format (str)</i><br>
Specify the format for naming downloaded files. Available placeholders:<br>
<code>{title}, {artist}, {album}, {track}, {date}, {year}, {position}, {isrc}, {duration}</code>.<br>
Default is <code>"{title} - {artist}"</code>.<br><br>

<i>use_artist_subfolders (bool)</i><br>
Set to <code>True</code> to organize downloaded files into subfolders by artist.<br><br>

<i>use_album_subfolders (bool)</i><br>
Set to <code>True</code> to organize downloaded files into subfolders by album.<br><br>

<i>loop (int)</i><br>
Specify the duration in minutes to keep retrying downloads in case of failures. Default is <code>None</code> (no retries).<br>

<h3>Example usage:</h3>
<pre>
from SpotiFLAC import SpotiFLAC

# Simple Download
SpotiFLAC(
    url="https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT",
    output_dir="./downloads"
)

# Advanced Configuration
SpotiFLAC(
    url="https://open.spotify.com/album/41MnTivkwTO3UUJ8DrqEJJ",
    output_dir="./MusicLibrary",
    services=["qobuz", "amazon", "tidal"],
    filename_format="{year} - {album}/{track}. {title}",
    use_artist_subfolders=True,
    use_album_subfolders=True
)
</pre>