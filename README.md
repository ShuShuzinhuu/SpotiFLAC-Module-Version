<h1>SpotiFLAC-Command-Line-Interface</h1>
<p>Command Line Interface version of SpotiFLAC.<br> Also available as a python module.</p>
<h2>Arguments</h2>
<i>service {tidal,qobuz,deezer,amazon}</i><br>
Specify the music service to use for downloading FLAC files. Specify multiple services separated by spaces to try them in order. Default is 'tidal'.<br><br>
<i>filename-format {title_artist,artist_title,title_only}</i><br>
Specify the format for naming downloaded files. Default is 'title_artist'.<br><br>
<i>use-track-numbers</i><br>
Include track numbers in the filenames.<br><br>
<i>use-artist-subfolders</i><br>
Organize downloaded files into subfolders by artist.<br><br>
<i>use-album-subfolders</i><br>
Organize downloaded files into subfolders by album.<br><br>
<i>loop minutes</i><br>
Specify the duration in minutes to keep retrying downloads in case of failures. Default is 0 (no retries).<br>

<h2>CLI usage</h2>
<p>Program can be ran by downloading one of the release files.<br>
Program can also be ran by downloading the python files and calling <code>python launcher.py</code> with the arguments.</p>

<h4>Windows:</h4>

```bash
./SpotiFLAC-Windows.exe [url]
                        [output_dir]
                        [--service tidal qobuz deezer amazon]
                        [--filename-format {title_artist,artist_title,title_only}]
                        [--use-track-numbers] [--use-artist-subfolders]
                        [--use-album-subfolders]
                        [--loop minutes]
```

<h4>Linux / Mac:</h4>

```bash
chmod +x SpotiFLAC-Linux
./SpotiFLAC-Linux [url]
                  [output_dir]
                  [--filename-format {title_artist,artist_title,title_only}]
                  [--use-track-numbers] [--use-artist-subfolders]
                  [--use-album-subfolders]
                  [--loop minutes]
```

<h2>Python Module Usage</h2>
<p>The program is now also available as a Python module:</p>

```bash
from SpotiFLAC import SpotiFLAC

SpotiFLAC(
    url,
    output_dir,
    services=["tidal", "deezer", "qobuz", "amazon"],
    filename_format="title_artist",
    use_track_numbers=False,
    use_artist_subfolders=False,
    use_album_subfolders=False,
    loop=None
)
```

<h3>Example</h3>

```bash
from SpotiFLAC import SpotiFLAC

SpotiFLAC(
    url="https://open.spotify.com/album/xyz",
    output_dir="/path/to/output_dir",
    services=["tidal", "deezer"],
    filename_format="artist_title",
    use_track_numbers=True,
    use_artist_subfolders=True,
    use_album_subfolders=True,
    loop=120
)

```
