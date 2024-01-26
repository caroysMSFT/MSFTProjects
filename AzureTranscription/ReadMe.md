This project is for using Azure cognitive services to automatically generate subtitles for a video, and then bake them in.

There's 2 main scripts for this:

`VideoTools.psm` - A collection of helper functions for manipulating the video/audio streams (mostly using VLC as our workhorse)
`AzureSpeechToText.ps1` - The script which sends the payloads to Azure for processing and handles the output

This works in the following way:

1. Separate the audio stream from the video
2. Upload to Azure Storage
3. Pull down the resulting translations
4. Take the first "guess" for each "sentence"
5. Write it out to an *.srt file
6. Manually merge the subtitle file into the resulting video container
