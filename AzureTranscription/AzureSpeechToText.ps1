
<# powershell module todo:

1. make distinct cmdlets for various functions:

   x Convert audio file to *.wav
   x Convert audio file to transcription text using Azure API
   - rip audio track from video to *.mp3
   - Translate a block of text from/to languages
   - Process a transcription file into a SRT (flags for translation)
   - Merge a SRT into an MP4
   - Merge video files into MP4
   - Convert disparate file types into MP4
   - Convert disparate file types into MP3 - started, needs a one line fix
2. X Read the VLC path out of the registry
   - Make sure all cmdlets fail that can't look up the VLC path
3. X Take parameters for SAS keys, account names, etc.

Gotchas:

1. You can't do this on the Free tier of transcription service.
2. You have to have a supported locale for batch transcription (or if your local is invalid, it will claim it's not supported)
https://learn.microsoft.com/en-us/azure/ai-services/speech-service/language-support?tabs=stt



#>

if($host.Version.Major -le 5)
{
    throw "This script and module doesn't work on lower versions of Powershell`nReason: ConvertTo-Json screws up SAS tokens" 
}


[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$srcfolder = "C:\Projects\Russian LOTR"
. .\VideoTools.ps1

#$profile = Connect-AzAccount -Subscription CaryRoysInternal
#Select-AzSubscription -Name CaryRoysInternal -Context $profile.Context

# API Key
$ocpKey = ""

$storageAccountName = ""
$resourceGroupName = ""

$container = ""



$src = get-item $srcfolder

foreach($file in $src.GetFiles("*.mp4"))
{
    $wavfile = Convertto-WAV -srcfile $file
    $fileobj = get-item -path $wavfile
    
    # upload to storage, get blob path

    Transcribe-Wav -srcfile $wavfile  -ocpKey $ocpKey -resourcegroupname $resourceGroupName -storageAccount $storageAccountName -container $container -upload $false -locale "ru-RU"
}
