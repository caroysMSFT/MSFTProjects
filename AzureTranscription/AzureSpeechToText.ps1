
<# powershell module todo:

1. make distinct cmdlets for various functions:

   x Convert audio file to *.wav
   x Convert audio file to transcription text using Azure API
   - Translate a block of text from/to languages
   - Process a transcription file into a SRT (flags for translation)
   - Merge a SRT into an MP4
   - Merge video files into MP4
   - Convert disparate file types into MP4
   - Convert disparate file types into MP3 - started, needs a one line fix
2. X Read the VLC path out of the registry
   - Make sure all cmdlets fail that can't look up the VLC path
3. X Take parameters for SAS keys, account names, etc.

Computer\HKEY_LOCAL_MACHINE\SOFTWARE\WOW6432Node\VideoLAN\VLC
#>



[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$srcfolder = "C:\Projects\Russian LOTR"
$vlcpath = (get-item -path Registry::HKEY_LOCAL_MACHINE\SOFTWARE\WOW6432Node\VideoLAN\VLC).GetValue("")

$src = get-item $srcfolder

# API Key
$key1 = "dbca1a69b2014d92b2432478d5da6d50"

$storageacct = get-azstorageaccount -resourcegroupname Skunkworks -name skunkworksdiag
$CSASToken = "?sv=2019-02-02&sr=c&ss=bfqt&srt=sco&sp=rwdlacup&se=2025-02-12T11:03:47Z&st=2020-02-12T03:03:47Z&spr=https&sig=TItO6ojwMunOMSnz7NJCk0KrJTmvbryM7CNpV2CpvEg%3D"
$SAStoken = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-12-31T10:59:52Z&st=2021-04-09T01:59:52Z&spr=https&sig=s5sq1JuVTKRylREW6Nu8xpFmnRhmJ936Eql4Mu35GyA%3D"
$serviceandSAS = "https://skunkworksdiag.blob.core.windows.net/?sv=2019-02-02&sr=c&&ss=bfqt&srt=sco&sp=rwdlacup&se=2025-02-12T11:03:47Z&st=2020-02-12T03:03:47Z&spr=https&sig=TItO6ojwMunOMSnz7NJCk0KrJTmvbryM7CNpV2CpvEg%3D"

$container = "https://skunkworksdiag.blob.core.windows.net/audiofiles"
$containerSAS = "$container$CSAStoken"


$Headers = @{
    'Ocp-Apim-Subscription-Key' = $key1;
    'Content-Length' = '0';
    'Content-type' = 'application/x-www-form-urlencoded'
    'Host' = 'centralus.api.cognitive.microsoft.com'
}
$OAuthToken = Invoke-RestMethod -Uri https://centralus.api.cognitive.microsoft.com/sts/v1.0/issueToken -Method Post -Headers $headers

foreach($file in $src.GetFiles("*.mp3"))
{
    
    $vlcparams = " -I dummy -vvv --sout=`"#transcode{vcodec=none,acodec=s16l,ab=128,channels=2,samplerate=16000}:std{access=file,mux=wav,dst='$($file.DirectoryName)\$($file.BaseName).wav'}`" `"$($file.FullName)`" vlc://quit"
    #run vlc to convert to *.wav file...
    Start-Process -FilePath $vlcpath -ArgumentList $vlcparams -Wait

    #file is converted to *.wav.  Do the needful with the REST API...

    # Audio File
    $audiofile = Get-ChildItem "$($file.DirectoryName)\$($file.BaseName).ogg"
    # Read audio into byte array
    $audioBytes = [System.IO.File]::ReadAllBytes($audiofile)



    #upload to blob storage
    set-azstorageblobcontent -file "$($file.DirectoryName)\$($file.BaseName).wav" -blob "$($file.BaseName).wav" -container audiofiles -context $storageacct.Context -Force -ClientTimeoutPerRequest 1800000 -ServerTimeoutPerRequest 1800000




$transcriptionBody = @"
    {
      "locale": "ru-RU",
      "displayName": "My transcription job test",
      "contentUrls": [
        "https://skunkworksdiag.blob.core.windows.net/audiofiles/Khranetali - Soviet LOTR.wav$SAStoken"
      ],
      "properties": {
        "diarizationEnabled": false,
        "wordLevelTimestampsEnabled": false,
        "punctuationMode": "DictatedAndAutomatic",
        "profanityFilterMode": "None "
      }
    }

"@

$transcriptionheaders = @{
        "Ocp-Apim-Subscription-Key" = "$key1";
        "content-type" = "application/json";
        "content-length" = $transcriptionBody.Length
    }

    $batchURI = "https://centralus.api.cognitive.microsoft.com/speechtotext/v3.0/transcriptions"

    $response = Invoke-WebRequest -Method POST -Uri $batchURI -Headers $transcriptionheaders -Body $transcriptionBody 

    $response.Content

    $responseobj = $response | convertfrom-json 
    ((Invoke-WebRequest -Method GET -Uri $responseobj.self -Headers $transcriptionheaders).Content | convertfrom-json).status #-Body $transcriptionBody 

    $joburi = $responseobj.self

        $downloaduri = "$joburi/files"
        $count = 0
    foreach($file in ((Invoke-WebRequest -Method GET -Uri $downloaduri  -Headers $transcriptionheaders).Content | convertfrom-json).values)
    {
        Invoke-WebRequest -Uri $file.links.contentUrl -OutFile "Khranetali - Soviet LOTR$count.json"
        $count++
    }

    $transcriptobj = (get-content "Khranetali - Soviet LOTR.json" -Encoding UTF8) | convertfrom-json

    function transtime-totimespan($srtTime)
    {
        #TODO: there's bugs here still - missing corner cases.  Write unit test or something, jesus christo.
        if($srtTime -contains "H")
        {
            $hr = $srtTime.Replace("PT","").Split("H")[0]
            
        }
        else
        {
            $hr = "00"
        }


        if($srtTime -like "*M*")
        {
            if($srtTime -like "*H*")
            {
                $hr = $srtTime.Replace("PT","").Split("H")[0]
                $min = $srtTime.Replace("PT","").Split("H")[1].Split("M")[0]
            }
            else
            {
                $min = $srtTime.Replace("PT","").Split("M")[0]
            }
            
            $sec = $srtTime.Replace("PT","").Split("M")[1].Split(".")[0].Replace("S","")
        }
        else
        {
            $min = "00"
            if($srtTime -like "*H*")
            {
                $hr = $srtTime.Replace("PT","").Split("H")[0]
                $sec = $srtTime.Replace("PT","").Split("H")[1].Split(".")[0].Replace("S","")
            }
            else
            {
                $sec = $srtTime.Replace("PT","").Split(".")[0].Replace("S","")
            }
        }

        if($srtTime -like "*.*")
        {
            $frac = [convert]::ToInt32($srtTime.Split(".")[1].Replace("S","")) * 10
        }
        else
        {
            $frac = 0
        }

        return [timespan]::new(0,$hr,$min,$sec,$frac)
    }

    function timespanto-SRTTime{
param(
    [Parameter(Mandatory=$true)]
    [timespan] $timespn
    )

    return "{0:d2}:{1:D2}:{2:d2},{3:d2}" -f $timespn.Hours, $timespn.Minutes, $timespn.Seconds, ($timespn.Milliseconds/10)
}


function translate-string{
param(
    [Parameter(Mandatory=$true)]
    [string] $srcLang,
    [Parameter(Mandatory=$false)]
    [string] $tgtLang = "en",
    [Parameter(Mandatory=$true)]
    [string] $langString,
    [Parameter(Mandatory=$true)]
    [string] $ocpKey,
    [Parameter(Mandatory=$false)]
    [string] $OAuthToken,
    [Parameter(Mandatory=$false)]
    [string] $Region = "centralus"

    )

    #To get list of supported languages:
    #https://api.cognitive.microsofttranslator.com/languages?api-version=3.0

    $translateUri = "https://api.cognitive.microsofttranslator.com/translate?api-version=3.0&to=$tgtLang&from=$srcLang"

    $body =  "[{'Text' : '$($langString)'}]"

    $headers = @{}
    $headers.Add("Ocp-Apim-Subscription-Key",$ocpKey)
    #$headers.Add("Content-Type","application/json; charset=utf-8")
    $headers.Add("Ocp-Apim-Subscription-Region",$Region)


    #get the translated string back

    $responseobj = (Invoke-RestMethod -Uri $translateUri -body $body -Method Post -Headers $Headers -ContentType "application/json; charset=utf-8")# | convertfrom-json

    return $responseobj
}

$sublines = 1

    foreach($sentence in ($transcriptobj.RecognizedPhrases | Where-Object -Property Channel -eq 0))
    {
        $stime = transtime-totimespan $sentence.Offset
        $durtime = transtime-totimespan $sentence.duration

        $ftime = ($stime + $durtime)

        
        #get all the guesses, sort by confidence, grab the first one
        $guesses = $sentence.nBest | sort-object -Property confidence -Descending


        #write to SRT file.
        #TODO: take the sentence and call out to translate service
        #TOOD: determine ~max chars, and insert line break at time.
        
        add-content -Path .\file.srt -Value $sublines
        add-content -Path .\file.srt -Value "$(timespanto-SRTTime $stime) --> $(timespanto-SRTTime $ftime)"

        write-host translate string $($guesses[0].display)
        #translate this guy:
        $translation = translate-string -langString $guesses[0].display -srcLang "ru" -tgtLang "en" -ocpKey 6eb6053467ed4dd480dc7342f82a4c58 
        
        add-content -Path .\file.srt -Value $translation.Translations[0].Text


        add-content -Path .\file.srt -Value "`n"
        $sublines++
    }

    $ffmpeg = find-ffmpeg

    $srtcmd = ""c:\program files\FFMPEG\bin\ffmpeg.exe"  -i "Khranetali - Soviet LOTR.mp4" -f srt -i file.srt -c:v copy -c:a copy -c:s mov_text "Khranetali - Soviet LORT.Subbed.mp4""

    $location = $response.Headers["Operation-Location"]
    <#
    #check the operation for status...
    $batchstatus =  Invoke-WebRequest -Uri $location -Headers $Headers
    start-sleep -Seconds 300
    while($responseobj.status -ne "Failed" -and $responseobj.status -ne "Succeeded")
    {
        $batchstatus =  Invoke-WebRequest -Uri $location -Headers $Headers

        $responseobj = convertfrom-json $batchstatus.Content
        $responseobj.status
        start-sleep -Seconds 300
    }
    #Running
    #Failed
    #>

    remove-item -Path "$($file.DirectoryName)\$($file.BaseName).wav"
    if($responseobj.status -eq "Succeeded")
    {
        Move-Item  $file.FullName -Destination "$($file.DirectoryName)\Done"
    }
}



#https://skunkworksdiag.blob.core.windows.net/audiofiles/unwritable.zip?sv=2019-02-02&ss=bfqt&srt=sco&sp=rwdlacup&se=2025-02-12T11:03:47Z&st=2020-02-12T03:03:47Z&spr=https&sig=TItO6ojwMunOMSnz7NJCk0KrJTmvbryM7CNpV2CpvEg%3D
