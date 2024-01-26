function Convert-toWAV {
param(
    [Parameter(Mandatory=$true)]
    [string] $srcfile
    )

    if(test-path $srcfile)
    {
        $file = get-item $srcfile
    }
    else
    {
        write-error "File $srcfile not found!"
    }
        

    $vlcpath = find-vlc

    $vlcparams = " -I dummy -vvv --sout=`"#transcode{vcodec=none,acodec=s16l,ab=128,channels=2,samplerate=16000}:std{access=file,mux=wav,dst='$($file.DirectoryName)\$($file.BaseName).wav'}`" `"$($file.FullName)`" vlc://quit"
    #TODO: check vlc return code for success/failure, throw exception/error
    Start-Process -FilePath $vlcpath -ArgumentList $vlcparams -Wait
    return '$($file.DirectoryName)\$($file.BaseName).wav'
}

function Transcribe-Wav {
param(
    [Parameter(Mandatory=$true)]
    [string] $srcfile,
    [Parameter(Mandatory=$true)]
    [string] $containerSAS,
    [Parameter(Mandatory=$true)]
    [string] $storageSAS,
    [Parameter(Mandatory=$true)]
    [string] $storageAcct,
    [Parameter(Mandatory=$true)]
    [string] $container,
    [Parameter(Mandatory=$true)]
    [string] $ocpKey,
    [Parameter(Mandatory=$true)]
    [string] $locale = "en-US",
    [Parameter(Mandatory=$true)]
    [string] $resourcegroupname
    )


    $storageacctobj = get-azstorageaccount -resourcegroupname $resourcegroupname -name $storageAcct

    $Headers = @{
    'Ocp-Apim-Subscription-Key' = $ocpKey;
    'Content-Length' = '0';
    'Content-type' = 'application/x-www-form-urlencoded'
    }

    #$OAuthToken = Invoke-RestMethod -Uri https://centralus.api.cognitive.microsoft.com/sts/v1.0/issuetoken -Method Post -Headers $headers

    #upload to blob storage
    set-azstorageblobcontent -file "$($file.DirectoryName)\$($file.BaseName).wav" -blob "$($file.BaseName).wav" -container audiofiles -context $storageacct.Context -Force -ClientTimeoutPerRequest 1800000 -ServerTimeoutPerRequest 1800000

$transcriptionBody = @"
    {
      "locale": $locale,
      "displayName": "$SrcFile $OcpKey $locale",
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

    $joburi = (Invoke-WebRequest -Method POST -Uri $batchURI -Headers $Headers -Body $transcriptionbody | convertfrom-json)

    start-sleep -Seconds 120

    $status = ((Invoke-WebRequest -Method GET -Uri $joburi  -Headers $transcriptionheaders).Content | convertfrom-json)

    #if exception handling is proper, we should be in a running state.
    while($status -eq "Running" -and $status -ne "Failed")
    {
        start-sleep -seconds 120
    }

    #download files...
    $downloaduri = "$joburi/files"
    (Invoke-WebRequest -Method GET -Uri $downloaduri  -Headers $transcriptionheaders).Content
}

function Translate-Text {
param(
    [Parameter(Mandatory=$true)]
    [string] $srcText,
    [Parameter(Mandatory=$true)]
    [string] $srcLang,
    [Parameter(Mandatory=$true)]
    [string] $tgtLang = "en",
    [Parameter(Mandatory=$true)]
    [string] $apmKey
    )

    #TODO: figure out how to define source language

    $translateUri = "https://api-nam.cognitive.microsofttranslator.com/translate?api-version=3.0&to=$tgtLang" 
    $Headers = @{
    'Ocp-Apim-Subscription-Key' = $apmKey;
    'Content-type' = 'application/json';
}

    $body = "[{'Text':'$($srcText)'}]"
    Invoke-WebRequest -Uri $translateUri -Headers $Headers -body $body
}

function make-srt {
    (
    [Parameter(Mandatory=$true)]
    [string] $srcA,
    [Parameter(Mandatory=$false)]
    [string] $srcB
    )

    #walk the files line by line, by timestamp.  Walk back and forth between the JSON objects by timestamp until you get to the end of them both.
    #write timestamps in the right format to an .srt file.
}

function Convert-toMP4 {
param(
    [Parameter(Mandatory=$true)]
    [string] $srcfile
    )

    if(test-path $srcfile)
    {
        $file = get-item $srcfile
    }
    else
    {
        write-error "File $srcfile not found!"
    }
        

    $vlcpath = find-vlc

    $vlcparams = " -I dummy -vvv --sout=`"#transcode{vcodec=none,acodec=s16l,ab=128,channels=2,samplerate=16000}:std{access=file,mux=mp4,dst='$($file.DirectoryName)\$($file.BaseName).mp4'}`" `"$($file.FullName)`" vlc://quit"
    #TODO: check vlc return code for success/failure, throw exception/error
    Start-Process -FilePath $vlcpath -ArgumentList $vlcparams -Wait
    return '$($file.DirectoryName)\$($file.BaseName).wav'
}

function Convert-toMP3 {
param(
    [Parameter(Mandatory=$true)]
    [string] $srcfile
    )

    $isvideo = $srcfile.split(".")[1] -in ("ts","mp4", "avi", "mpg","wmv","mpeg","vob")

    #for video: vlc file.ts --no-sout-video --sout '#std{mux=raw,dst=file.mp3}'

    if(test-path $srcfile)
    {
        $file = get-item $srcfile
    }
    else
    {
        write-error "File $srcfile not found!"
    }
        

    $vlcpath = find-vlc

    #below command doesn't produce a working MP3 - needs work.
    $vlcparams = " -I dummy -vvv --sout=`"#transcode{vcodec=none,acodec=s16l,ab=128,channels=2,samplerate=16000}:std{access=file,mux=mp3,dst='$($file.DirectoryName)\$($file.BaseName).mp3'}`" `"$($file.FullName)`" vlc://quit"
    #TODO: check vlc return code for success/failure, throw exception/error
    Start-Process -FilePath $vlcpath -ArgumentList $vlcparams -Wait
    return ("$($file.DirectoryName)\$($file.BaseName).mp3")
}

function Convert-toOGG {
param(
    [Parameter(Mandatory=$true)]
    [string] $srcfile
    )

    $isvideo = $srcfile.split(".")[1] -in ("ts","mp4", "avi", "mpg","wmv","mpeg","vob")

    #for video: vlc file.ts --no-sout-video --sout '#std{mux=raw,dst=file.mp3}'

    if(test-path $srcfile)
    {
        $file = get-item $srcfile
    }
    else
    {
        write-error "File $srcfile not found!"
    }
        

    $vlcpath = find-vlc

    #below command doesn't produce a working MP3 - needs work.
    $vlcparams = " -I dummy -vvv --sout=`"#transcode{vcodec=none,acodec=s16l,ab=128,channels=2,samplerate=16000}:std{access=file,mux=ogg,dst='$($file.DirectoryName)\$($file.BaseName).ogg'}`" `"$($file.FullName)`" vlc://quit"
    #TODO: check vlc return code for success/failure, throw exception/error
    Start-Process -FilePath $vlcpath -ArgumentList $vlcparams -Wait
    return ("$($file.DirectoryName)\$($file.BaseName).ogg")
}

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

function find-vlc() {

    $vlcpath = (get-item -path Registry::HKEY_LOCAL_MACHINE\SOFTWARE\WOW6432Node\VideoLAN\VLC).GetValue("")

    if(test-path $vlcpath)
    {
        return $vlcpath
    }
    else
    {
        $vlcpath = (get-item -path Registry::HKEY_LOCAL_MACHINE\SOFTWARE\VideoLAN\VLC).GetValue("")
        if(test-path $vlcpath)
        {
            return $vlcpath
        }
        Write-Error "VLC Not found!  Exiting..."
    }
}

function find-ffmpeg() {

    $ffmpegpath = (get-item -path 'C:\Program Files\FFMPEG\bin\ffmpeg.exe')

    if(test-path $ffmpegpath)
    {
        return $ffmpegpath
    }
    else
    {
        Write-Error "FFMPEG Not found!  Exiting..."
    }
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
    [string] $ocpKey

    )

    #To get list of supported languages:
    #https://api.cognitive.microsofttranslator.com/languages?api-version=3.0

    $translateUri = "https://api-nam.cognitive.microsofttranslator.com/translator/text/v3.0/translate?to=$tgtLang&from=$srcLang"

    $body = "[{'Text':'$langString'}]"

    $Headers = @{
    'Ocp-Apim-Subscription-Key' = $ocpKey;
    'Content-Length' = $body.Length;
    'Content-type' = "application/json; charset=UTF-8"
    }

    

    #get the translated string back

    $responseobj = (Invoke-WebRequest -Uri $translateUri -body $body).Content | convertfrom-json

    return $responseobj.translations[0].Text
}

Export-ModuleMember -Function Transcribe-Wav
Export-ModuleMember -Function Convert-toWAV
Export-ModuleMember -Function Convert-toMP3
Export-ModuleMember -Function Convert-toMP4
Export-ModuleMember -Function Translate-Text
Export-ModuleMember -Function transtime-totimespan
Export-ModuleMember -Function timespanto-SRTTime
Export-ModuleMember -Function translate-string
