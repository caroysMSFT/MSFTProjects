param ([int] $pollinterval, [string]$uri)

$headers = ("TimeStamp","HopNum","Source","Destination","HopHost","TimeTaken")
$logname = "tracehost"

function write-csv($logentry)
{
    #Create *.csv if it doesn't exist, with header values.
    $logfilepath = "$PSScriptRoot\$logname-$(get-date -format `"MM-dd-yyyy`").csv"
    if((test-path -Path $logfilepath) -eq $false)
    {
        foreach($header in $headers)
        {
            $headerrow += "$header,"
        }
        
        $headerrow = $headerrow.ToString().TrimEnd(",")
        $Utf8NoBomEncoding = New-Object System.Text.UTF8Encoding $False 
        [System.IO.File]::WriteAllLines($logfilepath, $headerrow, $Utf8NoBomEncoding)
    }

    $logentry >> $logfilepath
}

function get-egressip()
{
    $response = Invoke-WebRequest -Uri https://whatsmyip.com -UseBasicParsing

    $htmlobj = New-Object -Com "HTMLFile"

    $htmlobj.IHTMLDocument2_write($response.Content)

    $egressIP = $htmlobj.getElementById("shownIpv4").IHTMLElement_innerText

    return $egressIP
}

$hostname = $uri.split("/")[2]

while($true)
{
    $ttl = 1

    $CurrentMinuteTime = (get-date).ToString("o")
    foreach($node in (Test-NetConnection $hostname -traceroute).Traceroute)
    {
        $result = (Test-NetConnection $node -Hops $ttl)
        $record= "$CurrentMinuteTime,$ttl,$(get-egressip),$hostname,$node,$($result.PingReplyDetails.RoundtripTime)"
        write-csv $record
        $ttl++
    }

    #TODO: Do a GET request 
    $requesttime = (Measure-Command -Expression {$response = Invoke-WebRequest -UseBasicParsing -Uri $uri}).Milliseconds

    write-csv "$CurrentMinuteTime,,$(get-egressip),$uri,$hostname,$requesttime"

    start-sleep -Seconds $pollinterval
}
