
function run-appsvccmd($sitename, $resourceGroup, $cmd, $path = "site\\repository", $instanceid)
{
    $cmduri = "https://$sitename.scm.azurewebsites.net/api/command"

    $website = Get-AzWebApp -Name $websiteName -ResourceGroupName $resourceGroup

    $profile = Get-AzWebAppPublishingProfile -Name $websiteName -ResourceGroupName $resourceGroup -format WebDeploy
    $profileobj = new-object XML

    $profileobj.LoadXml($profile)
    $profileobj.publishData.Attributes

    $publishingUsername = $profileobj.publishData.publishProfile[0].userName
    $publishingPassword = $profileobj.publishData.publishProfile[0].userPWD

    $base64AuthInfo = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $publishingUsername, $publishingPassword)))
                    
    # Ref: https://github.com/projectkudu/kudu/wiki/REST-API
    # ref: https://techcommunity.microsoft.com/t5/iis-support-blog/memory-dumps-to-study-a-high-cpu-performance-issue/ba-p/2556170

    $body = new-object Object
    $body |  Add-Member -NotePropertyName "command" -NotePropertyValue $cmd
    $body |  Add-Member -NotePropertyName "dir" -NotePropertyValue $path

    $headers = @{}

    $headers["Authorization"] = "Basic $base64AuthInfo"
    $headers["content-type"] = "application/json"

    if($instanceid -ne $null)
    {
        $cookieVal = "ARRAffinity=$instanceid; ARRAffinitySameSite=$instanceid"
        $headers["Cookie"] = $cookieVal
    }

    return ((Invoke-WebRequest -uri $cmduri -Headers $headers -Body ($body | convertto-json) -UseBasicParsing -Method POST).Content | convertfrom-json)
}


function download-appsvcfile($sitename, $resourceGroup, $outfile,$filepath, $instanceid)
{


    $cmduri = "https://$sitename.scm.azurewebsites.net/api/vfs/$filepath"

    $website = Get-AzWebApp -Name $websiteName -ResourceGroupName $resourceGroup

    $profile = Get-AzWebAppPublishingProfile -Name $websiteName -ResourceGroupName $resourceGroup -format WebDeploy
    $profileobj = new-object XML

    $profileobj.LoadXml($profile)
    $profileobj.publishData.Attributes

    $publishingUsername = $profileobj.publishData.publishProfile[0].userName
    $publishingPassword = $profileobj.publishData.publishProfile[0].userPWD

    $base64AuthInfo = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $publishingUsername, $publishingPassword)))
                    
    # Ref: https://github.com/projectkudu/kudu/wiki/REST-API
    $headers = @{}

    $headers["Authorization"] = "Basic $base64AuthInfo"
    $headers["content-type"] = "application/json"

    if($instanceid -ne $null)
    {
        $cookieVal = "ARRAffinity=$instanceid; ARRAffinitySameSite=$instanceid"
        $headers["Cookie"] = $cookieVal
    }

     Invoke-WebRequest -uri $cmduri -Headers $headers -Method Get -OutFile $outfile
}

function get-appsvcinstances($subid, $sitename, $resourceGroup)
{
    $headers = @{}

    $cmduri = "https://management.azure.com/subscriptions/$subid/resourceGroups/$resourceGroup/providers/Microsoft.Web/sites/$sitename/instances?api-version=2022-03-01"
    $token = (get-azaccesstoken).Token
    $headers["Authorization"] = "Bearer $token"
    return ((Invoke-WebRequest -uri $cmduri -Headers $headers -Method Get).Content | convertfrom-json)
}


$subid = ""
$websiteName = ""
$slotName = ""
$resourceGroup = ""



$result = (run-appsvccmd -sitename $websiteName -cmd "powershell -command `"(get-process -name w3wp) | convertto-json`"" -path "\home\logfiles" -resourceGroup $resourceGroup)
foreach($proc in ($result.Output | convertfrom-json))
{
    write-host "Capturing dumps to c:\home\logfiles"
    $dmpcmd = "C:\devtools\sysinternals\procdump.exe -accepteula -ma $($proc.Id)"
    $result = (run-appsvccmd -sitename $websiteName -cmd "powershell -command `"$dmpcmd`"" -path "c:\home\logfiles")
    $result.Error
}


$result = (run-appsvccmd -sitename $websiteName -cmd "powershell -command `"(dir *.dmp).FullName`"" -path "\home\logfiles" -resourceGroup $resourceGroup)

foreach($file in $result.Output.split("`r`n"))
{
    if($file.Length -gt 0)
    {
        download-appsvcfile -sitename $websiteName -outfile c:\projects\$($file.Split("\")[$file.split("\").Count-1]) -filepath $file -resourceGroup $resourceGroup
    }
}


