[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string[]]$ResourceList,
    [Parameter()]
    [string]$ProxyResource,
    [Parameter(Mandatory)]
    [string]$DashboardName,
    [Parameter()]
    [string]$OutputFile = "outputfile.json",
    [Parameter()]
    [bool]$OpenNotepad = $false
)


function get-availablemetrics($resourceid)
{
    $metriclist = @()
    $uri = "https://management.azure.com$resourceid/providers/microsoft.insights/metricdefinitions?api-version=2018-01-01"
     foreach($metric in (az rest --uri $uri | convertfrom-json).value)
     {
        $metriclist += $metric.name.value
     }

    return $metriclist
}

if([string]::IsNullOrEmpty($proxyresource))
{
    Write-host "Proxy resource not used; using first resource to pull list of metrics:" -ForegroundColor Green
    Write-Host $vms[0] -ForegroundColor Green
    # Get the first resource to use it to pull the list of metrics
    $metrics = get-availablemetrics -resourceid $vms[0]
}
else
{
    #Use the proxy resource.  Why?  It lets us create dashboards for resources we have no access to, 
    #but pulling available metrics from a resource we DO have access to.
    $metrics = get-availablemetrics -resourceid $proxyresource
}


$vmdash = get-content ".\Dashboard Templates\dashboard.json" | convertfrom-json 

$graphcount = 0

foreach($metric in $metrics)
{
    $vmpart = get-content ".\Dashboard Templates\resourcepart.json" | convertfrom-json
    #Add a new graph to the Part element using the VMPart JSON

    $templatePart = $vmpart.metadata.settings.content.options.chart.metrics[0]

    $vmpart.metadata.settings.content.options.chart.metrics = @() 

    foreach($vm in $vms)
    {
        #this double conversion needed because pscustomobject.copy() method does shallow copy, and we end up updating by reference still on sub-properties
        #practically, this means - the last VM in the list ends up in every entry
        $vmpart2 = $templatePart | convertto-json | convertfrom-json 

        $vmpart2.resourceMetadata.id = $vm
        $vmpart2.name =  $metric
        $vmpart2.namespace = "$($vm.split("/")[6])/$($vm.split("/")[7])"
        $vmpart2.metricVisualization.displayName =  $metric
        $vmpart.metadata.settings.content.options.chart.metrics += $vmpart2
    }

    # 3 graphs across, with the template's 6x4 block dimensions
    $xcoord = ($graphcount * 6) % 18
    $ycoord = [math]::truncate($graphcount  / 3)  * 4

    $vmpart.position.x = $xcoord
    $vmpart.position.y = $ycoord
    $vmpart.metadata.settings.content.options.chart.title = "$metric by VM"


    $vmdash.properties.lenses.'0'.parts | add-member -Name $graphcount.tostring() -Value $vmpart -MemberType NoteProperty

    $graphcount++
}

if(get-item -Path $outputfile)
{
    remove-item $outputfile
}

$vmdash.name = $DashboardName
$vmdash.tags.'hidden-title' = $DashboardName

$vmdash | convertto-json -compress  -Depth 100 >> $outputfile

if($OpenNotepad)
{
    notepad $outputfile
}
