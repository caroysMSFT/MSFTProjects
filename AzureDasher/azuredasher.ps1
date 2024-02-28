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
    Write-Host $ResourceList[0] -ForegroundColor Green
    # Get the first resource to use it to pull the list of metrics
    $metrics = get-availablemetrics -resourceid $ResourceList[0]
}
else
{
    #Use the proxy resource.  Why?  It lets us create dashboards for resources we have no access to, 
    #but pulling available metrics from a resource we DO have access to.
    $metrics = get-availablemetrics -resourceid $proxyresource
}


$resourcedash = get-content ".\Dashboard Templates\dashboard.json" | convertfrom-json 

$graphcount = 0

foreach($metric in $metrics)
{
    $resourcepart = get-content ".\Dashboard Templates\resourcepart.json" | convertfrom-json
    #Add a new graph to the Part element using the resourcepart JSON

    $templatePart = $resourcepart.metadata.settings.content.options.chart.metrics[0]

    $resourcepart.metadata.settings.content.options.chart.metrics = @() 

    foreach($resource in $ResourceList)
    {
        #this double conversion needed because pscustomobject.copy() method does shallow copy, and we end up updating by reference still on sub-properties
        #practically, this means - the last resource in the list ends up in every entry
        $resourcepart2 = $templatePart | convertto-json | convertfrom-json 

        $resourcepart2.resourceMetadata.id = $resource
        $resourcepart2.name =  $metric
        $resourcepart2.namespace = "$($resource.split("/")[6])/$($resource.split("/")[7])"
        $resourcepart2.metricVisualization.displayName =  $metric
        $resourcepart.metadata.settings.content.options.chart.metrics += $resourcepart2
    }

    # 3 graphs across, with the template's 6x4 block dimensions
    $xcoord = ($graphcount * 6) % 18
    $ycoord = [math]::truncate($graphcount  / 3)  * 4

    $resourcepart.position.x = $xcoord
    $resourcepart.position.y = $ycoord
    $resourcepart.metadata.settings.content.options.chart.title = "$metric by Resource"


    $resourcedash.properties.lenses.'0'.parts | add-member -Name $graphcount.tostring() -Value $resourcepart -MemberType NoteProperty

    $graphcount++
}

if(get-item -Path $outputfile)
{
    remove-item $outputfile
}

$resourcedash.name = $DashboardName
$resourcedash.tags.'hidden-title' = $DashboardName

$resourcedash | convertto-json -compress  -Depth 100 >> $outputfile

if($OpenNotepad)
{
    notepad $outputfile
}
