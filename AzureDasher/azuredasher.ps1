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


# Get the first resource to use it to pull the list of metrics
$metrics = get-availablemetrics -resourceid $vms[0]




$vmdash = get-content ".\Dashboard Templates\vmmetric.json" | convertfrom-json 



$graphcount = 0

foreach($metric in $metrics)
{
    $vmpart = get-content ".\Dashboard Templates\base.json" | convertfrom-json
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

del output.json

$vmdash | convertto-json -compress  -Depth 100 >> output.json
