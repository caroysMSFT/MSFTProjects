function log($msg, $foregroundcolor = "white")
{
    Write-Host $msg -ForegroundColor $foregroundcolor
    "$(get-date): $msg" | Out-File -FilePath "$($pwd.Path)\adfbcdr.log" -Append
}

function run-azcmd($cmd, $deserialize = $true)
{
    $results = @()
    log "Command Running: $cmd"
    $scriptblock = {$cmd}
    $result = iex $cmd 2>&1
    if($LASTEXITCODE -ne 0)
    {
        #get the stderr of invoke-expression, log it.
        log "Last exit code was $LASTEXITCODE" -ForegroundColor Red
        log $Error[1] -ForegroundColor Red
        log $Error[0] -ForegroundColor Red
        throw $Error[0]
    }

    <#Run-AzCmd deserialize=$false flag is intended for pulling ARM templates.
    Therefore, we will only check for continuation token where we're sending back objects#>
    switch($deserialize)
    {
        $true {
                # first check here...
                $tmpresult = ($result | convertfrom-json )
                write-host $tmpresult
                switch($tmpresult)
                {
                    {($PSItem | get-member value) -ne $null} 
                        {
                            write-host "value array switch hit" -ForegroundColor Green
                            $results += ($result | convertfrom-json ).value
                        }
                    {$PSItem.count -gt 0}
                        {
                            write-host "array switch hit" -ForegroundColor Green
                            $results += ($result | convertfrom-json )
                        }
                    {($PSItem | get-member type) -ne $null}
                        {
                            write-host "single value switch hit" -ForegroundColor Green
                            $results += $PSItem
                        }
                }
                if(($result | convertfrom-json ).nextLink -ne $null)
                {
                    $nextlink = ($result | convertfrom-json ).nextLink
                    $bDone = $false
                    while($bDone -eq $false)
                    {
                        log "trying nextlink: $nextlink" -ForegroundColor Yellow
                        $tmp = (run-azcmd "az rest --uri `'`"$nextlink`"`' --method get") 
                        $results += $tmp
                        if($tmp.nextLink -eq $null) 
                        { $bDone = $true} 
                        else
                        { $nextlink = $tmp.nextLink }
                    } 
                }
                return $results
               }
        $false 
            {
                # this is only used for downloading templates, so no possibility of skiptoken
                return $result
            }
    }
}

function despace-template($json)
{
    $type = $json.id.split("/")[9]


    switch($type)
    {
        "pipelines" 
        {
            foreach($activity in $json.properties.activities)
            {
                foreach($property in $activity.typeProperties)
                {
                    if($property.dataflow -ne $null) 
                    { 
                        $property.dataflow.referenceName = $property.dataflow.referenceName.Replace(" ","_")
                    }
                }

                foreach($linkedservice in $activity.linkedServiceName)
                {
                    if($linkedservice.type -eq "LinkedServiceReference") { $linkedservice.referenceName = $linkedservice.referenceName.Replace(" ","_")}
                }
            }
        }
        "datasets" 
        {
            foreach($linkedservice in $json.properties.linkedServiceName)
            {
                if($linkedservice.type -eq "LinkedServiceReference") { $linkedservice.referenceName = $linkedservice.referenceName.Replace(" ","_")}
            }
        }
        "dataflows" 
        {
            foreach($source in $json.properties.typeProperties.sources)
            {
                if($source.linkedService.type -eq "LinkedServiceReference") { $linkedservice.referenceName = $linkedservice.referenceName.Replace(" ","_")}
            }

            foreach($sink in $json.properties.typeProperties.sinks)
            {
                if($sink.linkedService.type -eq "LinkedServiceReference") { $linkedservice.referenceName = $linkedservice.referenceName.Replace(" ","_")}
            }
        }
    }
    return $json
}

function backup-adffactory($sub, $rg, $adf, $outputfile)
{
    log "Starting backup of factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$($adf)?api-version=2018-06-01`'"

    $json = run-azcmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adffactory($sub, $rg, $adf, $inputfile, $region = "")
{
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$($adf)?api-version=2018-06-01"

   $token = (run-azcmd "az account get-access-token").accessToken

   $template =  (get-content -Path $inputfile | convertfrom-json)

   $template.name = $adf


   #This is required because if you are creating from scratch, it needs to be blank (creates a new MSI)
   #TODO:  Check and see if already exists.  This is not desirable when it already exists.
   if($template.identity.type -eq "SystemAssigned")
   {
        $template.identity.principalId = $null
        $template.identity.tenantId = $null
   }

   #restore to different region
   if($region -ne "")
   {
        $template.location = $region
   }

   #This bit enables cross-subscription restore
   $template.id = "/subscriptions/$sub/resourceGroups/$rg/providers/Microsoft.DataFactory/factories/$adf$($suffix)"

   $body = $template | convertto-json

   $headers = @{}
   $headers["Authorization"] = "Bearer $token"
   $headers["Content-Type"] = "application/json"
   log "Callling REST method: $uri"
   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}


function backup-adflinkedservice($sub, $rg, $adf, $linkedservice, $outputfile)
{
    log "Starting backup of linked service $linkedservice in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/linkedservices/$($linkedservice)?api-version=2018-06-01`'"

    $json = run-azcmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adflinkedservice($sub, $rg, $adf, $linkedservice, $inputfile)
{
   $linkedservice = $linkedservice.Replace(" ","_")
   log "Starting restore of linked service $linkedservice in factory $adf in resource group $rg"
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/linkedservices/$($linkedservice)?api-version=2018-06-01"

   $token = (run-azcmd "az account get-access-token").accessToken

   $template = despace-template (get-content -Path $inputfile | convertfrom-json)

   $template.name = $linkedservice

   $body = $template | convertto-json -depth 10

   $headers = @{}
   $headers["Authorization"] = "Bearer $token"
   log "Callling REST method: $uri"
   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}

function backup-adfdataflow($sub, $rg, $adf, $dataflow, $outputfile)
{
    log "Starting backup of data flow $dataflow in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/dataFlows/$($dataflow)?api-version=2018-06-01`'"

    $json = run-azcmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfdataflow($sub, $rg, $adf, $dataflow, $inputfile, $folder = $null)
{
   $dataflow = $dataflow.Replace(" ","_")
   log "Starting restore of data flow $dataflow in factory $adf in resource group $rg"
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/dataFlows/$($dataflow)?api-version=2018-06-01"

   $token = (run-azcmd "az account get-access-token").accessToken

   $template = despace-template (get-content -Path $inputfile | convertfrom-json)
   $template.name = $dataflow

   if($folder -ne $null)
   {
        if($template.properties.folder -ne $null)
        {
            $template.properties.folder.name = $folder
        }
        else
        {
            $template.properties | add-member -Name "folder" -Value ("{ `"name`": `"$folder`" }" | convertfrom-json) -MemberType NoteProperty
        }

   }
   $body = $template | convertto-json -Depth 10

   $headers = @{}
   $headers["Authorization"] = "Bearer $token"

   log "Callling REST method: $uri"
   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}


function backup-adfdataset($sub, $rg, $adf, $dataset, $outputfile)
{
    log "Starting backup of dataset $dataset in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01`'"

    $json = run-azcmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfdataset($sub, $rg, $adf, $dataset, $inputfile, $folder = $null)
{
   log "Starting deploy of data set $dataset in factory $adf in resource group $rg"
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01"

   $token = (run-azcmd "az account get-access-token").accessToken

   $template = get-content -Path $inputfile | convertfrom-json

   if($folder -ne $null)
   {
        if($template.properties.folder -ne $null)
        {
            $template.properties.folder.name = $folder
        }
        else
        {
            $template.properties | add-member -Name "folder" -Value ("{ `"name`": `"$folder`" }" | convertfrom-json) -MemberType NoteProperty
        }
        $body = $template | convertto-json -Depth 10
   }
   else
   {
        $body = get-content -Path $inputfile
   }

   $headers = @{}
   $headers["Authorization"] = "Bearer $token"

   log "Callling REST method: $uri"
   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}


function backup-adfpipeline($sub, $rg, $adf, $pipeline, $outputfile)
{
    log "Starting backup of pipeline $pipeline in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01`'"

    #"`'https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}?api-version=2018-06-01`'"
    $json = run-azcmd "az rest --uri $uri --method get"  -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfpipeline($sub, $rg, $adf, $pipeline, $inputfile, $folder = $null)
{
   log "Starting restore of pipeline $pipeline in factory $adf in resource group $rg"
   $pipeline = $pipeline.Replace(" ","_")
   
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01"
   $token = (run-azcmd "az account get-access-token").accessToken

   $template = despace-template (get-content -Path $inputfile | convertfrom-json)
   $template.name = $pipeline

   if($folder -ne $null)
   {

        if($template.properties.folder -ne $null)
        {
            $template.properties.folder.name = $folder
        }
        else
        {
            $template.properties | add-member -Name "folder" -Value ("{ `"name`": `"$folder`" }" | convertfrom-json) -MemberType NoteProperty
        }
        
   }

   $body = $template | convertto-json -Depth 10

   $headers = @{}
   $headers["Authorization"] = "Bearer $token"

   log "Callling REST method: $uri"
   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}

function ensure-adfdirectory($srcpath)
{
    
    if(Test-Path -Path $srcpath)
    {
        log "Path is found; trying to verify subfolders under $srcpath"
        if(!(Test-Path -Path "$srcpath\pipelines")) { log "Creating pipelines folder:  $srcpath\pipelines"; new-item -path "$srcpath" -ItemType Directory}
        if(!(Test-Path -Path "$srcpath\datasets")) { log "Creating datasets folder:  $srcpath\datasets"; new-item -path "$srcpath" -ItemType Directory}
        if(!(Test-Path -Path "$srcpath\linkedservices")) { log "Creating linkedservices folder: $srcpath\linkedservices ";new-item -path "$srcpath" -ItemType Directory}
        if(!(Test-Path -Path "$srcpath\dataflows")) { log "Creating linkedservices folder:  $srcpath\dataflows";new-item -path "$srcpath" -ItemType Directory}
    }
    else
    {
        log "Creating $srcpath from scratch...";
        new-item -path "$srcpath" -ItemType Directory
        new-item -path "$srcpath\pipelines" -ItemType Directory
        new-item -path "$srcpath\datasets" -ItemType Directory
        new-item -path "$srcpath\linkedservices" -ItemType Directory
        new-item -path "$srcpath\dataflows" -ItemType Directory
    }
}


function check-pipelinelastrun($adf, $rg, $pipeline, [int]$months = 8)
{

    $todayDateStamp = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH\:mm\:ss.fffffffZ")  #az datafactory command expects ISO8601 timestamp format
    $oldDateStamp = "2015-01-01T00:00:00.00Z"  #predates ARM, should be safe as a baseline date range.

    $runs = run-azcmd "az datafactory pipeline-run query-by-factory --resource-group $rg --factory-name $adf --last-updated-after $oldDateStamp --last-updated-before $todayDateStamp --filters operand=`"PipelineName`" operator=`"Equals`" values=`"$pipeline`""
    
    log "Pipeline $pipeline has this many runs: $($runs.Count)"
    if($runs.Count -gt 0)
    {
        $lastRun = [datetime]::parse(($runs | Sort-Object -Property runEnd -Descending)[0].runEnd)
    }
    else { 
        return $false 
    }
    
    return ([DateTime]::Compare($lastRun,[DateTime]::Now.AddMonths($months * -1)) -gt 0)
}

function get-datasets($json)
{
    $type = $json.id.split("/")[9]
    $datasets = @()

    switch($type)
    {
        "pipelines" 
        {
            foreach($activity in $json.properties.activities)
            {
                log "checking activity $($activity.name) for pipeline $($json.name)"
                foreach($input in $activity.inputs)
                {
                    if($input.type -eq "DatasetReference") 
                    { 
                        log "found dataset $($input.referenceName) for pipeline $($json.name)"
                        $datasets += $input.referenceName
                    }
                }

                foreach($output in $activity.outputs)
                {
                    if($output.type -eq "DatasetReference") 
                    { 
                        log "found dataset $($output.referenceName) for pipeline $($json.name)"
                        $datasets += $output.referenceName
                    }
                }
            }
        }
        "dataflows" 
        {
            foreach($source in $json.properties.typeProperties.sources)
            {
                if($source.dataset.type -eq "DatasetReference") { $datasets += $source.dataset.referenceName}
            }

            foreach($sink in $json.properties.typeProperties.sinks)
            {
                if($sink.dataset.type -eq "DatasetReference") { $datasets += $sink.dataset.referenceName}
            }
            
        }
    }
    return $datasets
}

function get-dataflows($json)
{
    $type = $json.id.split("/")[9]
    $dataflows = @()

    switch($type)
    {
        "pipelines" 
        {
            foreach($activity in $json.properties.activities)
            {
                foreach($property in $activity.typeProperties)
                {
                    if($property.dataflow -ne $null) 
                    { 
                        $dataflows += $property.dataflow.referenceName
                    }
                }
            }
        }
    }
    return $dataflows
}


function get-linkedservices($json)
{
    $type = $json.id.split("/")[9]
    $linkedservices = @()

    switch($type)
    {
        "pipelines" 
        {
            foreach($activity in $json.properties.activities)
            {
                foreach($linkedservice in $activity.linkedServiceName)
                {
                    if($linkedservice.type -eq "LinkedServiceReference") { $linkedservices += $linkedservice.referenceName}
                }
            }
        }
        "datasets" 
        {
            foreach($linkedservice in $json.properties.linkedServiceName)
            {
                if($linkedservice.type -eq "LinkedServiceReference") { $linkedservices += $linkedservice.referenceName}
            }
        }
        "dataflows" 
        {
            foreach($source in $json.properties.typeProperties.sources)
            {
                if($source.linkedService.type -eq "LinkedServiceReference") { $linkedservices += $source.linkedService.referenceName}
            }

            foreach($sink in $json.properties.typeProperties.sinks)
            {
                if($sink.linkedService.type -eq "LinkedServiceReference") { $linkedservices += $sink.linkedService.referenceName}
            }
        }
    }
    return $linkedservices
}

function get-dataflowdatasets($dataflows, $factory, $resourceGroup, $subscription)
{
    $datasets = @()
    #get dataflow JSON   

    foreach($dataflow in $dataflows)
    {
        $dataflowuri = "`'https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory)/dataFlows/$($dataflow)?api-version=2018-06-01`'"
        $dataflowobj = (run-azcmd "az rest --uri $dataflowuri --method get")
        $datasets += (get-datasets $dataflowobj)
    }
    return $datasets
}




function backup-factories($sub, $resourceGroup, $srcfolder, $filter = $false, $lookbackMonths = 12)
{
    log "Backup factories running on sub: $sub with RG: $resourceGroup with source folder: $srcfolder"
    $factories = (run-azcmd "az resource list --resource-group $resourceGroup --resource-type `"Microsoft.Datafactory/factories`"")
    foreach($factory in $factories)
    {
        log "Starting backup of Factory $($factory.name) in resource group $resourceGroup"
        $datasets = @()
        $dataflows = @()
        $linkedservices = @()

        #make sure all subfolders exist first...
        ensure-adfdirectory -srcpath "$srcfolder\$($factory.name)"

        #backup pipelines first
        $pipelineuri = "`'https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/pipelines?api-version=2018-06-01`'"

        foreach($pipeline in (run-azcmd "az rest --uri $pipelineuri --method get"))
        {
            log "Evaluating $($pipeline.name)" -foregroundcolor yellow

            #Don't back up if not run in last X months...
            if(($Filter -and (check-pipelinelastrun -adf $factory.name -rg $resourceGroup -pipeline $pipeline.name -months $lookbackMonths)) -or !($Filter)) 
            {
                log "Found pipeline: $($pipeline.name)" -ForegroundColor Green
                #See if it has a Folder element declared; if so, create a subfolder.
                if($pipeline.folder -ne $null)
                {
                    new-item -ItemType Directory -Path "$srcfolder\$($factory.name)\pipelines\$($pipeline.folder.name)" -force
                    $outputfile = "$srcfolder\$($factory.name)\pipelines\$($pipeline.folder.name)\$($pipeline.name).json"
                }
                else
                {
                    $outputfile = "$srcfolder\$($factory.name)\pipelines\$($pipeline.name).json"
                }

                $datasets += (get-datasets $pipeline)
                $dataflows += (get-dataflows $pipeline)
                $datasets += (get-dataflowdatasets -dataflows $dataflows -factory $factory.name -resourcegroup $resourceGroup -subscription $subscription)
                $linkedservices += (get-linkedservices $pipeline)

                log "found pipeline datasets:  " -foregroundcolor Green
                $datasets
                log "found pipeline dataflows: " -foregroundcolor Green
                $dataflows
                log "found pipeline linkedservices: " -foregroundcolor Green
                $linkedservices
                backup-adfpipeline -sub $subscription -rg $resourceGroup -adf $factory.name -pipeline $pipeline.name -outputfile $outputfile
            }
        }

        #backup dataflows
        $dataflowuri = "`'https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/dataflows?api-version=2018-06-01`'"
        foreach($dataflow in ((run-azcmd "az rest --uri $dataflowuri --method get")))
        {
            # ensure data flow is referenced by a recently run pipeline
            # not actually required...  commenting out to keep all
            # if($dataflows -contains $dataflow.name)
            # {
                log "Found data flow: $($dataflow.name)" -ForegroundColor Green

                # Logically keeping all dataflows also means, we shouldn't be tracking which linked services they reference.
                # and we might end up backing up nearly all datasets and linked services
                # $linkedservices += (get-linkedservices $dataflow)
                $datasets += (get-datasets $dataflow)  
                if($dataflow.properties.folder -ne $null)
                {
                    new-item -ItemType Directory -Path "$srcfolder\$($factory.name)\dataflows\$($dataflow.properties.folder.name)" -Force
                    $outputfile = "$srcfolder\$($factory.name)\dataflows\$($dataflow.properties.folder.name)\$($dataflow.name).json"
                }
                else
                {
                    $outputfile = "$srcfolder\$($factory.name)\dataflows\$($dataflow.name).json"
                }
                backup-adfdataflow -sub $subscription -rg $resourceGroup -adf $factory.name -dataflow $dataflow.name -outputfile $outputfile
            # }
            # else
            #{
            #    log "Data flow $($dataflow.name) not referenced by a recently run pipeline; skipping..." -foregroundcolor Yellow
            #} 
        }

        #backup datasets
        $dataseturi = "`'https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/datasets?api-version=2018-06-01`'"

        foreach($dataset in ((run-azcmd "az rest --uri $dataseturi --method get")))
        {
            if($datasets -contains $dataset.name)
            {
                log "Found data set: $($dataset.name)" -ForegroundColor Green

                # add its linked services to our recently used list
                $linkedservices += (get-linkedservices $dataset)

                if($dataset.properties.folder -ne $null)
                {
                    new-item -ItemType Directory -Path "$srcfolder\$($factory.name)\datasets\$($dataset.properties.folder.name)" -Force
                    $outputfile = "$srcfolder\$($factory.name)\datasets\$($dataset.properties.folder.name)\$($dataset.name).json"
                }
                else
                {
                    $outputfile = "$srcfolder\$($factory.name)\datasets\$($dataset.name).json"
                }
                backup-adfdataset -sub $subscription -rg $resourceGroup -adf $factory.name -dataset $dataset.name -outputfile $outputfile
            }
            else
            {
                log "Data flow $($dataset.name) not referenced by a recently run pipeline; skipping..." -foregroundcolor Yellow
            }
        }

        #backup linked services
        foreach($service in (run-azcmd "az datafactory linked-service list --resource-group $resourceGroup --factory-name $($factory.name)"))
        {
            if($linkedservices -contains $service.name)
            {
                log "Found linkedservice: $($service.name)" -ForegroundColor Green
                backup-adflinkedservice -sub $subscription -rg $resourceGroup -adf $factory.name -linkedservice $service.name -outputfile "$srcfolder\$($factory.name)\linkedservices\$($service.name).json"
            }
            else
            {
                log "Linked Service $($service.name) not referenced by a recently run pipeline or data set; skipping..." -foregroundcolor Yellow
            }
        }

        #backup the factory itself
        backup-adffactory -sub $subscription -rg $resourceGroup -adf $factory.name -outputfile "$srcfolder\$($factory.name)\$($factory.name).json"
    }

}


function restore-factories($sub, $rg, $srcfolder, $suffix = "", $region = "")
{
    log "Restore factories running on sub: $sub with RG: $resourceGroup with source folder: $srcfolder"
    $srcdir = get-item -Path $srcfolder

    foreach($factory in $srcdir.GetDirectories())
    {
        log "Starting restore of Factory $($factory.Name) in resource group $resourceGroup"

        try
        {
            #suffix is included here to ensure the ADF name is unique globally.  Backup and restore won't work if you haven't deleted the source factory otherwise.
            deploy-adffactory -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -inputfile "$($factory.FullName)\$($factory.Name).json" -region $region
        }
        catch
        {
            log $_.Exception -ForegroundColor Red
            continue
        }

        #deploy linked services
        foreach($service in $factory.GetDirectories("linkedservices").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
        {
            deploy-adflinkedservice -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -linkedservice $service.BaseName -inputfile $service.FullName
        }
      
        log "Deploying backed up Data sets..."
        foreach($dataset in $factory.GetDirectories("datasets").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
        {
            log "found dataset $dataset"
            $folder = $dataset.Directory.FullName.Replace("$($factory.FullName)\datasets","").Trim("\").Replace("\","/")
            deploy-adfdataset -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -dataset $dataset.BaseName -inputfile $dataset.FullName -folder $folder
        }

        log "Deploying backed up Data flows..."
        foreach($dataflow in $factory.GetDirectories("dataflows").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
        {
            log "found dataflow $dataflow"
            $folder = $dataflow.Directory.FullName.Replace("$($factory.FullName)\dataflows","").Trim("\").Replace("\","/")
            deploy-adfdataflow -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -dataflow $dataflow.BaseName -inputfile $dataflow.FullName -folder $folder
        }

        #deploy pipelines last
        foreach($pipeline in $factory.GetDirectories("pipelines").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
        {
            log "found pipeline $pipeline"
            $folder = $pipeline.Directory.FullName.Replace("$($factory.FullName)\pipelines","").Trim("\").Replace("\","/")
            deploy-adfpipeline -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -pipeline $pipeline.BaseName -inputfile $pipeline.FullName -folder $folder
        }
    }
}
