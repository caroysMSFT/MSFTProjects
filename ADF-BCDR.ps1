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

function backup-adffactory($sub, $rg, $adf, $outputfile)
{
    log "Starting backup of factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$($adf)?api-version=2018-06-01"

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

   $template = (get-content -Path $inputfile | convertfrom-json)


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
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/linkedservices/$($linkedservice)?api-version=2018-06-01"

    $json = run-azcmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adflinkedservice($sub, $rg, $adf, $linkedservice, $inputfile)
{
   log "Starting restore of linked service $linkedservice in factory $adf in resource group $rg"
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/linkedservices/$($linkedservice)?api-version=2018-06-01"

   $token = (run-azcmd "az account get-access-token").accessToken

   $body = get-content -Path $inputfile

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
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/dataFlows/$($dataflow)?api-version=2018-06-01"

    $json = run-azcmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfdataflow($sub, $rg, $adf, $dataflow, $inputfile)
{
   log "Starting restore of data flow $dataflow in factory $adf in resource group $rg"
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/dataFlows/$($dataflow)?api-version=2018-06-01"

   $token = (run-azcmd "az account get-access-token").accessToken

   $body = get-content -Path $inputfile

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
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01"

    $json = run-azcmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfdataset($sub, $rg, $adf, $dataset, $inputfile)
{
   log "Starting deploy of data set $dataset in factory $adf in resource group $rg"
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01"

   $token = (run-azcmd "az account get-access-token").accessToken

   $body = get-content -Path $inputfile

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
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01"

    #"https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}?api-version=2018-06-01"
    $json = run-azcmd "az rest --uri $uri --method get"  -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfpipeline($sub, $rg, $adf, $pipeline, $inputfile)
{
   log "Starting restore of pipeline $pipeline in factory $adf in resource group $rg"
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01"
   $token = (run-azcmd "az account get-access-token").accessToken

   $body = get-content -Path $inputfile
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
        if(!(Test-Path -Path "$srcpath\pipelines")) { log "Creating pipelines folder..."; new-item -path "$srcpath" -ItemType Directory}
        if(!(Test-Path -Path "$srcpath\datasets")) { log "Creating datasets folder..."; new-item -path "$srcpath" -ItemType Directory}
        if(!(Test-Path -Path "$srcpath\linkedservices")) { log "Creating linkedservices folder...";new-item -path "$srcpath" -ItemType Directory}
        if(!(Test-Path -Path "$srcpath\dataflows")) { log "Creating linkedservices folder...";new-item -path "$srcpath" -ItemType Directory}
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
    
    log "Pipeline $pipeline has this many runs: $($runs.Value.Count)"
    if($runs.Value.Count -gt 0)
    {
        $lastRun = [datetime]::parse(($runs.value | Sort-Object -Property runEnd -Descending)[0].runEnd)
    }
    else { 
        return $false 
    }
    
    return ([DateTime]::Compare($lastRun,[DateTime]::Now.AddMonths($months * -1)) -gt 0)
}


function backup-factories($sub, $resourceGroup, $srcfolder, $filter = $false, $lookbackMonths = 12)
{

    log "Backup factories running on sub: $sub with RG: $resourceGroup with source folder: $srcfolder"
    $factories = (run-azcmd "az resource list --resource-group $resourceGroup --resource-type `"Microsoft.Datafactory/factories`"")
    foreach($factory in $factories)
    {
        log "Starting backup of Factory $($factory.name) in resource group $resourceGroup"
        
        #make sure all subfolders exist first...
        ensure-adfdirectory -srcpath "$srcfolder\$($factory.name)"

        #backup pipelines first
        foreach($pipeline in (run-azcmd "az datafactory pipeline list --resource-group $resourceGroup --factory-name $($factory.name)"))
        {
            #Don't back up if not run in last X months...
            if(($Filter -and (check-pipelinelastrun -adf $factory.name -rg $resourceGroup -pipeline $pipeline.name -months $lookbackMonths)) -or !($Filter)) 
            {
                log "Found pipeline: $($pipeline.name)" -ForegroundColor Green
                backup-adfpipeline -sub $subscription -rg $resourceGroup -adf $factory.name -pipeline $pipeline.name -outputfile "$srcfolder\$($factory.name)\pipelines\$($pipeline.name).json"
            }
        }

        #backup dataflows
        $dataflowuri = "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/dataflows?api-version=2018-06-01"
        foreach($dataflow in ((run-azcmd "az rest --uri $dataflowuri --method get").value))
        {
            log "Found data flow: $($dataflow.name)" -ForegroundColor Green
            backup-adfdataflow -sub $subscription -rg $resourceGroup -adf $factory.name -dataflow $dataflow.name -outputfile "$srcfolder\$($factory.name)\dataflows\$($dataflow.name).json"
        }

        #backup datasets
        foreach($dataset in (run-azcmd "az datafactory dataset list --resource-group $resourceGroup --factory-name $($factory.name)"))
        {
            log "Found data set: $($dataset.name)" -ForegroundColor Green
            backup-adfdataset -sub $subscription -rg $resourceGroup -adf $factory.name -dataset $dataset.name -outputfile "$srcfolder\$($factory.name)\datasets\$($dataset.name).json"
        }

        #backup linked services
        foreach($service in (run-azcmd "az datafactory linked-service list --resource-group $resourceGroup --factory-name $($factory.name)"))
        {
            log "Found linkedservice: $($service.name)" -ForegroundColor Green
            backup-adflinkedservice -sub $subscription -rg $resourceGroup -adf $factory.name -linkedservice $service.name -outputfile "$srcfolder\$($factory.name)\linkedservices\$($service.name).json"
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
        foreach($service in $factory.GetDirectories("linkedservices").GetFiles())
        {
            deploy-adflinkedservice -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -linkedservice $service.BaseName -inputfile $service.FullName
        }

        #deploy dataflows
        log "Deploying backed up Data flows..."
        foreach($dataset in $factory.GetDirectories("datasets").GetFiles())
        {
            deploy-adfdataset -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -dataset $dataset.BaseName -inputfile $dataset.FullName
        }

                #deploy datasets
        foreach($dataflow in $factory.GetDirectories("dataflows").GetFiles())
        {
            deploy-adfdataflow -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -dataflow $dataflow.BaseName -inputfile $dataflow.FullName
        }

        #deploy pipelines last
        foreach($pipeline in $factory.GetDirectories("pipelines").GetFiles())
        {
            deploy-adfpipeline -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -pipeline $pipeline.BaseName -inputfile $pipeline.FullName
        }
    }
}
