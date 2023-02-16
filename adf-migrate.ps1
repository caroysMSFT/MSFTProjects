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

function despace-template([ref]$json)
{
    foreach($item in $json.Value.PSObject.Properties)
    {
        if($item.MemberType -eq "NoteProperty")
        {
            if($item.name -like "referenceName")
            {
                write-host "replacing spaces for $($json.Value.referenceName)"
                $json.Value.referenceName = $json.Value.referenceName.Replace(" ","_")
            }

            if($item.TypeNameOfValue -eq "System.Object[]")
            {
                foreach($object in $item.Value)
                {
                    despace-template ([ref]$object)
                }
            }
            else
            {
                despace-template ([ref]$item.Value)
            }
        }
    }
}

function unpin-shir([ref]$json)
{
    foreach($item in $json.Value.PSObject.Properties)
    {
        if($item.MemberType -eq "NoteProperty")
        {
            if($item.name -like "integrationRuntime")
            {
                $json.Value.PSOBJECT.Properties.Remove('integrationRuntime')
            }

            if($item.TypeNameOfValue -eq "System.Object[]")
            {
                foreach($object in $item.Value)
                {
                    unpin-shir ([ref]$object)
                }
            }
            else
            {
                unpin-shir ([ref]$item.Value)
            }
        }
    }
}

function get-references($json, $match)
{
    $references = @()
    foreach($item in $json.PSObject.Properties)
    {
        if($item.MemberType -eq "NoteProperty")
        {
            if($item.name -eq "type" -and $json.type -eq $match)
            {
                $references += $json.referenceName
            }

            if($item.TypeNameOfValue -eq "System.Object[]")
            {
                foreach($object in $item.Value)
                {
                    $references += get-references $object -match $match
                }
            }
            else
            {
                $references += get-references $item.Value -match $match
            }
        }
    }
    return ($references | select-object -Unique)
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

   $token = (run-azcmd "az account get-access-token" -deserialize $false | convertfrom-json).accessToken

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

   $token = (run-azcmd "az account get-access-token" -deserialize $false | convertfrom-json).accessToken
   $template = (get-content -Path $inputfile | convertfrom-json)
   despace-template ([ref]$template)
   
   $template.properties.PSObject.Properties.Remove('connectVia')

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

   $token = (run-azcmd "az account get-access-token" -deserialize $false | convertfrom-json).accessToken
   $template = (get-content -Path $inputfile | convertfrom-json)
   despace-template ([ref]$template)
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
   $dataset = $dataset.Replace(" ","_")
   log "Starting deploy of data set $dataset in factory $adf in resource group $rg"
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01"

   $token = (run-azcmd "az account get-access-token" -deserialize $false | convertfrom-json).accessToken
   $template = (get-content -Path $inputfile | convertfrom-json)
   despace-template ([ref]$template)
   $template.name = $dataset
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


function backup-adfpipeline($sub, $rg, $adf, $pipeline, $outputfolder)
{
    log "Starting backup of pipeline $pipeline in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01`'"

    #"`'https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}?api-version=2018-06-01`'"
    $json = run-azcmd "az rest --uri $uri --method get"

    #See if it has a Folder element declared; if so, create a subfolder.
    if($json.folder -ne $null)
    {
        new-item -ItemType Directory -Path "$outputfolder\$($pipeline.folder.name)" -force
        $outputfile = "$outputfolder\$($pipeline.folder.name)\$($json.name).json"
    }
    else
    {
        $outputfile = "$outputfolder\$($json.name).json"
    }

    # If it's already backed up - noop
    if(test-path $outputfile)
    {
        Write-host "Pipeline file already exists, skipping backup: $outputfile"
    }
    else
    {
        # Backup any referenced pipelines - there can be conditionals which mean references run less often than the parent
        foreach($pipelineref in (get-references -json $json -match "PipelineReference"))
        {
            Write-Host "Found pipeline reference $pipelineref,  Backing it up first..." -foreground Yellow
            backup-adfpipeline -sub $sub -rg $rg -adf $adf -pipeline $pipelineref -outputfolder $outputfolder
        }
        <# Finally, deploy original pipeline
        
        We can use this verbatim - no fixup needed.
    
        There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
        Meanwhile, it's data which is useful in source control as forensic/historical info.#>
        ($json | convertto-json -depth 100) | out-file $outputfile
    }
}

function Deploy-AdfPipeline {
    [CmdletBinding()]
    param (
        $sub, 
        $rg, 
        $adf, 
        $pipeline, 
        $inputfile, 
        [ref] $donelist
    )
    Log "Starting restore of pipeline $pipeline in factory $adf in resource group $rg, file: $inputfile"
    
    $token = (run-azcmd "az account get-access-token" -deserialize $false | convertfrom-json).accessToken
    $template = (get-content -Path $inputfile | convertfrom-json)
    
    $pipeline = $pipeline.Replace(" ","_")
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01"
    $template.name = $pipeline

    $headers = @{}
    $headers["Authorization"] = "Bearer $token"

    $fileobj = get-item $inputfile
       
    foreach($reference in get-references -json $template -match "PipelineReference")
    {
        if($donelist -notcontains $reference)
        {
            $splat = @{
                sub            = $sub
                rg             = $rg  
                adf            = $adf
                pipeline       = $reference
                inputfile      = "$($fileobj.Directory.FullName)\$reference.json"  # Bug: the reference could live in a subfolder - or not.
                donelist       = $donelist
            }
            $donelist
            deploy-adfpipeline @splat
            $donelist
        }
    }

    despace-template ([ref]$template)
    unpin-shir ([ref]$template)
    $body =  $template | convertto-json -depth 100

    if($donelist -notcontains $pipeline)
    {
        Log "Callling REST method: $uri"
        #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
        try {
            Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers -Verbose

            $donelist.Value += $pipeline           
        }
        catch {
            $message = $_.Exception
            Log $message -ForegroundColor Red
            throw "$message"
            $body
        }
    }
}

function ensure-adfdirectory($srcpath) {
    
    if (Test-Path -Path $srcpath) {
        log "Path is found; trying to verify subfolders under $srcpath"
        if (!(Test-Path -Path "$srcpath\pipelines")) { log "Creating pipelines folder:  $srcpath\pipelines"; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\datasets")) { log "Creating datasets folder:  $srcpath\datasets"; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\linkedservices")) { log "Creating linkedservices folder: $srcpath\linkedservices "; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\dataflows")) { log "Creating linkedservices folder:  $srcpath\dataflows"; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\triggers")) { log "Creating linkedservices folder:  $srcpath\triggers"; new-item -path "$srcpath" -ItemType Directory }
    }
    else {
        log "Creating $srcpath from scratch...";
        new-item -path "$srcpath" -ItemType Directory
        new-item -path "$srcpath\pipelines" -ItemType Directory
        new-item -path "$srcpath\datasets" -ItemType Directory
        new-item -path "$srcpath\linkedservices" -ItemType Directory
        new-item -path "$srcpath\dataflows" -ItemType Directory
        new-item -path "$srcpath\triggers" -ItemType Directory
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


function get-dataflowdatasets($dataflows, $factory, $resourceGroup, $subscription)
{
    $datasets = @()
    #get dataflow JSON   

    foreach($dataflow in $dataflows)
    {
        $dataflowuri = "`'https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory)/dataFlows/$($dataflow)?api-version=2018-06-01`'"
        $dataflowobj = (run-azcmd "az rest --uri $dataflowuri --method get")
        $datasets += (get-references -json $dataflowobj -match "DatasetReference")
    }
    return $datasets
}




function backup-factories($subscription, $resourceGroup, $srcfolder, $filter = $false, $lookbackMonths = 12)
{
    log "Backup factories running on sub: $subscription with RG: $resourceGroup with source folder: $srcfolder"
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
                $outputfolder = "$srcfolder\$($factory.name)\pipelines"

                backup-adfpipeline -sub $subscription -rg $resourceGroup -adf $factory.name -pipeline $pipeline.name -outputfolder $outputfolder
            }
        }
        $pipelinesfolder =  get-item "$srcfolder\$($factory.name)\pipelines"
        foreach($pipeline in $pipelinesfolder.GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
        {
            $json =  Get-Content -Path $pipeline.FullName | convertfrom-json
            $datasets += (get-references -json $json -match "DatasetReference" )
            $dataflows += (get-references -json $json -match "DataFlowReference" )
            $datasets += (get-dataflowdatasets -dataflows $dataflows -factory $factory.name -resourcegroup $resourceGroup -subscription $subscription)
            $linkedservices += (get-references -json $json -match "LinkedServiceReference" )
        }

        #Data flows can reference LinkedServices and DataSets
        #What if we have a linkedservice or dataset, which isn't referenced by a pipeline recently run, but it is referenced by a dataflow?
        # this is a real problem.  We should iterate when we're done for referenced data flows, and build the list of datasets and linked services off it.

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
                $datasets += (get-references -json $pipeline -match "DataFlowReference" )
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

        $dataflowfolder =  get-item "$srcfolder\$($factory.name)\dataflows"
        foreach($dataflow in $dataflows)
        {
            foreach($dataflowfile in $dataflowfolder.GetFiles("$dataflow.json", [System.IO.SearchOption]::AllDirectories))
            {
                $json = Get-Content -path $dataflowfile.FullName | convertfrom-json
                $datasets += (get-dataflowdatasets -dataflows $dataflows -factory $factory.name -resourcegroup $resourceGroup -subscription $subscription)
                $linkedservices += (get-references -json $json -match "LinkedServiceReference" )
            }
        }

        #backup datasets
        $dataseturi = "`'https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/datasets?api-version=2018-06-01`'"

        foreach($dataset in ((run-azcmd "az rest --uri $dataseturi --method get")))
        {
            if($datasets -contains $dataset.name)
            {
                log "Found data set: $($dataset.name)" -ForegroundColor Green

                # add its linked services to our recently used list
                $linkedservices += (get-references -json $pipeline -match "LinkedServiceReference" )

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

        $datasetfolder =  get-item "$srcfolder\$($factory.name)\datasets"
        foreach($dataset in $datasets)
        {
            foreach($datasetfile in $datasetfolder.GetFiles("$dataset.json", [System.IO.SearchOption]::AllDirectories))
            {
                $json = Get-Content -path $datasetfile.FullName | convertfrom-json
                $linkedservices += (get-references -json $json -match "LinkedServiceReference" )
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
        
        #backup triggers
        $triggeruri = "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/triggers?api-version=2018-06-01"
        foreach ($trigger in (run-azcmd "az rest --uri $triggeruri --method get")) {
            log "Found trigger: $($trigger.name)" -ForegroundColor Green
            backup-adftrigger -sub $subscription -rg $resourceGroup -adf $factory.name -trigger $trigger.name -outputfile "$srcfolder\$($factory.name)\triggers\$($trigger.name).json"
        }

        #backup the factory itself
        backup-adffactory -sub $subscription -rg $resourceGroup -adf $factory.name -outputfile "$srcfolder\$($factory.name)\$($factory.name).json"
    }

}


function restore-factories($subscription, $resourceGroup, $srcfolder, $suffix = "", $region = "")
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
            $donelist = @()
            deploy-adfpipeline -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -pipeline $pipeline.BaseName -inputfile $pipeline.FullName  -donelist ([ref]$donelist)
        }
    }
}

function Backup-AdfTrigger {
    [CmdletBinding()]
    param (
        $sub, 
        $rg, 
        $adf, 
        $trigger, 
        $outputfile
    )
    log "Starting backup of trigger $trigger in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/triggers/$($trigger)?api-version=2018-06-01`'"
    #"`'https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}?api-version=2018-06-01`'"
    $json = run-azcmd "az rest --uri $uri --method get"  -deserialize $false
    $json | out-file $outputfile
}

function Deploy-AdfTrigger {
    [CmdletBinding()]
    param (
        $sub, 
        $rg, 
        $adf, 
        $trigger, 
        $inputfile, 
        $folder = $null
    )
    log "Starting restore of trigger $trigger in factory $adf in resource group $rg"
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/triggers/$($trigger)?api-version=2018-06-01"
    $token = (run-azcmd "az account get-access-token").accessToken
    $body = get-content -Path 

    $headers = @{}
    $headers["Authorization"] = "Bearer $token"

    log "Callling REST method: $uri"
    #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
    try {
        Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers -Verbose
    }
    catch {
        $message = $_.ErrorDetails.Message
        log $message -ForegroundColor Red
        throw "$message"
    }
}

# Exists flag is there to tell this function not to deploy the factory - useful for pushing artifacts to an existing factory which had nothing to do with the source.
# There's not that much that is inherited from the factory itself (region, identity), so this is a worthwhile exercise.
function restore-factory($sub, $rg, $srcfile, $name = "", $region = "", $exists = $false)
{
    log "Restore factories running on sub: $sub with RG: $resourceGroup with source folder: $srcfolder"
    $srcfileobj = get-item -Path $srcfile

    $factory = (get-content -Path $srcfile | convertfrom-json)

    if($name -eq "")
    {
        $name = $factory.name
    }

    log "Starting restore of Factory $($factory.Name) in resource group $resourceGroup using name: $name"

    try
    {
        if($exists -ne $true)
        {
            deploy-adffactory -sub $subscription -rg $resourceGroup -adf $name -inputfile $srcfile -region $region
        }
    }
    catch
    {
        log $_.Exception -ForegroundColor Red
        continue
    }

    #deploy linked services
    foreach($service in $srcfileobj.directory.GetDirectories("linkedservices").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
    {
        deploy-adflinkedservice -sub $subscription -rg $resourceGroup -adf $name -linkedservice $service.BaseName -inputfile $service.FullName
    }
      
    log "Deploying backed up Data sets..."
    foreach($dataset in $srcfileobj.directory.GetDirectories("datasets").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
    {
        log "found dataset $dataset"
        $folder = $dataset.Directory.FullName.Replace("$($factory.FullName)\datasets","").Trim("\").Replace("\","/")
        deploy-adfdataset -sub $subscription -rg $resourceGroup -adf $name -dataset $dataset.BaseName -inputfile $dataset.FullName -folder $folder
    }

    log "Deploying backed up Data flows..."
    foreach($dataflow in $srcfileobj.directory.GetDirectories("dataflows").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
    {
        log "found dataflow $dataflow"
        $folder = $dataflow.Directory.FullName.Replace("$($factory.FullName)\dataflows","").Trim("\").Replace("\","/")
        deploy-adfdataflow -sub $subscription -rg $resourceGroup -adf $name -dataflow $dataflow.BaseName -inputfile $dataflow.FullName -folder $folder
    }

    #deploy pipelines 
    foreach($pipeline in $srcfileobj.directory.GetDirectories("pipelines").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories))
    {
        log "found pipeline $pipeline"
        $folder = $pipeline.Directory.FullName.Replace("$($factory.FullName)\pipelines","").Trim("\").Replace("\","/")
        deploy-adfpipeline -sub $subscription -rg $resourceGroup -adf $name -pipeline $pipeline.BaseName -inputfile $pipeline.FullName -folder $folder
    }
    <#
    # deploy triggers last
    foreach ($trigger in $factory.GetDirectories("triggers").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories)) {
        Deploy-AdfTrigger -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -trigger $trigger.BaseName -inputfile $trigger.FullName
    }#>
}

