function Write-OutLog($msg, $foregroundcolor = "white") {
    Write-Host $msg -ForegroundColor $foregroundcolor
    "$(get-date): $msg" | Out-File -FilePath "$($pwd.Path)\adfbcdr.log" -Append
}

function Invoke-AzCmd {
    [CmdletBinding()]
    param (
        [Parameter()]
        [string]
        $cmd,
        [bool]
        $deserialize = $true
    )
    Write-OutLog "Command Running: $cmd"
    $scriptblock = { $cmd }
    $result = Invoke-Expression $cmd 2>&1
    if ($LASTEXITCODE -ne 0) {
        #get the stderr of invoke-expression, log it.
        Write-OutLog "Last exit code was $LASTEXITCODE" -ForegroundColor Red
        Write-OutLog $Error[1] -ForegroundColor Red
        Write-OutLog $Error[0] -ForegroundColor Red
        throw $Error[0]
    }

    <#Invoke-AzCmd deserialize=$false flag is intended for pulling ARM templates.
    Therefore, we will only check for continuation token where we're sending back objects#>
    switch ($deserialize) {
        $true {
                
            if ((($result | convertfrom-json ) | get-member -name nextLink) -ne $null) {
                $results += ($result | convertfrom-json ).value
                $nextlink = ($result | convertfrom-json ).nextLink
                $bDone = $false
                while ($bDone -eq $false) {
                    Write-OutLog "trying nextlink: $nextlink" -ForegroundColor Yellow
                    $tmp = (Invoke-AzCmd "az rest --uri `'`"$nextlink`"`' --method get") 
                    $results += $tmp
                    if ($tmp.nextLink -eq $null) 
                    { $bDone = $true } 
                    else
                    { $nextlink = $tmp.nextLink }
                } 
            }
            else {
                $results += ($result | convertfrom-json )
            }

            if (($results | get-member -name value) -ne $null) {
                Write-OutLog "returning from paged result with value" -foregroundcolor red
                return $results.value
            }
            else {
                Write-OutLog "returning from paged result without value" -foregroundcolor red
                return $results
            }
        }
        $false {
            return $result
        }
    }
}

function backup-adffactory($sub, $rg, $adf, $outputfile) {
    Write-OutLog "Starting backup of factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$($adf)?api-version=2018-06-01`'"
    $json = Invoke-AzCmd "az rest --uri $uri --method get" -deserialize $false
    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adffactory($sub, $rg, $adf, $inputfile, $region = "") {
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$($adf)?api-version=2018-06-01"
    $token = (Invoke-AzCmd "az account get-access-token").accessToken
    $template = (get-content -Path $inputfile | convertfrom-json)
    $template.name = $adf

    #This is required because if you are creating from scratch, it needs to be blank (creates a new MSI)
    #TODO:  Check and see if already exists.  This is not desirable when it already exists.
    if ($template.identity.type -eq "SystemAssigned") {
        $template.identity.principalId = $null
        $template.identity.tenantId = $null
    }

    #restore to different region
    if ($region -ne "") {
        $template.location = $region
    }
    #This bit enables cross-subscription restore
    $template.id = "/subscriptions/$sub/resourceGroups/$rg/providers/Microsoft.DataFactory/factories/$adf$($suffix)"
    $body = $template | convertto-json
    $headers = @{}
    $headers["Authorization"] = "Bearer $token"
    $headers["Content-Type"] = "application/json"
    Write-OutLog "Callling REST method: $uri"
    #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
    Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}

function backup-adflinkedservice($sub, $rg, $adf, $linkedservice, $outputfile) {
    Write-OutLog "Starting backup of linked service $linkedservice in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/linkedservices/$($linkedservice)?api-version=2018-06-01`'"

    $json = Invoke-AzCmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adflinkedservice($sub, $rg, $adf, $linkedservice, $inputfile) {
    $linkedservice = $linkedservice.Replace(" ", "_")
    Write-OutLog "Starting restore of linked service $linkedservice in factory $adf in resource group $rg"
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/linkedservices/$($linkedservice)?api-version=2018-06-01"

    $token = (Invoke-AzCmd "az account get-access-token").accessToken

    $template = (get-content -Path $inputfile | convertfrom-json)

    $template.name = $linkedservice

    $body = $template | convertto-json -depth 10

    $headers = @{}
    $headers["Authorization"] = "Bearer $token"
    Write-OutLog "Callling REST method: $uri"
    #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
    Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}
function backup-adfintegrationruntime($sub, $rg, $adf, $integrationruntime, $outputfile) {
    Write-OutLog "Starting backup of linked service $linkedservice in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/integrationruntimes/$($integrationruntime)?api-version=2018-06-01`'"

    $json = Invoke-AzCmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfintegrationruntime($sub, $rg, $adf, $integrationruntime, $inputfile) {
    Write-OutLog "Starting restore of linked service $linkedservice in factory $adf in resource group $rg"
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/integrationruntimes/$($integrationruntime)?api-version=2018-06-01"
    $token = (Invoke-AzCmd "az account get-access-token").accessToken
    $template = (get-content -Path $inputfile)
    $body = $template | convertto-json -depth 4
    $headers = @{}
    $headers["Authorization"] = "Bearer $token"
    $headers["content-type"] = "application/json"
    Write-OutLog "Callling REST method: $uri"
    #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
    Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}
function backup-adfdataflow($sub, $rg, $adf, $dataflow, $outputfile) {
    Write-OutLog "Starting backup of data flow $dataflow in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/dataFlows/$($dataflow)?api-version=2018-06-01`'"

    $json = Invoke-AzCmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfdataflow($sub, $rg, $adf, $dataflow, $inputfile, $folder = $null) {
    $dataflow = $dataflow.Replace(" ", "_")
    Write-OutLog "Starting restore of data flow $dataflow in factory $adf in resource group $rg"
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/dataFlows/$($dataflow)?api-version=2018-06-01"

    $token = (Invoke-AzCmd "az account get-access-token").accessToken

    $template = (get-content -Path $inputfile | convertfrom-json)
    $template.name = $dataflow

    if ($folder -ne $null) {
        if ($template.properties.folder -ne $null) {
            $template.properties.folder.name = $folder
        }
        else {
            $template.properties | add-member -Name "folder" -Value ("{ `"name`": `"$folder`" }" | convertfrom-json) -MemberType NoteProperty
        }

    }
    $body = $template | convertto-json -Depth 4

    $headers = @{}
    $headers["Authorization"] = "Bearer $token"

    Write-OutLog "Callling REST method: $uri"
    #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
    Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}

function backup-adfdataset($sub, $rg, $adf, $dataset, $outputfile) {
    Write-OutLog "Starting backup of dataset $dataset in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01`'"

    $json = Invoke-AzCmd "az rest --uri $uri --method get" -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfdataset($sub, $rg, $adf, $dataset, $inputfile, $folder = $null) {
    Write-OutLog "Starting deploy of data set $dataset in factory $adf in resource group $rg"
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01"

    $token = (Invoke-AzCmd "az account get-access-token").accessToken

    $template = get-content -Path $inputfile | convertfrom-json

    if ($folder -ne $null) {
        if ($template.properties.folder -ne $null) {
            $template.properties.folder.name = $folder
        }
        else {
            $template.properties | add-member -Name "folder" -Value ("{ `"name`": `"$folder`" }" | convertfrom-json) -MemberType NoteProperty
        }
        $body = $template | convertto-json -Depth 4
    }
    else {
        $body = get-content -Path $inputfile
    }

    $headers = @{}
    $headers["Authorization"] = "Bearer $token"

    Write-OutLog "Callling REST method: $uri"
    #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
    Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}

function backup-adfpipeline($sub, $rg, $adf, $pipeline, $outputfile) {
    Write-OutLog "Starting backup of pipeline $pipeline in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01`'"

    #"`'https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}?api-version=2018-06-01`'"
    $json = Invoke-AzCmd "az rest --uri $uri --method get"  -deserialize $false

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function Deploy-AdfPipeline {
    [CmdletBinding()]
    param (
        $sub, 
        $rg, 
        $adf, 
        $pipeline, 
        $inputfile, 
        $folder = $null
    )
    Write-OutLog "Starting restore of pipeline $pipeline in factory $adf in resource group $rg"
    $pipeline = $pipeline.Replace(" ", "_") 
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01"
    $token = (Invoke-AzCmd "az account get-access-token").accessToken
    $template = (get-content -Path $inputfile | convertfrom-json)
    $body = $template
    $headers = @{}
    $headers["Authorization"] = "Bearer $token"

    Write-OutLog "Callling REST method: $uri"
    #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
    Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
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
    Write-OutLog "Starting backup of trigger $trigger in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "`'https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/triggers/$($trigger)?api-version=2018-06-01`'"
    #"`'https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}?api-version=2018-06-01`'"
    $json = Invoke-AzCmd "az rest --uri $uri --method get"  -deserialize $false
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
    Write-OutLog "Starting restore of trigger $trigger in factory $adf in resource group $rg"
    $pipeline = $pipeline.Replace(" ", "_")
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/triggers/$($trigger)?api-version=2018-06-01"
    $token = (Invoke-AzCmd "az account get-access-token").accessToken
    $template = (get-content -Path $inputfile | convertfrom-json)
    $template.name = $pipeline
    if ($null -ne $folder) {
        if ($null -ne $template.properties.folder ) {
            $template.properties.folder.name = $folder
        }
        else {
            $template.properties | add-member -Name "folder" -Value ("{ `"name`": `"$folder`" }" | convertfrom-json) -MemberType NoteProperty
        }
    }
    $body = $template | convertto-json -Depth 4
    $headers = @{}
    $headers["Authorization"] = "Bearer $token"

    Write-OutLog "Callling REST method: $uri"
    #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
    Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}

function ensure-adfdirectory($srcpath) {
    
    if (Test-Path -Path $srcpath) {
        Write-OutLog "Path is found; trying to verify subfolders under $srcpath"
        if (!(Test-Path -Path "$srcpath\pipelines")) { Write-OutLog "Creating pipelines folder:  $srcpath\pipelines"; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\datasets")) { Write-OutLog "Creating datasets folder:  $srcpath\datasets"; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\linkedservices")) { Write-OutLog "Creating linkedservices folder: $srcpath\linkedservices "; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\dataflows")) { Write-OutLog "Creating linkedservices folder:  $srcpath\dataflows"; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\integrationruntimes")) { Write-OutLog "Creating linkedservices folder:  $srcpath\integrationruntimes"; new-item -path "$srcpath" -ItemType Directory }
        if (!(Test-Path -Path "$srcpath\triggers")) { Write-OutLog "Creating linkedservices folder:  $srcpath\triggers"; new-item -path "$srcpath" -ItemType Directory }
    }
    else {
        Write-OutLog "Creating $srcpath from scratch...";
        new-item -path "$srcpath" -ItemType Directory
        new-item -path "$srcpath\pipelines" -ItemType Directory
        new-item -path "$srcpath\datasets" -ItemType Directory
        new-item -path "$srcpath\linkedservices" -ItemType Directory
        new-item -path "$srcpath\dataflows" -ItemType Directory
        new-item -path "$srcpath\integrationruntimes" -ItemType Directory
        new-item -path "$srcpath\triggers" -ItemType Directory
    }
}


function check-pipelinelastrun($adf, $rg, $pipeline, [int]$months = 8) {

    $todayDateStamp = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH\:mm\:ss.fffffffZ")  #az datafactory command expects ISO8601 timestamp format
    $oldDateStamp = "2015-01-01T00:00:00.00Z"  #predates ARM, should be safe as a baseline date range.

    $runs = Invoke-AzCmd "az datafactory pipeline-run query-by-factory --resource-group $rg --factory-name $adf --last-updated-after $oldDateStamp --last-updated-before $todayDateStamp --filters operand=`"PipelineName`" operator=`"Equals`" values=`"$pipeline`""
    
    Write-OutLog "Pipeline $pipeline has this many runs: $($runs.Value.Count)"
    if ($runs.Value.Count -gt 0) {
        $lastRun = [datetime]::parse(($runs.value | Sort-Object -Property runEnd -Descending)[0].runEnd)
    }
    else { 
        return $false 
    }
    
    return ([DateTime]::Compare($lastRun, [DateTime]::Now.AddMonths($months * -1)) -gt 0)
}

function get-datasets($json) {
    $type = $json.id.split("/")[9]
    $datasets = @()

    switch ($type) {
        "pipelines" {
            foreach ($activity in $json.properties.activities) {
                Write-OutLog "checking activity $($activity.name) for pipeline $($json.name)"
                foreach ($input in $activity.inputs) {
                    if ($input.type -eq "DatasetReference") { 
                        Write-OutLog "found dataset $($input.referenceName) for pipeline $($json.name)"
                        $datasets += $input.referenceName
                    }
                }

                foreach ($output in $activity.outputs) {
                    if ($output.type -eq "DatasetReference") { 
                        Write-OutLog "found dataset $($output.referenceName) for pipeline $($json.name)"
                        $datasets += $output.referenceName
                    }
                }
            }
        }
        "dataflows" {
            foreach ($source in $json.properties.typeProperties.sources) {
                if ($source.dataset.type -eq "DatasetReference") { $datasets += $source.dataset.referenceName }
            }

            foreach ($sink in $json.properties.typeProperties.sinks) {
                if ($sink.dataset.type -eq "DatasetReference") { $datasets += $sink.dataset.referenceName }
            }
            
        }
    }
    return $datasets
}

function get-dataflows($json) {
    $type = $json.id.split("/")[9]
    $dataflows = @()

    switch ($type) {
        "pipelines" {
            foreach ($activity in $json.properties.activities) {
                foreach ($property in $activity.typeProperties) {
                    if ($property.dataflow -ne $null) { 
                        $dataflows += $property.dataflow.referenceName
                    }
                }
            }
        }
    }
    return $dataflows
}


function get-linkedservices($json) {
    $type = $json.id.split("/")[9]
    $linkedservices = @()

    switch ($type) {
        "pipelines" {
            foreach ($activity in $json.properties.activities) {
                foreach ($linkedservice in $activity.linkedServiceName) {
                    if ($linkedservice.type -eq "LinkedServiceReference") { $linkedservices += $linkedservice.referenceName }
                }
            }
        }
        "datasets" {
            foreach ($linkedservice in $json.properties.linkedServiceName) {
                if ($linkedservice.type -eq "LinkedServiceReference") { $linkedservices += $linkedservice.referenceName }
            }
        }
        "dataflows" {
            foreach ($source in $json.properties.typeProperties.sources) {
                if ($source.linkedService.type -eq "LinkedServiceReference") { $linkedservices += $source.linkedService.referenceName }
            }

            foreach ($sink in $json.properties.typeProperties.sinks) {
                if ($sink.linkedService.type -eq "LinkedServiceReference") { $linkedservices += $sink.linkedService.referenceName }
            }
        }
    }
    return $linkedservices
}

function get-dataflowdatasets($dataflows, $factory, $resourceGroup, $subscription) {
    $datasets = @()
    #get dataflow JSON   

    foreach ($dataflow in $dataflows) {
        $dataflowuri = "`'https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory)/dataFlows/$($dataflow)?api-version=2018-06-01`'"
        $dataflowobj = (Invoke-AzCmd "az rest --uri $dataflowuri --method get")
        $datasets += (get-datasets $dataflowobj)
    }
    return $datasets
}

function backup-factories($sub, $resourceGroup, $srcfolder, $filter = $false, $lookbackMonths = 12) {

    Write-OutLog "Backup factories running on sub: $sub with RG: $resourceGroup with source folder: $srcfolder"
    $factories = (Invoke-AzCmd "az resource list --resource-group $resourceGroup --resource-type `"Microsoft.Datafactory/factories`"")
    foreach ($factory in $factories) {
        Write-OutLog "Starting backup of Factory $($factory.name) in resource group $resourceGroup"
        
        #make sure all subfolders exist first...
        ensure-adfdirectory -srcpath "$srcfolder\$($factory.name)"

        #backup pipelines first
        $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/pipelines?api-version=2018-06-01"
        foreach ($pipeline in (Invoke-AzCmd "az rest --uri $uri --method get")) {
            #Don't back up if not run in last X months...
            if (($Filter -and (check-pipelinelastrun -adf $factory.name -rg $resourceGroup -pipeline $pipeline.name -months $lookbackMonths)) -or !($Filter)) {
                Write-OutLog "Found pipeline: $($pipeline.name)" -ForegroundColor Green
                backup-adfpipeline -sub $subscription -rg $resourceGroup -adf $factory.name -pipeline $pipeline.name -outputfile "$srcfolder\$($factory.name)\pipelines\$($pipeline.name).json"
            }
        }

        #backup dataflows
        $dataflowuri = "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/dataflows?api-version=2018-06-01"
        foreach ($dataflow in (Invoke-AzCmd "az rest --uri $dataflowuri --method get")) {
            Write-OutLog "Found data flow: $($dataflow.name)" -ForegroundColor Green
            backup-adfdataflow -sub $subscription -rg $resourceGroup -adf $factory.name -dataflow $dataflow.name -outputfile "$srcfolder\$($factory.name)\dataflows\$($dataflow.name).json"
        }

        #backup datasets
        $dataseturi = "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/datasets?api-version=2018-06-01"
        foreach ($dataset in (Invoke-AzCmd "az rest --uri $dataseturi --method get")) {
            Write-OutLog "Found data set: $($dataset.name)" -ForegroundColor Green
            backup-adfdataset -sub $subscription -rg $resourceGroup -adf $factory.name -dataset $dataset.name -outputfile "$srcfolder\$($factory.name)\datasets\$($dataset.name).json"
        }

        #backup linked services
        $linkedservicesuri = "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/linkedservices?api-version=2018-06-01"
        foreach ($service in (Invoke-AzCmd "az rest --uri $linkedservicesuri --method get")) {
            Write-OutLog "Found linkedservice: $($service.name)" -ForegroundColor Green
            backup-adflinkedservice -sub $subscription -rg $resourceGroup -adf $factory.name -linkedservice $service.name -outputfile "$srcfolder\$($factory.name)\linkedservices\$($service.name).json"
        }

        #backup integration runtimes
        $linkedservicesuri = "https://management.azure.com/subscriptions/$subscription/resourceGroups/$resourceGroup/providers/Microsoft.DataFactory/factories/$($factory.name)/integrationruntimes?api-version=2018-06-01"
        foreach ($runtime in (Invoke-AzCmd "az rest --uri $linkedservicesuri --method get")) {
            Write-OutLog "Found integration runtime: $($runtime.name)" -ForegroundColor Green
            backup-adflinkedservice -sub $subscription -rg $resourceGroup -adf $factory.name -linkedservice $service.name -outputfile "$srcfolder\$($factory.name)\integrationruntimes\$($runtime.name).json"
        }

        #backup the factory itself
        backup-adffactory -sub $subscription -rg $resourceGroup -adf $factory.name -outputfile "$srcfolder\$($factory.name)\$($factory.name).json"
    }

}

function restore-factories {
    [CmdletBinding()]
    param (
        $sub, 
        $resourceGroup, 
        $srcfolder, 
        $suffix = "", 
        $region = ""
    )
    Write-OutLog "Restore factories running on sub: $sub with RG: $resourceGroup with source folder: $srcfolder"
    $srcdir = get-item -Path $srcfolder

    foreach ($factory in $srcdir.GetDirectories()) {
        Write-OutLog "Starting restore of Factory $($factory.Name) in resource group $resourceGroup"

        try {
            #suffix is included here to ensure the ADF name is unique globally.  Backup and restore won't work if you haven't deleted the source factory otherwise.
            deploy-adffactory -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -inputfile "$($factory.FullName)\$($factory.Name).json" -region $region
        }
        catch {
            Write-OutLog $_.Exception -ForegroundColor Red
            continue
        }

        # New/Restore ADF
        # Pause needed as it fails if created too soon after ADF is created
        Write-OutLog "Pause for 60 seconds..."
        Start-Sleep -Seconds 30
        Write-OutLog "30 seconds left..."
        Start-Sleep -Seconds 20
        Write-OutLog "10 seconds left..."
        Start-Sleep -Seconds 10
        $factoryPrincipalId = (Invoke-AzCmd -cmd "az datafactory list" -deserialize $true | Where-Object { $_.Name -eq "$($factory.name)$suffix" }).identity.principalId 
        if($null -eq $factoryPrincipalId){ Write-OutLog -msg "Unable to get ID to "}
        #deploy integration runtimes
        foreach ($runtime in $factory.GetDirectories("integrationruntimes").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories)) {
            # Old/Existing ADF
            $scope = (Get-Content $runtime.FullName | ConvertFrom-Json -Depth 4).id
            Write-OutLog "Scope is $scope"
            Write-OutLog "Factory Pricipal ID is $factoryPrincipalId"
            Write-OutLog "Setting Role for Integrated Runtime"
            Invoke-AzCmd -cmd "az role assignment create --role 'Contributor' --assignee $factoryPrincipalId --scope $scope" -deserialize $false
            deploy-adfintegrationruntime -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -integrationruntime $runtime.BaseName -inputfile $runtime.FullName
        }

        #Set Role Assignment to System Indentity
        Set-ADFManagedIdentity -factoryName $factory.name -newFactoryName "$($factory.name)$suffix"

        #deploy linked services
        foreach ($service in $factory.GetDirectories("linkedservices").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories)) {
            deploy-adflinkedservice -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -linkedservice $service.BaseName -inputfile $service.FullName
        }
      
        Write-OutLog "Deploying backed up Data sets..."
        foreach ($dataset in $factory.GetDirectories("datasets").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories)) {
            Write-OutLog "found dataset $dataset"
            $folder = $dataset.Directory.FullName.Replace("$($factory.FullName)\datasets", "").Trim("\").Replace("\", "/")
            if($null -ne $folder){ Write-OutLog "Folder is $folder" }
            deploy-adfdataset -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -dataset $dataset.BaseName -inputfile $dataset.FullName -folder $folder
        }

        Write-OutLog "Deploying backed up Data flows..."
        foreach ($dataflow in $factory.GetDirectories("dataflows").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories)) {
            Write-OutLog "found dataflow $dataflow"
            $folder = $dataflow.Directory.FullName.Replace("$($factory.FullName)\dataflows", "").Trim("\").Replace("\", "/")
            if($null -ne $folder){ Write-OutLog "Folder is $folder" }
            deploy-adfdataflow -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -dataflow $dataflow.BaseName -inputfile $dataflow.FullName -folder $folder
        }

        #deploy pipelines
        $splat = @{
            SouceDirectory = $srcfolder
            Subscription   = $subscription
            ResourceGroup  = $resourceGroup
            ADF            = "$($factory.name)$suffix"
        }
        Deploy-AdfPipelineDependancy @splat

        # deploy triggers last
        foreach ($trigger in $factory.GetDirectories("triggers").GetFiles("*.json", [System.IO.SearchOption]::AllDirectories)) {
            Deploy-AdfTrigger -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -trigger $trigger.BaseName -inputfile $trigger.FullName
        }
    }
}

function Deploy-AdfPipelineDependancy {
    [CmdletBinding()]
    param (
        $SouceDirectory,
        $Subscription,
        $ResourceGroup,
        $ADF
    )
    $folder = Get-childitem -path $SouceDirectory 'pipelines' -Recurse -Directory
    $list = Get-ChildItem *.json -Path $folder
    $dependsOn = @()
    #build hashtable
    foreach ($path in $list) {
        $json = Get-Content $path.FullName
        $get = $json | ConvertFrom-Json
        if ($get.properties.activities.typeProperties.activities.typeProperties.pipeline) {
            #"Found Dependancy"
            $dependsOn += [PSCustomObject][Ordered]@{
                Name      = $path.BaseName
                DependsOn = $get.properties.activities.typeProperties.activities.typeProperties.pipeline.referenceName
                Deployed  = $null
            }
        }
        else { 
            #"No Depends On" 
            $dependsOn += [PSCustomObject][Ordered]@{
                Name      = $path.BaseName
                DependsOn = $null
                Deployed  = $null
            }
        }
    }
    #deploy
    $deployCount = ($dependsOn | Where-Object { $null -eq $_.Deployed }).count
    while ($deployCount -gt 0) {
        foreach ($path in $list) {
            Write-OutLog -msg "Attempting to deploy pipeline $($path.BaseName)"
            $get = $dependson | Where-Object { $_.Name -eq $path.BaseName }
            if ($get.Deployed -ieq "X") { Write-OutLog -msg "$($path.BaseName) Pipeline Deployed" -ForegroundColor Green }
            elseif ($null -ne $get.DependsOn ) {
                Write-OutLog -msg "$($path.BaseName) Has Dependancy $($get.DependsOn)"
                $depend = $get.DependsOn
                $getdepend = $dependson | Where-object { $_.Name -eq $depend }
                if ($null -eq $getdepend.Deployed) {
                    Write-OutLog -msg "$($path.BaseName) dependancy, $($get.DependsOn), not deployed yet" -ForegroundColor Yellow
                }
                else {
                    Write-OutLog -msg "Dependancy $($get.DependsOn) deployed. Deploying pipeline $($path.BaseName)!" -ForegroundColor Cyan
                    try {
                        deploy-adfpipeline -sub $subscription -rg $resourceGroup -adf $adf -pipeline $path.BaseName -inputfile $path.FullName -folder $folder
                        $get.Deployed = "X"
                    }
                    catch {
                        $message = $_.Exception
                        Write-OutLog -msg "Failed to deploy pipeline $($path.BaseName)"
                        Write-OutLog $message -ForegroundColor Red
                        throw "Failed to deploy pipeline $($path.BaseName): $message"
                    }
                }
            }
            else {
                Write-OutLog -msg "$($path.BaseName) Does not have dependancy. Deploying pipeline." -ForegroundColor Green
                try {
                    deploy-adfpipeline -sub $subscription -rg $resourceGroup -adf $adf -pipeline $path.BaseName -inputfile $path.FullName -folder $folder
                    $get.Deployed = "X"
                }
                catch {
                    $message = $_.Exception
                    Write-OutLog -msg "Failed to deploy pipeline $($path.BaseName)"
                    Write-OutLog $message -ForegroundColor Red
                    throw "Failed to deploy pipeline $($path.BaseName): $message"
                }
            }
        }
        $deployCount = ($dependsOn | Where-Object { $null -eq $_.Deployed }).count
    }
}

function Set-ADFManagedIdentity {
    [CmdletBinding()]
    param (
        $factoryName,
        $newFactoryName
    )
    # figure out if it's system or user assigned, currently assuming system
    #$type = (Get-Content (Get-ChildItem g-adf-2d-gdp-redaanalytics-01.json -Path $path).FullName | ConvertFrom-Json).identity.type
    #if ($type -eq 'SystemAssigned') {
    Write-OutLog "Setting System Managed Identity Roles..."
    #Not using user assgined
    ##User Assgined Managed ID
    #$managedIdName = (az identity list | ConvertFrom-Json | Where-Object{$_.principalId -eq $midPrincipalId}).name
    #az ad sp list --display-name $managedIdName --query [].id --output tsv
    #System Assigned Managed ID
    # No output when using Invoke-AzCmd, need that output
    $oldaid = az ad sp list --display-name $factoryName --query [].id --output tsv
    #az role assignment list --assignee # uses graph API
    # does not work returnns [] # only works if --all is added
    $roles = az role assignment list --assignee $oldaid --all | ConvertFrom-Json -Depth 4
    Write-OutLog "Role Information: "
    Write-OutLog $roles
    $aid = az ad sp list --display-name $newFactoryName --query [].id --output tsv
    # Assign Role
    foreach ($role in $roles) {
        Invoke-AzCmd -cmd "az role assignment create --assignee ""$aid"" --role ""$($role.roleDefinitionName)"" --scope ""$($role.scope)"" "
    }

}