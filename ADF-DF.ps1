function backup-adffactory($sub, $rg, $adf, $outputfile)
{
    Write-Host "Starting backup of factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$($adf)?api-version=2018-06-01"
    Write-Host "Callling REST method: $uri"
    $json = az rest --uri $uri --method get 

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adffactory($sub, $rg, $adf, $inputfile)
{
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$($adf)?api-version=2018-06-01"
   Write-Host "Callling REST method: $uri"
   $token = (az account get-access-token | convertfrom-json).accessToken

   $template = (get-content -Path $inputfile | convertfrom-json)
   $template.name = $adf

   #This is required because if you are creating from scratch, it needs to be blank (creates a new MSI)
   #TODO:  Check and see if already exists.  This is not desirable when it already exists.
   if($template.identity.type -eq "SystemAssigned")
   {
        $template.identity.principalId = $null
        $template.identity.tenantId = $null
   }

   #This bit enables cross-subscription restore
   $template.id = "/subscriptions/$sub/resourceGroups/$rg/providers/Microsoft.DataFactory/factories/$adf"

   $body = $template | convertto-json

   $headers = @{}
   $headers["Authorization"] = "Bearer $token"
   $headers["Content-Type"] = "application/json"

   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}


function backup-adflinkedservice($sub, $rg, $adf, $linkedservice, $outputfile)
{
    Write-Host "Starting backup of linked service $linkedservice in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/linkedservices/$($linkedservice)?api-version=2018-06-01"
    Write-Host "Callling REST method: $uri"
    $json = az rest --uri $uri --method get 

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adflinkedservice($sub, $rg, $adf, $linkedservice, $inputfile)
{
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/linkedservices/$($linkedservice)?api-version=2018-06-01"
   Write-Host "Callling REST method: $uri"
   $token = (az account get-access-token | convertfrom-json).accessToken

   $body = get-content -Path $inputfile

   $headers = @{}
   $headers["Authorization"] = "Bearer $token"

   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}


function backup-adfdataset($sub, $rg, $adf, $dataset, $outputfile)
{
    Write-Host "Starting backup of dataset $dataset in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01"
    Write-Host "Callling REST method: $uri"
    $json = az rest --uri $uri --method get 

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfdataset($sub, $rg, $adf, $dataset, $inputfile)
{
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/datasets/$($dataset)?api-version=2018-06-01"
   Write-Host "Callling REST method: $uri"
   $token = (az account get-access-token | convertfrom-json).accessToken

   $body = get-content -Path $inputfile

   $headers = @{}
   $headers["Authorization"] = "Bearer $token"

   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}


function backup-adfpipeline($sub, $rg, $adf, $pipeline, $outputfile)
{
    Write-Host "Starting backup of pipeline $pipeline in factory $adf in resource group $rg"
    # Get the pipeline via AZ CLI - gives us the cleanest JSON
    $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01"
    Write-Host "Callling REST method: $uri"
    #"https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}?api-version=2018-06-01"
    $json = az rest --uri $uri --method get 

    <# We can use this verbatim - no fixup needed.
    
    There's some extra stuff in the JSON, like ETAG, modified time, etc.  But this gets stripped when you publish it.  
    Meanwhile, it's data which is useful in source control as forensic/historical info.#>
    $json | out-file $outputfile
}

function deploy-adfpipeline($sub, $rg, $adf, $pipeline, $inputfile)
{
   $uri = "https://management.azure.com/subscriptions/$sub/resourcegroups/$rg/providers/Microsoft.DataFactory/factories/$adf/pipelines/$($pipeline)?api-version=2018-06-01"
   Write-Host "Callling REST method: $uri"
   $token = (az account get-access-token | convertfrom-json).accessToken

   $body = get-content -Path $inputfile
   $headers = @{}
   $headers["Authorization"] = "Bearer $token"

   #Using REST call direct, as AZ CLI has some validation which the pipeline JSON doesn't pass for some reason.
   Invoke-RestMethod -Method Put -Uri $uri -body $body -Headers $headers 
}

function ensure-adfdirectory($srcpath)
{
    
    if(Test-Path -Path $srcpath)
    {
        Write-Host "Path is found; trying to verify subfolders under $srcpath"
        if(!(Test-Path -Path "$srcpath\pipelines")) { Write-Host "Creating pipelines folder..."; new-item -path "$srcpath" -ItemType Directory}
        if(!(Test-Path -Path "$srcpath\datasets")) { Write-Host "Creating datasets folder..."; new-item -path "$srcpath" -ItemType Directory}
        if(!(Test-Path -Path "$srcpath\linkedservices")) { Write-Host "Creating linkedservices folder...";new-item -path "$srcpath" -ItemType Directory}
    }
    else
    {
        Write-Host "Creating $srcpath from scratch...";
        new-item -path "$srcpath" -ItemType Directory
        new-item -path "$srcpath\pipelines" -ItemType Directory
        new-item -path "$srcpath\datasets" -ItemType Directory
        new-item -path "$srcpath\linkedservices" -ItemType Directory
    }
}


function check-pipelinelastrun($adf, $rg, $pipeline, [int]$months = 8)
{

    $todayDateStamp = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH\:mm\:ss.fffffffZ")  #az datafactory command expects ISO8601 timestamp format
    $oldDateStamp = "2015-01-01T00:00:00.00Z"  #predates ARM, should be safe as a baseline date range.

    $runs = az datafactory pipeline-run query-by-factory --resource-group $rg --factory-name $adf --last-updated-after $oldDateStamp --last-updated-before $todayDateStamp --filters operand="PipelineName" operator="Equals" values="$pipeline" | convertfrom-json
    
    Write-Host "Pipeline $pipeline has this many runs: $($runs.Value.Count)"
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

    Write-Host "Backup factories running on sub: $sub with RG: $resourceGroup with source folder: $srcfolder"
    Write-Host "Executing command: az resource list --resource-group $resourceGroup --resource-type `"Microsoft.Datafactory/factories`""
    $factories = (az resource list --resource-group $resourceGroup --resource-type "Microsoft.Datafactory/factories") | convertfrom-json
    foreach($factory in $factories)
    {
        Write-Host "Starting backup of Factory $($factory.name) in resource group $resourceGroup"
        
        #make sure all subfolders exist first...
        ensure-adfdirectory -srcpath "$srcfolder\$($factory.name)"

        #backup pipelines first
        Write-Host "Command Running: az datafactory pipeline list --resource-group $resourceGroup --factory-name $($factory.name)"
        foreach($pipeline in (az datafactory pipeline list --resource-group $resourceGroup --factory-name $factory.name | convertfrom-json))
        {
            #Don't back up if not run in last 12 months...
            if(($Filter -and (check-pipelinelastrun -adf $factory.name -rg $resourceGroup -pipeline $pipeline.name -months $lookbackMonths)) -or !($Filter)) 
            {
                backup-adfpipeline -sub $subscription -rg $resourceGroup -adf $factory.name -pipeline $pipeline.name -outputfile "$srcfolder\$($factory.name)\pipelines\$($pipeline.name).json"
            }
        }

        #backup datasets
        Write-Host "Command Running: az datafactory dataset list --resource-group $resourceGroup --factory-name $($factory.name)"
        foreach($dataset in (az datafactory dataset list --resource-group $resourceGroup --factory-name $factory.name | convertfrom-json))
        {
       
            backup-adfdataset -sub $subscription -rg $resourceGroup -adf $factory.name -dataset $dataset.name -outputfile "$srcfolder\$($factory.name)\datasets\$($dataset.name).json"
        }

        #backup linked services
        Write-Host "Command running: az datafactory linked-service list --resource-group $resourceGroup --factory-name $($factory.name)"
        foreach($service in (az datafactory linked-service list --resource-group $resourceGroup --factory-name $factory.name | convertfrom-json))
        {
        
            backup-adflinkedservice -sub $subscription -rg $resourceGroup -adf $factory.name -linkedservice $service.name -outputfile "$srcfolder\$($factory.name)\linkedservices\$($service.name).json"
        }

        #backup the factory itself
        backup-adffactory -sub $subscription -rg $resourceGroup -adf $factory.name -outputfile "$srcfolder\$($factory.name)\$($factory.name).json"
    }

}


function restore-factories($sub, $rg, $srcfolder, $suffix = "")
{
    Write-Host "Restore factories running on sub: $sub with RG: $resourceGroup with source folder: $srcfolder"
    $srcdir = get-item -Path $srcfolder

    foreach($factory in $srcdir.GetDirectories())
    {
        Write-Host "Starting restore of Factory $($factory.Name) in resource group $resourceGroup"

        try
        {
            #suffix is included here to ensure the ADF name is unique globally.  Backup and restore won't work if you haven't deleted the source factory otherwise.
            deploy-adffactory -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -inputfile "$($factory.FullName)\$($factory.Name).json" -suffix $suffix
        }
        catch
        {
            Write-Host $_.Exception -ForegroundColor Red
            continue
        }

        #deploy linked services
        foreach($service in $factory.GetDirectories("linkedservices").GetFiles())
        {
            deploy-adflinkedservice -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -linkedservice $service.BaseName -inputfile $service.FullName
        }

        #deploy datasets
        Write-Host "Command Running: az datafactory dataset list --resource-group $resourceGroup --factory-name $($factory.name)"
        foreach($dataset in $factory.GetDirectories("datasets").GetFiles())
        {
            deploy-adfdataset -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -dataset $dataset.BaseName -inputfile $dataset.FullName
        }

        #deploy pipelines last
        foreach($pipeline in $factory.GetDirectories("pipelines").GetFiles())
        {
            deploy-adfpipeline -sub $subscription -rg $resourceGroup -adf "$($factory.name)$suffix" -pipeline $pipeline.BaseName -inputfile $pipeline.FullName
        }
    }
}
