function execute-kql($clusterUrl, $databaseName, $query)
{

#following : https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/powershell/powershell

try
{
    Add-Type -LiteralPath "$(get-KEPath)\Kusto.Data.dll"
}
catch
{
  $_.ErrorDetails | get-member
  $_.Exception.LoaderExceptions
}

#   Option A: using Azure AD User Authentication
$kcsb = New-Object Kusto.Data.KustoConnectionStringBuilder ($clusterUrl, $databaseName)
$kcsb."AAD Federated Security" = $true
$queryProvider = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslQueryProvider($kcsb)
$queryProvider | Get-Member

Write-Host "Executing query: '$query' with connection string: '$($kcsb.ToString())'"
#   Optional: set a client request ID and set a client request property (e.g. Server Timeout)
$crp = New-Object Kusto.Data.Common.ClientRequestProperties
$crp.ClientRequestId = "MyPowershellScript.ExecuteQuery." + [Guid]::NewGuid().ToString()
$crp.SetOption([Kusto.Data.Common.ClientRequestProperties]::OptionServerTimeout, [TimeSpan]::FromSeconds(30))

$crp.SecurityToken

#   Execute the query
$reader = $queryProvider.ExecuteQuery($query, $crp)

# Do something with the result datatable, for example: print it formatted as a table, sorted by the
# "StartTime" column, in descending order
$dataTable = [Kusto.Cloud.Platform.Data.ExtendedDataReader]::ToDataSet($reader).Tables[0]

return $dataTable

}

function execute-kqlcommand($clusterUrl, $databaseName = "NetDefaultDB", $command)
{

try
{
    Add-Type -LiteralPath "$(get-KEPath)\Kusto.Data.dll"
}
catch
{
  $_.ErrorDetails | get-member
  $_.Exception.LoaderExceptions
}

#   Option A: using Azure AD User Authentication
$kcsb = New-Object Kusto.Data.KustoConnectionStringBuilder ($clusterUrl, $databaseName)
$kcsb."AAD Federated Security" = $true
$adminProvider = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslAdminProvider($kcsb)


Write-Host "Executing command: '$query' with connection string: '$($kcsb.ToString())'"
#   Optional: set a client request ID and set a client request property (e.g. Server Timeout)
$crp = New-Object Kusto.Data.Common.ClientRequestProperties
$crp.ClientRequestId = "MyPowershellScript.ExecuteQuery." + [Guid]::NewGuid().ToString()
$crp.SetOption([Kusto.Data.Common.ClientRequestProperties]::OptionServerTimeout, [TimeSpan]::FromSeconds(30))

$crp.SecurityToken

#   Execute the query
$reader = $adminProvider.ExecuteControlCommand($command, $crp)

# Do something with the result datatable, for example: print it formatted as a table, sorted by the
# "StartTime" column, in descending order
$dataTable = [Kusto.Cloud.Platform.Data.ExtendedDataReader]::ToDataSet($reader).Tables[0]

return $dataTable

}

function get-KEPath
{
    $temppath = "$ENV:TEMP\KustoLibs"
    if(!(test-path -path $temppath)) { 
    
        new-item -ItemType Directory -Path $temppath

        if(test-path -Path "$temppath\Kusto.Explorer.Application")
        {
            remove-item -LiteralPath "$temppath\Kusto.Explorer.Application"
        }

        Invoke-WebRequest -Uri "https://kusto.azureedge.net/kustoexplorer/Kusto.Explorer.application" -OutFile "$temppath\Kusto.Explorer.Application"

        $xmlfile = [xml](get-content "$temppath\Kusto.Explorer.Application" -encoding utf8)

        $kustoUri = $xmlfile.ChildNodes.dependency.dependentAssembly.codebase

        $kustoUri = $kustoUri.Replace("\","/").Replace("./", "https://kusto.azureedge.net/kustoexplorer/").Replace(" ","%20")
        $manifestFile = $kustoUri.Split("/")[$kustoUri.Split("/").Count-1]

        Invoke-WebRequest -Uri $kustoUri -OutFile "$temppath\$manifestFile"

        $manifestxml = [xml](get-content "$temppath\$manifestFile" -encoding utf8)

        $manifestbase = $kustoUri.Replace($kustoUri.split("/")[$kustoUri.split("/").Length - 1],"")

        foreach($file in $manifestxml.ChildNodes.dependency)
        {
            #let's not mess with localized files - no UI for us here...  *.dll's only too.
            if($file.dependentAssembly.codebase -notlike "*\*" -and $file.dependentAssembly.codebase -like "*.dll")
            {
                Invoke-WebRequest -Uri "$manifestbase$($file.dependentAssembly.codebase).deploy" -OutFile "$temppath\$($file.dependentAssembly.codebase)"
            }
        }
    }

    return $temppath
}

export-modulemember -Function execute-kqlcommand
export-modulemember -Function execute-kql
