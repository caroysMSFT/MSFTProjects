function execute-kql($clusterUrl, $databaseName, $query)
{

#following : https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/powershell/powershell
#We should adjust this to piggyback off Kusto Explorer which has everything we need.

#Maybe open this shortcut?  C:\Users\caroys\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Kusto.Explorer
$packagesRoot = "C:\Users\caroys\AppData\Local\Apps\2.0\2AAM8PME.NQR\9PTOQX6V.M54\kust..tion_bdc860f2c35357b9_0001.0000_4a3e0ee7342a8691"

dir $packagesRoot\* | Unblock-File


try
{
    Add-Type -LiteralPath "$packagesRoot\Kusto.Data.dll"
}
catch
{
  $_.ErrorDetails | get-member
  $_.Exception.LoaderExceptions
}

#   Option A: using Azure AD User Authentication
$kcsb = New-Object Kusto.Data.KustoConnectionStringBuilder ($clusterUrl, $databaseName)
$kcsb."AAD Federated Security" = $true
$kcsb
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
#$dataView = New-Object System.Data.DataView($dataTable)
#$dataView | Sort StartTime -Descending | Format-Table -AutoSize

}

export-modulemember -Function execute-kql
