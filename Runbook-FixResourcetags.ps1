Edit PowerShell Runbook
FixTags
Directory: Microsoft
2122232425262728293031323334353637383940415678910111213141516171819203421
import-module Az.Resources

$connectionName = "AzureRunAsConnection"
try
{
    # Get the connection "AzureRunAsConnection "
    $servicePrincipalConnection=Get-AutomationConnection -Name $connectionName         

    "Logging in to Azure..."
    Add-AzAccount `

