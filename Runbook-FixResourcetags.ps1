<#
    .DESCRIPTION
        An example runbook which illustrates how you can use an Azure Policy plus some automation to enforce correct resource tags.

    .NOTES
        AUTHOR: Cary Roys
        LASTEDIT: September 22, 2022
#>

import-module Az.ResourceGraph
import-module Az.Resources

$connectionName = "AzureRunAsConnection"
try
{
    # Get the connection "AzureRunAsConnection "
    $servicePrincipalConnection=Get-AutomationConnection -Name $connectionName         

    "Logging in to Azure..."
    Add-AzAccount `
        -ServicePrincipal `
        -TenantId $servicePrincipalConnection.TenantId `
        -ApplicationId $servicePrincipalConnection.ApplicationId `
        -CertificateThumbprint $servicePrincipalConnection.CertificateThumbprint 
}
catch {
    if (!$servicePrincipalConnection)
    {
        $ErrorMessage = "Connection $connectionName not found."
        throw $ErrorMessage
    } else{
        Write-Error -Message $_.Exception
        throw $_.Exception
    }
}

$subscriptionID = "e36582a6-9e0c-4644-9b78-592ffe29a705"

Set-AzContext -Subscription $subscriptionID

$valueMap = @{}

#populate our tag value mappings to fix.  Include good ones here too, since every iteration will rewrite stuff.
$valueMap["asdf"] = "CaryApp"
$valueMap["AnotherTest"] = "CaryApp2"
$valueMap["Test"] = "CaryApp"
$valueMap["test"] = "CaryApp"
$valueMap["testing4"] = "CaryApp"
$valueMap["CaryApp"] = "CaryApp"
$valueMap["CaryApp2"] = "CaryApp"
$valueMap["blhblah"] = "CaryApp2"


$GoodName = "ApplicationName"

# All the bad permutations for a tag name we want to fix up
$possibleTags = @("application name","Application name","Application Name","Application name", "AppName") 

$getpolicyId = @'
policyresources
| where kind == "policyassignments"
| where properties.displayName == "Require an ApplicationName tag on resources"
| extend policyDefinitionId = properties.policyDefinitionId
'@

$policyId = (search-azgraph -Query $getpolicyId).policyDefinitionId

$getpolicyViolations =  @'
policyresources 
| where kind != "policyassignments"
| extend resourceId = properties.resourceId
'@

$getpolicyViolations += "`n| where properties.policyDefinitionId =~ `"$policyId`""

$foundValue = ""


foreach($resource in (search-azgraph -Query $getpolicyViolations))
{
    $bFoundName = $false
    $tags = (Get-AzTag -ResourceId $resource.properties.resourceId)
    $tagscol = @{}
    if($tags.Properties.TagsProperty -ne $null)
    {
        foreach($tag in $tags.Properties.TagsProperty.GetEnumerator())
        {
            if($tag.Key -like $GoodName -or $possibleTags -contains $tag.Key)
            {
                $tagscol[$GoodName] = $valueMap[$tag.value]
                #check the app name mapping
                $bFoundName = $true
            }
            else
            {
                $tagscol[$tag.Key] = $tag.Value
            }

            if($bFoundName)
            {
                New-AzTag -tag $tagscol -ResourceId $resource.properties.resourceId
            }
        }
    }
    if($bFoundName -eq $false)
    {
        Write-Host "No tag mappable to ApplicationName was found!  See resourceID: $($resource.properties.resourceId)"
        #Do stuff.  Probably log it somewhere, or trigger an action
    }
}
