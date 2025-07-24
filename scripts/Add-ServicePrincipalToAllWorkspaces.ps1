'''
This script will add a service principal to all Power BI workspaces with the Admin Role
Replace $GroupObjectId with the Object ID of the service principal you want to add.
Ensure that the MicrosoftPowerBIMgmt module is installed and updated.
'''

$GroupObjectId = 'ed904bb7-750e-4443-96f5-65309fb082e7'   # default security group
$ExcludeWorkspaces = @()  # ensure an empty array

Import-Module MicrosoftPowerBIMgmt.Profile   -ErrorAction Stop
Import-Module MicrosoftPowerBIMgmt.Workspaces -ErrorAction Stop

# 1. Interactive sign-in
Connect-PowerBIServiceAccount -ErrorAction Stop

try {
    # 2. Fetch all non-personal workspaces
    $workspaces = Get-PowerBIWorkspace -Scope Organization -All |
                 Where-Object { $_.Type -ne 'PersonalGroup' }

    Write-Host "Total shared workspaces found: $($workspaces.Count)"

    foreach ($ws in $workspaces) {

        if ($ExcludeWorkspaces -contains $ws.Id -or $ExcludeWorkspaces -contains $ws.Name) {
            Write-Host "Skipping workspace '$($ws.Name)' ($($ws.Id))"
            continue
        }

        if ($GroupObjectId) {
            Write-Host "Adding security group to workspace '$($ws.Name)' ..."
            Add-PowerBIWorkspaceUser `
                -Id $ws.Id `
                -PrincipalType Group `
                -Identifier $GroupObjectId `
                -AccessRight Admin `
        }
        else {
            Write-Warning "GroupObjectId is empty; no group added to workspace '$($ws.Name)'."
        }
    }

    Write-Host "Completed."
}
catch {
    Write-Error "Failed: $_"
}