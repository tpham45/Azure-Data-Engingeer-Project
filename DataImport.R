library(AzureStor)

--------------------------------------------------------------------------------------------------------
#Create Token:
# Acquiring an Azure token for secure and temporary authentication to Azure services.
# Tokens ensure safe access without exposing main credentials and can be scoped for specific permissions.
  
token <- AzureRMR::get_azure_token ("https://storage.azure.com"
                  ,tenant="31ec2513-6818-4351-bfdf-52eda65b2f71"
                  ,app="1e1bc807-4976-4770-b9bb-10462fe37f13"
                  ,password="NSZ8Q~4L8eOXJp8J9QKZASW.Z2ou~cwC9rzBUaTR"
                   )
---------------------------------------------------------------------------------------------------------
#Create Endpoint Token:
# Set up an endpoint to connect to the 'tokyoplympicsdata' Azure Blob storage.
# Use the previously obtained authentication token for secure access.
ad_endp_tok <- storage_endpoint("https://tokyoplympicsdata.blob.core.windows.net/",token=token)
---------------------------------------------------------------------------------------------------------
# Connect to the 'tokyodatasources' container within the Azure Blob storage endpoint.
# List all files within the 'transformed-data' directory of the container.
cont <- storage_container(ad_endp_tok, "tokyodatasources")
list_storage_files(cont,"transformed-data")
---------------------------------------------------------------------------------------------------------
#Athletes Data Import:
storage_download(cont, "transformed-data/athletes/athletes_fd.csv", "athletes_fd.csv", overwrite = TRUE)
athletes = read.csv("athletes_fd.csv")
print (athletes)

#Coaches Data Import:
storage_download(cont, "transformed-data/coaches/coach_fd.csv", "coach_fd.csv", overwrite = TRUE)
coaches = read.csv("coach_fd.csv")
print (coaches)

#Entriesgender Data Import:
storage_download(cont, "transformed-data/entriesgender/entriesgen_fd.csv", "entriesgen_fd.csv", overwrite = TRUE)
entriesgender = read.csv("entriesgen_fd.csv")
print (entriesgender)

#Medals Data Import:
storage_download(cont, "transformed-data/medals/medal_fd.csv", "medal_fd.csv", overwrite = TRUE)
medals = read.csv("medal_fd.csv")
print (medals)

#Teams Data Import:
storage_download(cont, "transformed-data/teams/team_fd.csv", "team_fd.csv", overwrite = TRUE)
teams = read.csv("team_fd.csv")
print (teams)

#AthletesMed Data Import:
storage_download(cont, "transformed-data/athletesmedal/athletesmd_fd.csv", "athletesmd_fd.csv", overwrite = TRUE)
athmedals = read.csv("athletesmd_fd.csv")
print (athmedals)

