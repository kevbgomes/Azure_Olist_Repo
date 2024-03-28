# End-to-End Azure Data Engineering Project
E-Commerce Public Dataset by Olist - Ingested, Transformed, Stored on Azure

![pipeline](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/fc23813d-f10f-4bf5-b447-5a8d20764fc9)

## Dataset Used 
 Source[Ecommerce public dataset of orders made at Olist Store]: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download

 ## Services Used
1. **Azure SQL Server:** For connect to SQL Server Management Studio.
2. **Azure SQL database:** For manipulate data in SQL Server Management Studio.
3. **SQL Server Management Studio:** For load the csv into the database and create tables.
4. **Azure Data Lake Storage Gen2**: As the primary data storage solution.
5. **Azure Data Factory:** To copy the database tables to the container.
6. **Azure Databricks:** For data transformation tasks.
7. **Azure Key Vault:** For security of secret key value.
8. **Power BI:** For Data visualization

## Workflow 

## Initial Setup in Azure
1. Create Azure account (Free Subscription)  
2. Create a Resource Group 'olist_resource_group' to house and manage all the Azure resources associated with this project. 
3. Within the created resource group, setup a storage account 'oliststorageaccount2' . This is specifically configured to leverage Azure Data Lake Storage(ADLS) Gen2 capabilities.
4. Create a Container inside this storage account to hold the project's data. Three directories 'landing', 'processing' and 'curated' are created to store raw data and transformed data.

![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/80290039-ec4e-4976-b224-9d04be64cb5b)

## Data Ingestion using Azure Data Factory
1. First, created an Azure Data Factory workspace within the previously established resource group (olist_resource_group).
2. After setting up the workspace, launch the Azure Data Factory Studio. 
3. Within the studio, initialize a new data integration pipeline. 
   - Configuring Copy data to move the csv file from the database to the landing container .
   - Establishing the Linked Service for source and sink.
   - Source Linked Service
     
     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/a44d80be-5edd-43c5-a632-9600360fd12c)

   - Sink Linked Service

     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/8a3c2918-13d6-4850-ab15-87fa42274fab)

 4. On Sql server defining firewall rules.

    ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/04f09982-26e9-4a05-8565-5fa6c35cc31d)

    
5. Create an ingest wizard to move all the csv's (tables) from the database to the landing container at once, using a foreach .
   
   - Ingest set up

     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/b3335303-b76c-4d11-a59f-2a3990d55a41)

   - Run once now
       
     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/0af32cc5-3e6d-44d9-b2cb-3568c158ea82)

   - Choose source type, the connection and select all tables.
  
     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/90404f24-683d-425b-bcbf-a6d02571d131)

   - Choose the type of destination, the connection and the folder path (container).

     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/f1dfe9f9-0f10-47ed-bff6-9674ab44668d)

   - File format settings.

     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/f32f3ec2-97d5-451e-a07d-01937782c171)
     

  6. Press the trigger, it passes all the tables by parameters at once.

     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/e7a9c6c6-75fa-4352-83f4-3b596ace53dd)

  7. Populated landing container.

     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/1c386e3d-1c77-407a-9285-9171c0f6066b)

## Data Transformation using Azure Databricks
1. Navigate to Azure Databricks within the Azure portal and create a workspace within the previously established resource group and launch it.
2. Configuring Compute in Databricks
3. Create a new notebook within Databricks.

4. Establishing a Connection to Azure Data Lake Storage (ADLS) using App Registration.
   - Create a new registration;

     ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/22e54d9c-4055-45e8-9a1f-e49ba4a5b6b8)

     
   - Copy the credentials (Client ID, Tenant ID), to later write the appropriate code in the Databricks notebook to mount ADLS.
    ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/44c88f04-6b73-4a11-b3c5-007b8037baf8)

     
5. Using Key Vault in the Azure account for Secure key protecting:
   - In Key Vault, create a key vault with the same resource used before;
   - Now in the 'Secrets', generate a secret with the App Registration Key secret, name it and create.
   - This Key Vault will secure your secret key so it exposed in the mount code.

6. In Databricks, created a secret scope:
  - To create it, substitute url go to https://<databricks_instance.net>#secrets/createScope;
  - Create a Scope using the DNS and Resource ID from the Key Vault properties.
    ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/1799c69c-80a2-4dc3-9b57-033954d30a8b)
    ![image](https://github.com/kevbgomes/Azure_Olist_Repo/assets/111183588/086ba5e1-9b6b-4f75-af43-5eb1320d2bde)

   

7. Writing Data Transformations mount ADLS Gen2 to Databricks.
   - Within the code using python with OS library, removed files that were not used. (code is provided in the reference below)
   - Create a dataframe (pyspark), transformed columns with the correct datatype and created table within a database to query.



8. Writing Transformed Data to ADLS Gen2
   - Folder is create in the container with the partitioned files


Refer below notebook to transformations and code used to mount ADLS Gen2 to Databricks.


 






    
   


   


