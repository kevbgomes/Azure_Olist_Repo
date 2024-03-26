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


   


