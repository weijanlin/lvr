-- 刪除現有資料表
-- 注意：這會刪除所有現有資料！

-- LVR_UsedHouse 資料庫
DROP TABLE IF EXISTS [LVR_UsedHouse].[dbo].[main_data];
DROP TABLE IF EXISTS [LVR_UsedHouse].[dbo].[build_data];
DROP TABLE IF EXISTS [LVR_UsedHouse].[dbo].[land_data];
DROP TABLE IF EXISTS [LVR_UsedHouse].[dbo].[park_data];

-- LVR_PreSale 資料庫
DROP TABLE IF EXISTS [LVR_PreSale].[dbo].[presale_data];
DROP TABLE IF EXISTS [LVR_PreSale].[dbo].[build_data];
DROP TABLE IF EXISTS [LVR_PreSale].[dbo].[land_data];
DROP TABLE IF EXISTS [LVR_PreSale].[dbo].[park_data];

-- LVR_Rental 資料庫
DROP TABLE IF EXISTS [LVR_Rental].[dbo].[rental_data];
DROP TABLE IF EXISTS [LVR_Rental].[dbo].[build_data];
DROP TABLE IF EXISTS [LVR_Rental].[dbo].[land_data];
DROP TABLE IF EXISTS [LVR_Rental].[dbo].[park_data];

