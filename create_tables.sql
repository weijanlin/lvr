-- 建立新的資料表

-- LVR_UsedHouse 資料庫
CREATE TABLE [LVR_UsedHouse].[dbo].[main_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    鄉鎮市區 NVARCHAR(200),
    交易標的 NVARCHAR(200),
    土地位置建物門牌 NVARCHAR(500),
    土地移轉總面積平方公尺 DECIMAL(15,2),
    都市土地使用分區 NVARCHAR(500),
    非都市土地使用分區 NVARCHAR(200),
    非都市土地使用編定 NVARCHAR(200),
    交易年月日 NVARCHAR(20),
    交易筆棟數 INT,
    移轉層次 NVARCHAR(50),
    總樓層數 INT,
    建物型態 NVARCHAR(200),
    主要用途 NVARCHAR(200),
    主要建材 NVARCHAR(200),
    建築完成年月 NVARCHAR(20),
    建物移轉總面積平方公尺 DECIMAL(15,2),
    建物現況格局-房 INT,
    建物現況格局-廳 INT,
    建物現況格局-衛 INT,
    建物現況格局-隔間 NVARCHAR(50),
    有無管理組織 NVARCHAR(20),
    總價元 DECIMAL(15,2),
    單價元平方公尺 DECIMAL(15,2),
    車位類別 NVARCHAR(50),
    車位移轉總面積平方公尺 DECIMAL(15,2),
    車位總價元 DECIMAL(15,2),
    備註 NVARCHAR(1000),
    編號 NVARCHAR(100),
    主建物面積 DECIMAL(15,2),
    附屬建物面積 DECIMAL(15,2),
    陽台面積 DECIMAL(15,2),
    電梯 NVARCHAR(20),
    移轉編號 NVARCHAR(100),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_UsedHouse].[dbo].[build_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    屋齡 INT,
    建物移轉面積平方公尺 DECIMAL(15,2),
    主要用途 NVARCHAR(200),
    主要建材 NVARCHAR(200),
    建築完成日期 NVARCHAR(20),
    總層數 INT,
    建物分層 NVARCHAR(100),
    移轉情形 NVARCHAR(200),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_UsedHouse].[dbo].[land_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    土地位置 NVARCHAR(200),
    土地移轉面積平方公尺 DECIMAL(15,2),
    使用分區或編定 NVARCHAR(500),
    權利人持分分母 DECIMAL(15,2),
    權利人持分分子 DECIMAL(15,2),
    移轉情形 NVARCHAR(200),
    地號 NVARCHAR(100),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_UsedHouse].[dbo].[park_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    車位類別 NVARCHAR(50),
    車位價格 DECIMAL(15,2),
    車位面積平方公尺 DECIMAL(15,2),
    車位所在樓層 NVARCHAR(50),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

-- LVR_PreSale 資料庫
CREATE TABLE [LVR_PreSale].[dbo].[presale_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    鄉鎮市區 NVARCHAR(200),
    交易標的 NVARCHAR(200),
    土地位置建物門牌 NVARCHAR(500),
    土地移轉總面積平方公尺 DECIMAL(15,2),
    都市土地使用分區 NVARCHAR(500),
    非都市土地使用分區 NVARCHAR(200),
    非都市土地使用編定 NVARCHAR(200),
    交易年月日 NVARCHAR(20),
    交易筆棟數 INT,
    移轉層次 NVARCHAR(50),
    總樓層數 INT,
    建物型態 NVARCHAR(200),
    主要用途 NVARCHAR(200),
    主要建材 NVARCHAR(200),
    建築完成年月 NVARCHAR(20),
    建物移轉總面積平方公尺 DECIMAL(15,2),
    建物現況格局-房 INT,
    建物現況格局-廳 INT,
    建物現況格局-衛 INT,
    建物現況格局-隔間 NVARCHAR(50),
    有無管理組織 NVARCHAR(20),
    總價元 DECIMAL(15,2),
    單價元平方公尺 DECIMAL(15,2),
    車位類別 NVARCHAR(50),
    車位移轉總面積平方公尺 DECIMAL(15,2),
    車位總價元 DECIMAL(15,2),
    備註 NVARCHAR(1000),
    編號 NVARCHAR(100),
    建案名稱 NVARCHAR(200),
    棟及號 NVARCHAR(100),
    解約情形 NVARCHAR(50),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_PreSale].[dbo].[build_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    屋齡 INT,
    建物移轉面積平方公尺 DECIMAL(15,2),
    主要用途 NVARCHAR(200),
    主要建材 NVARCHAR(200),
    建築完成日期 NVARCHAR(20),
    總層數 INT,
    建物分層 NVARCHAR(100),
    移轉情形 NVARCHAR(200),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_PreSale].[dbo].[land_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    土地位置 NVARCHAR(200),
    土地移轉面積平方公尺 DECIMAL(15,2),
    使用分區或編定 NVARCHAR(500),
    權利人持分分母 DECIMAL(15,2),
    權利人持分分子 DECIMAL(15,2),
    移轉情形 NVARCHAR(200),
    地號 NVARCHAR(100),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_PreSale].[dbo].[park_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    車位類別 NVARCHAR(50),
    車位價格 DECIMAL(15,2),
    車位面積平方公尺 DECIMAL(15,2),
    車位所在樓層 NVARCHAR(50),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

-- LVR_Rental 資料庫
CREATE TABLE [LVR_Rental].[dbo].[rental_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    鄉鎮市區 NVARCHAR(200),
    交易標的 NVARCHAR(200),
    土地位置建物門牌 NVARCHAR(500),
    土地面積平方公尺 DECIMAL(15,2),
    都市土地使用分區 NVARCHAR(500),
    非都市土地使用分區 NVARCHAR(200),
    非都市土地使用編定 NVARCHAR(200),
    租賃年月日 NVARCHAR(20),
    租賃筆棟數 INT,
    租賃層次 NVARCHAR(50),
    總樓層數 INT,
    建物型態 NVARCHAR(200),
    主要用途 NVARCHAR(200),
    主要建材 NVARCHAR(200),
    建築完成年月 NVARCHAR(20),
    建物總面積平方公尺 DECIMAL(15,2),
    建物現況格局-房 INT,
    建物現況格局-廳 INT,
    建物現況格局-衛 INT,
    建物現況格局-隔間 NVARCHAR(50),
    有無管理組織 NVARCHAR(20),
    有無附傢俱 NVARCHAR(20),
    總額元 DECIMAL(15,2),
    單價元平方公尺 DECIMAL(15,2),
    車位類別 NVARCHAR(50),
    車位面積平方公尺 DECIMAL(15,2),
    車位總額元 DECIMAL(15,2),
    備註 NVARCHAR(1000),
    編號 NVARCHAR(100),
    出租型態 NVARCHAR(50),
    有無管理員 NVARCHAR(20),
    租賃期間 NVARCHAR(50),
    有無電梯 NVARCHAR(20),
    附屬設備 NVARCHAR(500),
    租賃住宅服務 NVARCHAR(200),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_Rental].[dbo].[build_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    屋齡 INT,
    建物移轉面積平方公尺 DECIMAL(15,2),
    主要用途 NVARCHAR(200),
    主要建材 NVARCHAR(200),
    建築完成日期 NVARCHAR(20),
    總層數 INT,
    建物分層 NVARCHAR(100),
    移轉情形 NVARCHAR(200),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_Rental].[dbo].[land_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    土地位置 NVARCHAR(200),
    土地移轉面積平方公尺 DECIMAL(15,2),
    使用分區或編定 NVARCHAR(500),
    權利人持分分母 DECIMAL(15,2),
    權利人持分分子 DECIMAL(15,2),
    移轉情形 NVARCHAR(200),
    地號 NVARCHAR(100),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

CREATE TABLE [LVR_Rental].[dbo].[park_data] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    編號 NVARCHAR(100),
    車位類別 NVARCHAR(50),
    車位價格 DECIMAL(15,2),
    車位面積平方公尺 DECIMAL(15,2),
    車位所在樓層 NVARCHAR(50),
    source_file NVARCHAR(200),
    quarter NVARCHAR(20)
);

