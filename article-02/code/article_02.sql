/*
 * The code in this file is automatically generated @ 2023-05-22 13:13:25.499179.
 * Manual changes to this file will be removed when this file is regenerated.
 */
CREATE SCHEMA [orders];
GO


CREATE TABLE [orders].[Customer] (
  [id] uniqueidentifier NOT NULL
, [address] nvarchar(255) NULL
, CONSTRAINT [pk_Customer]
    PRIMARY KEY (
      [id]
    )
)
;
GO

CREATE TABLE [orders].[Product] (
  [id] uniqueidentifier NOT NULL
, [description] nvarchar(255) NULL
, CONSTRAINT [pk_Product]
    PRIMARY KEY (
      [id]
    )
)
;
GO

CREATE TABLE [orders].[Order] (
  [id] uniqueidentifier NOT NULL
, [orderTimestamp_utc] datetime2 NULL
, [amount] int NULL
, [orderedBy_Customer_id] uniqueidentifier NOT NULL
, [orderFor_Product_id] uniqueidentifier NOT NULL
, CONSTRAINT [pk_Order]
    PRIMARY KEY (
      [id]
    )
)
;
GO

ALTER TABLE [orders].[Order]
ADD
  CONSTRAINT [fk_orderedBy_Customer]
    FOREIGN KEY 
      ([orderedBy_Customer_id])
    REFERENCES [orders].[Customer] 
      ([id])
, CONSTRAINT [fk_orderFor_Product]
    FOREIGN KEY 
      ([orderFor_Product_id])
    REFERENCES [orders].[Product] 
      ([id])
;
GO

CREATE SCHEMA [orders_stg];
GO


CREATE TABLE [orders_stg].[Customer] (
  [stg_timestamp_utc] datetime2 NOT NULL
, [stg_runId] uniqueidentifier NOT NULL
, [id] uniqueidentifier NOT NULL
, [address] nvarchar(255) NULL
, CONSTRAINT [pk_Customer]
    PRIMARY KEY (
      [stg_runId]
    , [id]
    )
)
;
GO

CREATE TABLE [orders_stg].[Product] (
  [stg_timestamp_utc] datetime2 NOT NULL
, [stg_runId] uniqueidentifier NOT NULL
, [id] uniqueidentifier NOT NULL
, [description] nvarchar(255) NULL
, CONSTRAINT [pk_Product]
    PRIMARY KEY (
      [stg_runId]
    , [id]
    )
)
;
GO

CREATE TABLE [orders_stg].[Order] (
  [stg_timestamp_utc] datetime2 NOT NULL
, [stg_runId] uniqueidentifier NOT NULL
, [id] uniqueidentifier NOT NULL
, [orderTimestamp_utc] datetime2 NULL
, [amount] int NULL
, [orderedBy_Customer_id] uniqueidentifier NULL
, [orderFor_Product_id] uniqueidentifier NULL
, CONSTRAINT [pk_Order]
    PRIMARY KEY (
      [stg_runId]
    , [id]
    )
)
;
GO

-- Load table orders_stg.Customer
CREATE OR ALTER PROCEDURE [orders_stg].[usp_loadCustomer] (
  @stg_runId uniqueIdentifier
, @stg_timestamp_utc datetime
)
AS
BEGIN
  INSERT INTO [orders_stg].[Customer] (
    [stg_timestamp_utc]
  , [stg_runId]
  , [id]
  , [address]
  )
  SELECT
    @stg_timestamp_utc
  , @stg_runId
  , [id]
  , [address]
  FROM 
    [orders].[Customer]
  ;
END
;
GO

-- Load table orders_stg.Product
CREATE OR ALTER PROCEDURE [orders_stg].[usp_loadProduct] (
  @stg_runId uniqueIdentifier
, @stg_timestamp_utc datetime
)
AS
BEGIN
  INSERT INTO [orders_stg].[Product] (
    [stg_timestamp_utc]
  , [stg_runId]
  , [id]
  , [description]
  )
  SELECT
    @stg_timestamp_utc
  , @stg_runId
  , [id]
  , [description]
  FROM 
    [orders].[Product]
  ;
END
;
GO

-- Load table orders_stg.Order
CREATE OR ALTER PROCEDURE [orders_stg].[usp_loadOrder] (
  @stg_runId uniqueIdentifier
, @stg_timestamp_utc datetime
)
AS
BEGIN
  INSERT INTO [orders_stg].[Order] (
    [stg_timestamp_utc]
  , [stg_runId]
  , [id]
  , [orderTimestamp_utc]
  , [amount]
  , [orderedBy_Customer_id]
  , [orderFor_Product_id]
  )
  SELECT
    @stg_timestamp_utc
  , @stg_runId
  , [id]
  , [orderTimestamp_utc]
  , [amount]
  , [orderedBy_Customer_id]
  , [orderFor_Product_id]
  FROM 
    [orders].[Order]
  ;
END
;
GO

CREATE SCHEMA [orders_hda];
GO


CREATE TABLE [orders_hda].[Customer] (
  [hda_runId] uniqueidentifier NOT NULL
, [hda_validFrom_utc] datetime2 NOT NULL
, [stg_runId] uniqueidentifier NOT NULL
, [id] uniqueidentifier NOT NULL
, [address] nvarchar(255) NULL
, CONSTRAINT [pk_Customer]
    PRIMARY KEY (
      [hda_timestamp_utc]
    , [id]
    )
)
;
GO

CREATE TABLE [orders_hda].[Product] (
  [hda_runId] uniqueidentifier NOT NULL
, [hda_validFrom_utc] datetime2 NOT NULL
, [stg_runId] uniqueidentifier NOT NULL
, [id] uniqueidentifier NOT NULL
, [description] nvarchar(255) NULL
, CONSTRAINT [pk_Product]
    PRIMARY KEY (
      [hda_timestamp_utc]
    , [id]
    )
)
;
GO

CREATE TABLE [orders_hda].[Order] (
  [hda_runId] uniqueidentifier NOT NULL
, [hda_validFrom_utc] datetime2 NOT NULL
, [stg_runId] uniqueidentifier NOT NULL
, [id] uniqueidentifier NOT NULL
, [orderTimestamp_utc] datetime2 NULL
, [amount] int NULL
, [orderedBy_Customer_id] uniqueidentifier NULL
, [orderFor_Product_id] uniqueidentifier NULL
, CONSTRAINT [pk_Order]
    PRIMARY KEY (
      [hda_timestamp_utc]
    , [id]
    )
)
;
GO

-- Load table orders_hda.Customer
CREATE OR ALTER PROCEDURE [orders_hda].[usp_load_Customer] (
  @hda_runId uniqueidentifier
, @stg_runId uniqueidentifier
)
AS
BEGIN
  -- Load HDA table with data from specific staging load.
  INSERT INTO [orders_hda].[Customer] (
    [hda_runId]
  , [hda_validFrom_utc]
  , [stg_runId]
  , [id]
  , [address]
  )
  SELECT
    @hda_runId
  , [stg_timestamp_utc]
  , [stg_runId]
  , [id]
  , [address]
  FROM 
    [orders_stg].[Customer]
  WHERE
    [stg_runId] = @stg_runId
  ;
  -- Cleanup staging table after HDA load succeeded.
  DELETE FROM
    [orders_stg].[Customer]
  WHERE
    [stg_runId] = @stg_runId
  ;
END
;
GO

-- Load table orders_hda.Product
CREATE OR ALTER PROCEDURE [orders_hda].[usp_load_Product] (
  @hda_runId uniqueidentifier
, @stg_runId uniqueidentifier
)
AS
BEGIN
  -- Load HDA table with data from specific staging load.
  INSERT INTO [orders_hda].[Product] (
    [hda_runId]
  , [hda_validFrom_utc]
  , [stg_runId]
  , [id]
  , [description]
  )
  SELECT
    @hda_runId
  , [stg_timestamp_utc]
  , [stg_runId]
  , [id]
  , [description]
  FROM 
    [orders_stg].[Product]
  WHERE
    [stg_runId] = @stg_runId
  ;
  -- Cleanup staging table after HDA load succeeded.
  DELETE FROM
    [orders_stg].[Product]
  WHERE
    [stg_runId] = @stg_runId
  ;
END
;
GO

-- Load table orders_hda.Order
CREATE OR ALTER PROCEDURE [orders_hda].[usp_load_Order] (
  @hda_runId uniqueidentifier
, @stg_runId uniqueidentifier
)
AS
BEGIN
  -- Load HDA table with data from specific staging load.
  INSERT INTO [orders_hda].[Order] (
    [hda_runId]
  , [hda_validFrom_utc]
  , [stg_runId]
  , [id]
  , [orderTimestamp_utc]
  , [amount]
  , [orderedBy_Customer_id]
  , [orderFor_Product_id]
  )
  SELECT
    @hda_runId
  , [stg_timestamp_utc]
  , [stg_runId]
  , [id]
  , [orderTimestamp_utc]
  , [amount]
  , [orderedBy_Customer_id]
  , [orderFor_Product_id]
  FROM 
    [orders_stg].[Order]
  WHERE
    [stg_runId] = @stg_runId
  ;
  -- Cleanup staging table after HDA load succeeded.
  DELETE FROM
    [orders_stg].[Order]
  WHERE
    [stg_runId] = @stg_runId
  ;
END
;
GO

