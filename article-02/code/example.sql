CREATE SCHEMA [orders]
;
GO

CREATE TABLE [orders].[product] (
  [id] [uniqueidentifier] NOT NULL
, [description] [nvarchar](max) NOT NULL
, CONSTRAINT [pk_product] PRIMARY KEY ([id])
)
;
GO

CREATE SCHEMA [orders_hda]
;
GO

CREATE TABLE [orders_hda].[product] (
  [hda_validFrom_utc] [datetime2] NOT NULL
, [hda_runId] [uniqueidentifier] NOT NULL
, [hda_voided] [bit] NOT NULL
, [id] [uniqueidentifier] NOT NULL
, [description] [nvarchar](max) NULL
, CONSTRAINT [pk_product] PRIMARY KEY ([id], [hda_validFrom_utc])
)
;
GO

INSERT INTO [orders_hda].[product] (
  [hda_validFrom_utc]
, [hda_runId]
, [hda_voided]
, [id]
, [description]
) VALUES (
  '2023-12-01', '11111111-0000-0000-0000-000000000000', 0, '00000000-0000-0000-0000-000000000000', 'Red hat'
),  (
  '2023-12-05', '22222222-0000-0000-0000-000000000000', 0, '00000000-0000-0000-0000-000000000000', 'Green hat'
),  (
  '2023-12-10', '33333333-0000-0000-0000-000000000000', 0, '00000000-0000-0000-0000-000000000000', 'Red hat'
),  (
  '2023-12-01', '11111111-0000-0000-0000-000000000000', 0, '11111111-1111-1111-1111-111111111111', 'Red hat'
),  (
  '2023-12-05', '22222222-0000-0000-0000-000000000000', 1, '11111111-1111-1111-1111-111111111111', NULL
),  (
  '2023-12-10', '33333333-0000-0000-0000-000000000000', 0, '11111111-1111-1111-1111-111111111111', 'Blue hat'
)
;
GO

CREATE FUNCTION [orders_hda].[product_pit] (
  @timestamp_utc [datetime2]
)
RETURNS TABLE
AS 
  RETURN (
    SELECT
      [hda_validFrom_utc]
    , [hda_runId]
    , [hda_voided]
    , [id]
    , [description]
	FROM 
	  [orders_hda].[product] AS [p]
    WHERE
	  [p].[hda_validFrom_utc] <= @timestamp_utc
	AND
	  [p].[hda_voided] = 0
	AND
	  [hda_validFrom_utc] = (
	    SELECT 
		  MAX([hda_validFrom_utc])
		FROM 
	      [orders_hda].[product] AS [h]
		WHERE
		  [h].[id] = [p].[id]
		AND
	      [h].[hda_validFrom_utc] <= @timestamp_utc  
	  )
  )
GO
;

SELECT * FROM [orders_hda].[product_actual]('2023-11-30')
;
GO

SELECT * FROM [orders_hda].[product_actual]('2023-12-02')
;
GO

SELECT * FROM [orders_hda].[product_actual]('2023-12-09')
;
GO

SELECT * FROM [orders_hda].[product_actual]('2023-12-11')
;
GO

DROP FUNCTION IF EXISTS [orders_hda].[product_actual]
;
GO

DROP TABLE IF EXISTS [orders_hda].[product]
;
GO

DROP SCHEMA IF EXISTS [orders_hda]
;
GO

DROP TABLE IF EXISTS [orders].[product]
;
GO

DROP SCHEMA IF EXISTS [orders]
;
GO