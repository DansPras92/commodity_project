--Data commodity Answers--
USE TrainingWorks
GO

--How big the Import Market--
with total_usd as
(
	SELECT
	flow,
	[YEAR],
	sum(trade_usd) as total_value
FROM dbo.commodity_data
WHERE flow = 'Import'
GROUP BY flow,[YEAR]
)
,lag_total_usd as
(
SELECT
	total.flow,
	total.[year],
	total.total_value,
	LAG(total.total_value) OVER (ORDER BY [YEAR]) as last_total_value
FROM total_usd as total
)

SELECT 
	t1.[year],
	t1.flow,
	t1.total_value as total_value,
	format((convert(float,t1.total_value)-convert(float,t1.last_total_value))/convert(float,t1.last_total_value),'P','en-us')as growth
FROM lag_total_usd as t1

--================================================================================================
--cinnamon based commodity in 2016--

--cinnamon based commodity total import value--
SELECT 
	--comm_code
	--,commodity
	sum(trade_usd) as cinnamon_total_trade_usd
FROM dbo.commodity_data
WHERE flow = 'Import' AND commodity like '%cinna%' AND [year] = '2016'
--GROUP BY comm_code, commodity

--Top 10 country with highest import value for cinnamon--
SELECT TOP 10
	country_or_area
	,[year]
	--,comm_code
	--,commodity
	,flow
	,sum(trade_usd) as trade_usd_total
	,sum(weight_kg) as wight_kg_total
	,quantity_name
	,sum(quantity) as total_quantity
	--,category
FROM dbo.commodity_data
WHERE flow = 'Import' AND commodity like '%cinna%' AND [year] = '2016'
GROUP BY country_or_area, 
	--comm_code, commodity, 
	[year], flow, quantity_name, category
ORDER BY trade_usd_total desc

--=============================================================================================

--corn based commodity total import value--

--Total corn value in 2016
SELECT 
	--comm_code
	--,commodity
	--,category
	sum(trade_usd) as corn_total_trade_usd
FROM dbo.commodity_data
WHERE flow = 'Import' 
	AND [year] = '2016'
	AND commodity not like '%except%maize%' --to exclude all rows with "except maize" in it
	AND commodity not like '%except%corn%' --to exclude all rows with "except corn" in it
	AND commodity like '%maize%'--to take all rows with "maize" in it
	OR 
	flow = 'Import' 
	AND [year] = '2016'
	AND commodity not like '%except%maize%' --to exclude all rows with "except maize" in it
	AND commodity not like '%except%corn%' --to exclude all rows with "except corn" in it
	AND commodity like '%corn[^a-z]%'--to take all rows with "maize" in it
--GROUP BY comm_code, commodity, category
ORDER BY corn_total_trade_usd desc

--Top 10 country with highest import value for corn--
SELECT TOP 10
	country_or_area
	,[year]
	--,comm_code
	--,commodity
	,flow
	,sum(trade_usd) as trade_usd_total
	,sum(weight_kg) as weight_kg_total
	,quantity_name
	--,sum(quantity) as total_quantity
	--,category
FROM dbo.commodity_data
WHERE flow = 'Import' 
	AND [year] = '2016'
	AND commodity not like '%except%maize%' --to exclude all rows with "except maize" in it
	AND commodity not like '%except%corn%' --to exclude all rows with "except corn" in it
	AND commodity like '%maize%'--to take all rows with "maize" in it
	OR 
	flow = 'Import' 
	AND [year] = '2016'
	AND commodity not like '%except%maize%' --to exclude all rows with "except maize" in it
	AND commodity not like '%except%corn%' --to exclude all rows with "except corn" in it
	AND commodity like '%corn[^a-z]%'--to take all rows with "maize" in it
GROUP BY country_or_area, 
	--comm_code, commodity,category, 
	[year], flow, quantity_name
ORDER BY trade_usd_total desc
--================================================================================

SELECT TOP 10
	country_or_area,
	[year],
	--commodity,
	flow,
	sum(trade_usd) as total_trade_usd
FROM dbo.commodity_data
WHERE
	[year] = '2016'
	AND flow = 'Export'
	AND country_or_area = 'indonesia'

--remove comment below to get corn	
	--AND commodity not like '%except%maize%' --to exclude all rows with "except maize" in it
	--AND commodity not like '%except%corn%' --to exclude all rows with "except corn" in it
	--AND commodity like '%maize%'--to take all rows with "maize" in it
	--OR 
	--flow = 'Export' 
	--AND [year] = '2016'
	--AND country_or_area = 'indonesia'
	--AND commodity not like '%except%maize%' --to exclude all rows with "except maize" in it
	--AND commodity not like '%except%corn%' --to exclude all rows with "except corn" in it
	--AND commodity like '%corn[^a-z]%'--to take all rows with "maize" in it
--remove comment below to get cinnamon
	AND commodity like '%cinna%'
GROUP BY
	country_or_area,
	[year],
	--commodity,
	flow
ORDER BY 
	total_trade_usd desc