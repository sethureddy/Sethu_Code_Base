CREATE OR REPLACE PROCEDURE `Saga.ProcFinalObjectsRefresh`()
BEGIN
drop Materialized view if exists Saga.MV_WPH_TOP_WELL_COUNT;
create or replace  Materialized view Saga.MV_WPH_TOP_WELL_COUNT
cluster by StateName,County ,BasinName,DisplayFormation
as
(SELECT StateName,County ,BasinName,DisplayFormation,Slant,OperatorName,HasProduction,WellStatus,WellType,
InterpretedProducingFormation,HasForecast,FieldName, count(UWI) as WellCount  
FROM Saga.WellPerformanceHeader Where Country = 'US' 
group by StateName,County,BasinName,DisplayFormation,Slant,OperatorName,HasProduction,WellStatus,WellType,
InterpretedProducingFormation,HasForecast,FieldName);

--drop table if EXISTS Saga.DeclineCurveParameters;
Create table if not exists Saga.DeclineCurveParameters

(
WellID	INT64
,UWI	STRING
,OilStart	DATE
,OilEND	DATE
,OilInitialRate	FLOAT64
,OilExponent	FLOAT64
,OilDeclineRate	FLOAT64
,OilAlgorithm	STRING
,CumOIl	DECIMAL(20,3)
,GasStart	DATE
,GasEND	DATE
,GasInitialRate	FLOAT64
,GasExponent	FLOAT64
,GasDeclineRate	FLOAT64
,GasAlgorithm	STRING
,CumGas	DECIMAL(20,3)
,WaterStart	DATE
,WaterEnd	DATE
,WaterInitialRate	FLOAT64
,WaterExponent	FLOAT64
,WaterDeclineRate	FLOAT64
,WaterAlgorithm	STRING
,CumWater	DECIMAL(20,3)
)
cluster  by WellID;

TRUNCATE TABLE Saga.DeclineCurveParameters;
Insert into Saga.DeclineCurveParameters
SELECT  
h.WellID,h.UWI
--,dateadd(month,gid.OilShift,h.firstmonth) OilStart
,DATE_ADD(Oilhist.OilStart, INTERVAL gid.OilShift MONTH) AS OilStart,
Oil.OilEND
,gid.OilInitialRate
,CASE WHEN IFNULL(h.CumOil,0)=0 THEN 0 ELSE (gid.OilExponent*h.CumOil)/(h.CumOil) END OilExponent
,CASE WHEN IFNULL(h.CumOil,0)=0 THEN 0 ELSE (gid.OilDeclineRate*h.CumOil)/(h.CumOil) END OilDeclineRate
,CASE WHEN gid.OilExponent>0 THEN 'HYPERBOLIC' WHEN gid.OilDeclineRate>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END OilAlgorithm
,h.CumOil
,DATE_ADD(Gashist.GasStart, INTERVAL gid.GasShift MONTH) AS GasStart,Gas.GasEND
,gid.GasInitialRate
,CASE WHEN IFNULL(h.CumGas,0)=0 THEN 0 ELSE (gid.GasExponent*h.CumGas)/(h.CumGas) END GasExponent
,CASE WHEN IFNULL(h.CumGas,0)=0 THEN 0 ELSE (gid.GasDeclineRate*h.CumGas)/(h.CumGas) END GasDeclineRate
,CASE WHEN gid.GasExponent>0 THEN 'HYPERBOLIC' WHEN gid.GasDeclineRate>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END GasAlgorithm
,h.CumGas
,DATE_ADD(Waterhist.WaterStart, INTERVAL gid.WaterShift MONTH) AS WaterStart,Water.WaterEND
,gid.WaterInitialRate
,CASE WHEN IFNULL(h.CumWater,0)=0 THEN 0 ELSE (gid.waterExponent*h.CumWater)/(h.CumWater) END WaterExponent
,CASE WHEN IFNULL(h.CumWater,0)=0 THEN 0 ELSE (gid.waterDeclineRate*h.CumWater)/(h.CumWater) END WaterDeclineRate
,CASE WHEN gid.Waterexponent>0 THEN 'HYPERBOLIC' WHEN gid.WaterDeclineRate>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END WaterAlgorithm
,h.CumWater
FROM tgs_gold.production_forecast_ForecastGroupIDsWell gid
JOIN Saga.WellPerformanceHeader h ON gid.WellID = h.WellID
LEFT JOIN (SELECT WellID, MAX(productionmonth) OilEND 
FROM Saga.ProductionForecast WHERE Oil > 0 GROUP BY WellID) Oil ON h.WellID=Oil.WellID 
LEFT JOIN (SELECT WellID, MAX(productionmonth) GasEND 
FROM Saga.ProductionForecast WHERE Gas > 0 GROUP BY WellID) Gas ON h.WellID=Gas.WellID
LEFT JOIN (SELECT WellID, MAX(productionmonth) WaterEND 
FROM Saga.ProductionForecast WHERE Water > 0 GROUP BY WellID) Water ON h.WellID=Water.WellID
LEFT JOIN (SELECT WellID, MIN(productionmonth) OilStart
FROM Saga.Production WHERE Oil > 0 GROUP BY WellID) Oilhist ON h.WellID=Oilhist.WellID 
LEFT JOIN (SELECT WellID, MIN(productionmonth) GasStart
FROM Saga.Production WHERE Gas > 0 GROUP BY WellID) Gashist ON h.WellID=Gashist.WellID
LEFT JOIN (SELECT WellID, MIN(productionmonth) WaterStart
FROM Saga.Production WHERE Water > 0 GROUP BY WellID) Waterhist ON h.WellID=Waterhist.WellID


UNION ALL

SELECT  
h.WellID,h.UWI
--,dateadd(month,gid.OilShift,h.firstmonth) OilStart
,MIN(DATE_ADD(Oilhist.OilStart, INTERVAL gid.OilShift MONTH)) AS OilStart,MAX(Oil.OilEND)OilEND
,SUM(gid.OilInitialRate)OilInitialRate
,CASE WHEN SUM(IFNULL(h.CumOil,0))=0 THEN 0 ELSE SUM(gid.OilExponent*h.CumOil)/SUM(h.CumOil) END OilExponent
,CASE WHEN SUM(IFNULL(h.CumOil,0))=0 THEN 0 ELSE SUM(gid.OilDeclineRate*h.CumOil)/SUM(h.CumOil) END OilDeclineRate
,CASE WHEN SUM(gid.OilExponent)>0 THEN 'HYPERBOLIC' WHEN SUM(gid.OilDeclineRate)>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END OilAlgorithm
,MAX(h.CumOil)CumOil
,MIN(DATE_ADD(Gashist.GasStart, INTERVAL gid.GasShift MONTH)) AS GasStart,MAX(Gas.GasEND)GasEND
,SUM(gid.GasInitialRate)GasInitialRate
,CASE WHEN SUM(IFNULL(h.CumGas,0))=0 THEN 0 ELSE SUM(gid.GasExponent*h.CumGas)/SUM(h.CumGas) END GasExponent
,CASE WHEN SUM(IFNULL(h.CumGas,0))=0 THEN 0 ELSE SUM(gid.GasDeclineRate*h.CumGas)/SUM(h.CumGas) END GasDeclineRate
,CASE WHEN SUM(gid.GasExponent)>0 THEN 'HYPERBOLIC' WHEN SUM(gid.GasDeclineRate)>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END GasAlgorithm
,MAX(h.CumGas)CumGas
,MIN(DATE_ADD(Waterhist.WaterStart, INTERVAL gid.WaterShift MONTH)) AS WaterStart,MAX(Water.WaterEND)WaterEND
,SUM(gid.WaterInitialRate)WaterInitialRate
,CASE WHEN SUM(IFNULL(h.CumWater,0))=0 THEN 0 ELSE SUM(gid.waterExponent*h.CumWater)/SUM(h.CumWater) END WaterExponent
,CASE WHEN SUM(IFNULL(h.CumWater,0))=0 THEN 0 ELSE SUM(gid.waterDeclineRate*h.CumWater)/SUM(h.CumWater) END WaterDeclineRate
,CASE WHEN SUM(gid.Waterexponent)>0 THEN 'HYPERBOLIC' WHEN SUM(gid.WaterDeclineRate)>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END WaterAlgorithm
,MAX(h.CumWater)CumWater
FROM tgs_gold.production_forecast_ForecastGroupIDsFormation gid
JOIN Saga.WellPerformanceHeader h on gid.WellID = h.WellID
LEFT JOIN (SELECT WellID, MAX(productionmonth) OilEND 
FROM Saga.ProductionForecast WHERE Oil > 0 GROUP BY WellID) Oil ON h.WellID=Oil.WellID 
LEFT JOIN (SELECT WellID, MAX(productionmonth) GasEND 
FROM Saga.ProductionForecast WHERE Gas > 0 GROUP BY WellID) Gas ON h.WellID=Gas.WellID
LEFT JOIN (SELECT WellID, MAX(productionmonth) WaterEND 
FROM Saga.ProductionForecast WHERE Water > 0 GROUP BY WellID) Water ON h.WellID=Water.WellID
LEFT JOIN (SELECT WellID, MIN(productionmonth) OilStart
FROM Saga.Production WHERE Oil > 0 GROUP BY WellID) Oilhist ON h.WellID=Oilhist.WellID 
LEFT JOIN (SELECT WellID, MIN(productionmonth) GasStart
FROM Saga.Production WHERE Gas > 0 GROUP BY WellID) Gashist ON h.WellID=Gashist.WellID
LEFT JOIN (SELECT WellID, MIN(productionmonth) WaterStart
FROM Saga.Production WHERE Water > 0 GROUP BY WellID) Waterhist ON h.WellID=Waterhist.WellID
GROUP BY h.WellID,h.UWI

UNION ALL

SELECT  
h.WellID,h.UWI
--,dateadd(month,gid.OilShift,h.firstmonth) OilStart
,MIN(DATE_ADD(Oilhist.OilStart, INTERVAL gid.OilShift MONTH)) AS OilStart,MAX(Oil.OilEND)OilEND
,SUM(gid.OilInitialRate)OilInitialRate
,CASE WHEN SUM(IFNULL(h.CumOil,0))=0 THEN 0 ELSE SUM(gid.OilExponent*h.CumOil)/SUM(h.CumOil) END OilExponent
,CASE WHEN SUM(IFNULL(h.CumOil,0))=0 THEN 0 ELSE SUM(gid.OilDeclineRate*h.CumOil)/SUM(h.CumOil) END OilDeclineRate
,CASE WHEN SUM(gid.OilExponent)>0 THEN 'HYPERBOLIC' WHEN SUM(gid.OilDeclineRate)>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END OilAlgorithm
,MAX(h.CumOil)CumOil
,MIN(DATE_ADD(Gashist.GasStart, INTERVAL gid.GasShift MONTH)) AS GasStart,MAX(Gas.GasEND)GasEND
,SUM(gid.GasInitialRate)GasInitialRate
,CASE WHEN SUM(IFNULL(h.CumGas,0))=0 THEN 0 ELSE SUM(gid.GasExponent*h.CumGas)/SUM(h.CumGas) END GasExponent
,CASE WHEN SUM(IFNULL(h.CumGas,0))=0 THEN 0 ELSE SUM(gid.GasDeclineRate*h.CumGas)/SUM(h.CumGas) END GasDeclineRate
,CASE WHEN SUM(gid.GasExponent)>0 THEN 'HYPERBOLIC' WHEN SUM(gid.GasDeclineRate)>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END GasAlgorithm
,MAX(h.CumGas)CumGas
,MIN(DATE_ADD(Waterhist.WaterStart, INTERVAL gid.WaterShift MONTH)) AS WaterStart,MAX(Water.WaterEND)WaterEND
,SUM(gid.WaterInitialRate)WaterInitialRate
,CASE WHEN SUM(IFNULL(h.CumWater,0))=0 THEN 0 ELSE SUM(gid.waterExponent*h.CumWater)/SUM(h.CumWater) END WaterExponent
,CASE WHEN SUM(IFNULL(h.CumWater,0))=0 THEN 0 ELSE SUM(gid.waterDeclineRate*h.CumWater)/SUM(h.CumWater) END WaterDeclineRate
,CASE WHEN SUM(gid.Waterexponent)>0 THEN 'HYPERBOLIC' WHEN SUM(gid.WaterDeclineRate)>0 THEN 'EXPONENTIAL' ELSE 'NO DATA' END WaterAlgorithm
,MAX(h.CumWater)CumWater
FROM tgs_gold.production_forecast_ForecastGroupIDsAllocatedLease gid
JOIN Saga.WellPerformanceHeader h on gid.WellID = h.WellID
LEFT JOIN (SELECT WellID, MAX(productionmonth) OilEND 
FROM Saga.ProductionForecast WHERE Oil > 0 GROUP BY WellID) Oil ON h.WellID=Oil.WellID 
LEFT JOIN (SELECT WellID, MAX(productionmonth) GasEND 
FROM Saga.ProductionForecast WHERE Gas > 0 GROUP BY WellID) Gas ON h.WellID=Gas.WellID
LEFT JOIN (SELECT WellID, MAX(productionmonth) WaterEND 
FROM Saga.ProductionForecast WHERE Water > 0 GROUP BY WellID) Water ON h.WellID=Water.WellID
LEFT JOIN (SELECT WellID, MIN(productionmonth) OilStart
FROM Saga.Production WHERE Oil > 0 GROUP BY WellID) Oilhist ON h.WellID=Oilhist.WellID 
LEFT JOIN (SELECT WellID, MIN(productionmonth) GasStart
FROM Saga.Production WHERE Gas > 0 GROUP BY WellID) Gashist ON h.WellID=Gashist.WellID
LEFT JOIN (SELECT WellID, MIN(productionmonth) WaterStart
FROM Saga.Production WHERE Water > 0 GROUP BY WellID) Waterhist ON h.WellID=Waterhist.WellID
GROUP BY h.WellID,h.UWI;


DROP TABLE IF EXISTS  `Saga.WellSpot_Surface_Attributes`;

CREATE TABLE IF NOT EXISTS `Saga.WellSpot_Surface_Attributes`
(
  ParentWellID INT64,
  SurfaceUWI STRING,
  SurfaceGeoPoint GEOGRAPHY,
  SurfaceLatitude_WGS84 BIGNUMERIC,
  SurfaceLongitude_WGS84 BIGNUMERIC,
  StateName STRING,
  County STRING,
  FieldName STRING,
  BasinName STRING,
  DisplayFormation STRING,
  InterpretedProducingFormation STRING,
  OperatorName STRING,
  UltimateOwner STRING,
  OriginalOperator STRING,
  LeaseName STRING,
  LeaseID STRING,
  WellStatus STRING,
  WellType STRING,
  plotinfo STRING,
  MeasuredDepth INT64,
  TotalVerticalDepth INT64,
  PerfIntervalTop INT64,
  PerfIntervalBottom INT64,
  Slant STRING,
  ElevationDrillFloor INT64,
  ElevationGround INT64,
  ElevationKellyBushing INT64,
  ElevationWaterDepth INT64,
  CalculatedMajorPhase STRING,
  LastInjectionType STRING,
  LastInjectionFormation STRING,
  DispositionType STRING,
  HasProduction BOOL,
  HasForecast BOOL,
  HasInjection BOOL,
  HasVentFlare BOOL,
  OffShoreFlg BOOL,
  SpudDate DATE,
  TDDate DATE,
  CompletionDate DATE,
  FirstMonth DATE,
  LastMonth DATE,
  LateralLength INT64,
  FractureFluidAmount NUMERIC(20, 3),
  ProppantAmount NUMERIC(20, 3),
  AcidAmount NUMERIC(20, 3),
  ProppantAmountPerFt NUMERIC(20, 3),
  FractureFluidAmountPerFt NUMERIC(20, 3),
  BOEMaxPer1000Ft NUMERIC(20, 3),
  EURPer1000Ft NUMERIC(20, 3),
  Section STRING,
  Township STRING,
  TownshipDirection STRING,
  RangeName STRING,
  RangeDirection STRING,
  District STRING,
  Area STRING,
  Offshore STRING,
  OffshoreBlock STRING,
  MaxBOE NUMERIC(20, 3),
  CumBOE NUMERIC(20, 3),
  CumGas NUMERIC(20, 3),
  MaxGas NUMERIC(20, 3),
  MaxGasPlus2 NUMERIC(20, 3),
  CumOil NUMERIC(20, 3),
  MaxOil NUMERIC(20, 3),
  MaxOilPlus2 NUMERIC(20, 3),
  CumWater NUMERIC(20, 3),
  MaxWater NUMERIC(20, 3),
  MaxWaterPlus2 NUMERIC(20, 3),
  CumGOR NUMERIC(20, 3),
  CumYield NUMERIC(20, 3),
  CumInjectionLiquid NUMERIC(20, 3),
  CumInjectionGas NUMERIC(20, 3),
  CumVentFlareVol NUMERIC(20, 3),
  WellForecastBOERemaining NUMERIC(20, 3),
  WellForecastBOEUltimate NUMERIC(20, 3),
  WellForecastGasRemaining NUMERIC(20, 3),
  WellForecastGasUltimate NUMERIC(20, 3),
  WellForecastOilRemaining NUMERIC(20, 3),
  WellForecastOilUltimate NUMERIC(20, 3),
  WellForecastWaterRemaining NUMERIC(20, 3),
  WellForecastWaterUltimate NUMERIC(20, 3),
  WellForecastGORUltimate NUMERIC(20, 3),
  WellForecastYieldUltimate NUMERIC(20, 3),
  WellForecastWaterCutUltimate NUMERIC(20, 3)
)


cluster by ParentWellID,OperatorName,StateName,County;



truncate table Saga.WellSpot_Surface_Attributes;

insert into Saga.WellSpot_Surface_Attributes


WITH Flag_Stats 
AS (
SELECT S.ParentWellID,S.SurfaceUWI,MAX(HasProduction)HasProduction,MAX(HasForecast)HasForecast,MAX(HasInjection)HasInjection,MAX(HasVentFlare)HasVentFlare,MAX(OffShoreFlg)OffShoreFlg
,SUM(LateralLength)LateralLength,SUM(FractureFluidAmount)FractureFluidAmount,SUM(ProppantAmount)ProppantAmount,SUM(AcidAmount)AcidAmount,AVG(ProppantAmountPerFt)ProppantAmountPerFt,AVG(FractureFluidAmountPerFt)FractureFluidAmountPerFt
,AVG(BOEMaxPer1000Ft)BOEMaxPer1000Ft,AVG(EURPer1000Ft)EURPer1000Ft,MAX(MaxBOE)MaxBOE,SUM(CumBOE)CumBOE,SUM(CumGas)CumGas,MAX(MaxGas)MaxGas,MAX(MaxGasPlus2)MaxGasPlus2
,SUM(CumOil)CumOil,MAX(MaxOil)MaxOil,MAX(MaxOilPlus2)MaxOilPlus2,SUM(CumWater)CumWater,MAX(MaxWater)MaxWater,MAX(MaxWaterPlus2)MaxWaterPlus2,AVG(CumGOR)CumGOR,AVG(CumYield)CumYield
,SUM(CumInjectionLiquid)CumInjectionLiquid,SUM(CumInjectionGas)CumInjectionGas,SUM(CumVentFlareVol)CumVentFlareVol,SUM(WellForecastBOERemaining)WellForecastBOERemaining,SUM(WellForecastBOEUltimate)WellForecastBOEUltimate
,SUM(WellForecastGasRemaining)WellForecastGasRemaining,SUM(WellForecastGasUltimate)WellForecastGasUltimate,SUM(WellForecastOilRemaining)WellForecastOilRemaining
,SUM(WellForecastOilUltimate)WellForecastOilUltimate,SUM(WellForecastWaterRemaining)WellForecastWaterRemaining,SUM(WellForecastWaterUltimate)WellForecastWaterUltimate
,AVG(WellForecastGORUltimate)WellForecastGORUltimate,AVG(WellForecastYieldUltimate)WellForecastYieldUltimate,AVG(WellForecastWaterCutUltimate)WellForecastWaterCutUltimate
FROM Saga.view_surface_locations S 
JOIN `Saga.WellPerformanceHeader`  H ON H.UWI10=S.SurfaceUWI AND H.ParentWellID=S.ParentWellID
--where S.ParentWellID=400790
GROUP BY  S.ParentWellID,S.SurfaceUWI
)
SELECT 
F.ParentWellID,F.SurfaceUWI,SurfaceGeoPoint,SurfaceLatitude_WGS84,SurfaceLongitude_WGS84,StateName,County,FieldName,BasinName,DisplayFormation,InterpretedProducingFormation,OperatorName,UltimateOwner,OriginalOperator
,LeaseName,LeaseID,WellStatus,WellType,plotinfo,MeasuredDepth,TotalVerticalDepth,PerfIntervalTop,PerfIntervalBottom,Slant,ElevationDrillFloor,ElevationGround,ElevationKellyBushing,ElevationWaterDepth
,CalculatedMajorPhase,LastInjectionType,LastInjectionFormation,DispositionType,HasProduction,HasForecast,HasInjection,HasVentFlare,OffShoreFlg,SpudDate,TDDate,CompletionDate,FirstMonth,LastMonth,LateralLength
,FractureFluidAmount,ProppantAmount,AcidAmount,ProppantAmountPerFt,FractureFluidAmountPerFt,BOEMaxPer1000Ft,EURPer1000Ft,Section,Township,TownshipDirection,RangeName,RangeDirection,District,Area,Offshore,OffshoreBlock
,MaxBOE,CumBOE,CumGas,MaxGas,MaxGasPlus2,CumOil,MaxOil,MaxOilPlus2,CumWater,MaxWater,MaxWaterPlus2,CumGOR,CumYield,CumInjectionLiquid,CumInjectionGas,CumVentFlareVol,WellForecastBOERemaining
,WellForecastBOEUltimate,WellForecastGasRemaining,WellForecastGasUltimate,WellForecastOilRemaining,WellForecastOilUltimate,WellForecastWaterRemaining,WellForecastWaterUltimate,WellForecastGORUltimate,WellForecastYieldUltimate,WellForecastWaterCutUltimate

FROM (
SELECT *,ROW_NUMBER() OVER(PARTITION BY ParentWellID,SurfaceUWI ORDER BY UWI)rno
FROM 
(
Select 
S.ParentWellID,S.SurfaceUWI,S.SurfaceGeoPoint,S.SurfaceLatitude_WGS84,S.SurfaceLongitude_WGS84,UWI
,FIRST_VALUE(StateName IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)StateName
,FIRST_VALUE(County IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)County
,FIRST_VALUE(FieldName IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)FieldName
,FIRST_VALUE(BasinName IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)BasinName
,FIRST_VALUE(DisplayFormation IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)DisplayFormation
,FIRST_VALUE(InterpretedProducingFormation IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)InterpretedProducingFormation
,FIRST_VALUE(OperatorName IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)OperatorName
,FIRST_VALUE(UltimateOwner IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)UltimateOwner
,FIRST_VALUE(OriginalOperator IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)OriginalOperator
,FIRST_VALUE(LeaseName IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LeaseName
,FIRST_VALUE(LeaseID IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LeaseID
,FIRST_VALUE(WellStatus IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)WellStatus
,FIRST_VALUE(WellType IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)WellType
,FIRST_VALUE(plotinfo IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)plotinfo
,FIRST_VALUE(MeasuredDepth IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)MeasuredDepth
,FIRST_VALUE(TotalVerticalDepth IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)TotalVerticalDepth
,FIRST_VALUE(PerfIntervalTop IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by PerfIntervalTop asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)PerfIntervalTop
,FIRST_VALUE(PerfIntervalBottom IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by PerfIntervalBottom desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)PerfIntervalBottom
,FIRST_VALUE(Slant IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Slant
,FIRST_VALUE(ElevationDrillFloor IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)ElevationDrillFloor
,FIRST_VALUE(ElevationGround IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)ElevationGround
,FIRST_VALUE(ElevationKellyBushing IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)ElevationKellyBushing
,FIRST_VALUE(ElevationWaterDepth IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)ElevationWaterDepth
,FIRST_VALUE(CalculatedMajorPhase IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)CalculatedMajorPhase
,FIRST_VALUE(LastInjectionType IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LastInjectionType
,FIRST_VALUE(LastInjectionFormation IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LastInjectionFormation
,FIRST_VALUE(DispositionType IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)DispositionType
,FIRST_VALUE(SpudDate IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by SpudDate desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)SpudDate
,FIRST_VALUE(TDDate IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by TDDate desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)TDDate
,FIRST_VALUE(CompletionDate IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by CompletionDate desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)CompletionDate
,FIRST_VALUE(FirstMonth IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by FirstMonth asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)FirstMonth
,FIRST_VALUE(LastMonth IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by LastMonth desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LastMonth
,FIRST_VALUE(Section IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Section
,FIRST_VALUE(Township IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Township
,FIRST_VALUE(TownshipDirection IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)TownshipDirection
,FIRST_VALUE(RangeName IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)RangeName
,FIRST_VALUE(RangeDirection IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)RangeDirection
,FIRST_VALUE(District IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)District
,FIRST_VALUE(Area IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Area
,FIRST_VALUE(Offshore IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Offshore
,FIRST_VALUE(OffshoreBlock IGNORE NULLS) OVER(Partition by S.ParentWellID,S.SurfaceUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)OffshoreBlock
FROM Saga.view_surface_locations S 
JOIN `Saga.WellPerformanceHeader`  H ON H.UWI10=S.SurfaceUWI AND H.ParentWellID=S.ParentWellID
--where S.ParentWellID=400790
)
)X
JOIN Flag_Stats F ON F.SurfaceUWI=X.SurfaceUWI AND F.ParentWellID=X.ParentWellID
WHERE rno=1;
--30secs
--4,127,058

DROP TABLE IF EXISTS `Saga.WellSpot_Bottom_Attributes`;
CREATE TABLE IF NOT EXISTS `Saga.WellSpot_Bottom_Attributes`
(
  ParentWellID INT64,
  BottomUWI STRING,
  BottomGeoPoint GEOGRAPHY,
  BottomLatitude_WGS84 BIGNUMERIC,
  BottomLongitude_WGS84 BIGNUMERIC,
  StateName STRING,
  County STRING,
  FieldName STRING,
  BasinName STRING,
  DisplayFormation STRING,
  InterpretedProducingFormation STRING,
  OperatorName STRING,
  UltimateOwner STRING,
  OriginalOperator STRING,
  LeaseName STRING,
  LeaseID STRING,
  WellStatus STRING,
  WellType STRING,
  plotinfo STRING,
  MeasuredDepth INT64,
  TotalVerticalDepth INT64,
  PerfIntervalTop INT64,
  PerfIntervalBottom INT64,
  Slant STRING,
  ElevationDrillFloor INT64,
  ElevationGround INT64,
  ElevationKellyBushing INT64,
  ElevationWaterDepth INT64,
  CalculatedMajorPhase STRING,
  LastInjectionType STRING,
  LastInjectionFormation STRING,
  DispositionType STRING,
  HasProduction BOOL,
  HasForecast BOOL,
  HasInjection BOOL,
  HasVentFlare BOOL,
  OffShoreFlg BOOL,
  SpudDate DATE,
  TDDate DATE,
  CompletionDate DATE,
  FirstMonth DATE,
  LastMonth DATE,
  LateralLength INT64,
  FractureFluidAmount NUMERIC(20, 3),
  ProppantAmount NUMERIC(20, 3),
  AcidAmount NUMERIC(20, 3),
  ProppantAmountPerFt NUMERIC(20, 3),
  FractureFluidAmountPerFt NUMERIC(20, 3),
  BOEMaxPer1000Ft NUMERIC(20, 3),
  EURPer1000Ft NUMERIC(20, 3),
  Section STRING,
  Township STRING,
  TownshipDirection STRING,
  RangeName STRING,
  RangeDirection STRING,
  District STRING,
  Area STRING,
  Offshore STRING,
  OffshoreBlock STRING,
  MaxBOE NUMERIC(20, 3),
  CumBOE NUMERIC(20, 3),
  CumGas NUMERIC(20, 3),
  MaxGas NUMERIC(20, 3),
  MaxGasPlus2 NUMERIC(20, 3),
  CumOil NUMERIC(20, 3),
  MaxOil NUMERIC(20, 3),
  MaxOilPlus2 NUMERIC(20, 3),
  CumWater NUMERIC(20, 3),
  MaxWater NUMERIC(20, 3),
  MaxWaterPlus2 NUMERIC(20, 3),
  CumGOR NUMERIC(20, 3),
  CumYield NUMERIC(20, 3),
  CumInjectionLiquid NUMERIC(20, 3),
  CumInjectionGas NUMERIC(20, 3),
  CumVentFlareVol NUMERIC(20, 3),
  WellForecastBOERemaining NUMERIC(20, 3),
  WellForecastBOEUltimate NUMERIC(20, 3),
  WellForecastGasRemaining NUMERIC(20, 3),
  WellForecastGasUltimate NUMERIC(20, 3),
  WellForecastOilRemaining NUMERIC(20, 3),
  WellForecastOilUltimate NUMERIC(20, 3),
  WellForecastWaterRemaining NUMERIC(20, 3),
  WellForecastWaterUltimate NUMERIC(20, 3),
  WellForecastGORUltimate NUMERIC(20, 3),
  WellForecastYieldUltimate NUMERIC(20, 3),
  WellForecastWaterCutUltimate NUMERIC(20, 3)
)

cluster by ParentWellID,BottomUWI, OperatorName,County;





truncate table Saga.WellSpot_Bottom_Attributes;

insert into Saga.WellSpot_Bottom_Attributes


WITH Flag_Stats 
AS (
SELECT B.ParentWellID,B.BottomUWI,MAX(HasProduction)HasProduction,MAX(HasForecast)HasForecast,MAX(HasInjection)HasInjection,MAX(HasVentFlare)HasVentFlare,MAX(OffShoreFlg)OffShoreFlg
,SUM(LateralLength)LateralLength,SUM(FractureFluidAmount)FractureFluidAmount,SUM(ProppantAmount)ProppantAmount,SUM(AcidAmount)AcidAmount,AVG(ProppantAmountPerFt)ProppantAmountPerFt,AVG(FractureFluidAmountPerFt)FractureFluidAmountPerFt
,AVG(BOEMaxPer1000Ft)BOEMaxPer1000Ft,AVG(EURPer1000Ft)EURPer1000Ft,MAX(MaxBOE)MaxBOE,SUM(CumBOE)CumBOE,SUM(CumGas)CumGas,MAX(MaxGas)MaxGas,MAX(MaxGasPlus2)MaxGasPlus2
,SUM(CumOil)CumOil,MAX(MaxOil)MaxOil,MAX(MaxOilPlus2)MaxOilPlus2,SUM(CumWater)CumWater,MAX(MaxWater)MaxWater,MAX(MaxWaterPlus2)MaxWaterPlus2,AVG(CumGOR)CumGOR,AVG(CumYield)CumYield
,SUM(CumInjectionLiquid)CumInjectionLiquid,SUM(CumInjectionGas)CumInjectionGas,SUM(CumVentFlareVol)CumVentFlareVol,SUM(WellForecastBOERemaining)WellForecastBOERemaining,SUM(WellForecastBOEUltimate)WellForecastBOEUltimate
,SUM(WellForecastGasRemaining)WellForecastGasRemaining,SUM(WellForecastGasUltimate)WellForecastGasUltimate,SUM(WellForecastOilRemaining)WellForecastOilRemaining
,SUM(WellForecastOilUltimate)WellForecastOilUltimate,SUM(WellForecastWaterRemaining)WellForecastWaterRemaining,SUM(WellForecastWaterUltimate)WellForecastWaterUltimate
,AVG(WellForecastGORUltimate)WellForecastGORUltimate,AVG(WellForecastYieldUltimate)WellForecastYieldUltimate,AVG(WellForecastWaterCutUltimate)WellForecastWaterCutUltimate
FROM Saga.view_bottom_locations B 
JOIN `Saga.WellPerformanceHeader`  H ON LEFT(H.UWI,12)=B.BottomUWI AND H.ParentWellID=B.ParentWellID
--where S.ParentWellID=400790
GROUP BY  B.ParentWellID,B.BottomUWI
)

SELECT 
F.ParentWellID,F.BottomUWI,BottomGeoPoint,BottomLatitude_WGS84,BottomLongitude_WGS84,StateName,County,FieldName,BasinName,DisplayFormation,InterpretedProducingFormation,OperatorName,UltimateOwner,OriginalOperator
,LeaseName,LeaseID,WellStatus,WellType,plotinfo,MeasuredDepth,TotalVerticalDepth,PerfIntervalTop,PerfIntervalBottom,Slant,ElevationDrillFloor,ElevationGround,ElevationKellyBushing,ElevationWaterDepth
,CalculatedMajorPhase,LastInjectionType,LastInjectionFormation,DispositionType,HasProduction,HasForecast,HasInjection,HasVentFlare,OffShoreFlg,SpudDate,TDDate,CompletionDate,FirstMonth,LastMonth,LateralLength
,FractureFluidAmount,ProppantAmount,AcidAmount,ProppantAmountPerFt,FractureFluidAmountPerFt,BOEMaxPer1000Ft,EURPer1000Ft,Section,Township,TownshipDirection,RangeName,RangeDirection,District,Area,Offshore,OffshoreBlock
,MaxBOE,CumBOE,CumGas,MaxGas,MaxGasPlus2,CumOil,MaxOil,MaxOilPlus2,CumWater,MaxWater,MaxWaterPlus2,CumGOR,CumYield,CumInjectionLiquid,CumInjectionGas,CumVentFlareVol,WellForecastBOERemaining
,WellForecastBOEUltimate,WellForecastGasRemaining,WellForecastGasUltimate,WellForecastOilRemaining,WellForecastOilUltimate,WellForecastWaterRemaining,WellForecastWaterUltimate,WellForecastGORUltimate,WellForecastYieldUltimate,WellForecastWaterCutUltimate

FROM (
SELECT *,ROW_NUMBER() OVER(PARTITION BY ParentWellID,BottomUWI ORDER BY UWI)rno
FROM 
(
Select 
B.ParentWellID,B.BottomUWI,B.BottomGeoPoint,B.BottomLatitude_WGS84,B.BottomLongitude_WGS84,UWI
,FIRST_VALUE(StateName IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)StateName
,FIRST_VALUE(County IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)County
,FIRST_VALUE(FieldName IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)FieldName
,FIRST_VALUE(BasinName IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)BasinName
,FIRST_VALUE(DisplayFormation IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)DisplayFormation
,FIRST_VALUE(InterpretedProducingFormation IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)InterpretedProducingFormation
,FIRST_VALUE(OperatorName IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)OperatorName
,FIRST_VALUE(UltimateOwner IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)UltimateOwner
,FIRST_VALUE(OriginalOperator IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)OriginalOperator
,FIRST_VALUE(LeaseName IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LeaseName
,FIRST_VALUE(LeaseID IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LeaseID
,FIRST_VALUE(WellStatus IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)WellStatus
,FIRST_VALUE(WellType IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)WellType
,FIRST_VALUE(plotinfo IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)plotinfo
,FIRST_VALUE(MeasuredDepth IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)MeasuredDepth
,FIRST_VALUE(TotalVerticalDepth IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)TotalVerticalDepth
,FIRST_VALUE(PerfIntervalTop IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by PerfIntervalTop asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)PerfIntervalTop
,FIRST_VALUE(PerfIntervalBottom IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by PerfIntervalBottom desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)PerfIntervalBottom
,FIRST_VALUE(Slant IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Slant
,FIRST_VALUE(ElevationDrillFloor IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)ElevationDrillFloor
,FIRST_VALUE(ElevationGround IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)ElevationGround
,FIRST_VALUE(ElevationKellyBushing IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)ElevationKellyBushing
,FIRST_VALUE(ElevationWaterDepth IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)ElevationWaterDepth
,FIRST_VALUE(CalculatedMajorPhase IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)CalculatedMajorPhase
,FIRST_VALUE(LastInjectionType IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LastInjectionType
,FIRST_VALUE(LastInjectionFormation IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LastInjectionFormation
,FIRST_VALUE(DispositionType IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)DispositionType
,FIRST_VALUE(SpudDate IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by SpudDate desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)SpudDate
,FIRST_VALUE(TDDate IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by TDDate desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)TDDate
,FIRST_VALUE(CompletionDate IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by CompletionDate desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)CompletionDate
,FIRST_VALUE(FirstMonth IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by FirstMonth asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)FirstMonth
,FIRST_VALUE(LastMonth IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by LastMonth desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LastMonth
,FIRST_VALUE(Section IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Section
,FIRST_VALUE(Township IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Township
,FIRST_VALUE(TownshipDirection IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)TownshipDirection
,FIRST_VALUE(RangeName IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)RangeName
,FIRST_VALUE(RangeDirection IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)RangeDirection
,FIRST_VALUE(District IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)District
,FIRST_VALUE(Area IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Area
,FIRST_VALUE(Offshore IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)Offshore
,FIRST_VALUE(OffshoreBlock IGNORE NULLS) OVER(Partition by B.ParentWellID,B.BottomUWI order by UWI asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)OffshoreBlock
FROM Saga.view_bottom_locations B 
JOIN `Saga.WellPerformanceHeader`  H ON LEFT(H.UWI,12)=B.BottomUWI AND H.ParentWellID=B.ParentWellID
--where S.ParentWellID=400790
)
)X
JOIN Flag_Stats F ON F.BottomUWI=X.BottomUWI and F.ParentWellID=X.ParentWellID
WHERE rno=1;
--25secs
--4180910s

CREATE TABLE  if not exists `Saga.WPH_NON_PRIORITY_WELL_COUNT`
(
  ColumnValue STRING,
  ColumnName STRING,
  WellCount INT64
)
CLUSTER BY ColumnName;


truncate table `Saga.WPH_NON_PRIORITY_WELL_COUNT` ;

insert into `Saga.WPH_NON_PRIORITY_WELL_COUNT`

SELECT LastInjectionType as ColumnValue,"LastInjectionType" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by LastInjectionType

UNION ALL

SELECT LastInjectionFormation as ColumnValue,"LastInjectionFormation" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by LastInjectionFormation

UNION ALL

SELECT CAST(Test as STRING) as ColumnValue,"Test" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by Test

UNION ALL

SELECT CAST(Perf as STRING) as ColumnValue ,"Perf" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by Perf

UNION ALL

SELECT CAST(HasInjection as STRING) as ColumnValue,"HasInjection" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by HasInjection

UNION ALL 

SELECT CAST(HasVentFlare as STRING) as ColumnValue,"HasVentFlare" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by HasVentFlare

UNION ALL

SELECT CAST(OffShoreFlg as STRING) as ColumnValue,"OffShoreFlg" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by OffShoreFlg

UNION ALL

SELECT Section as ColumnValue ,"Section" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by Section

UNION ALL

SELECT Township as ColumnValue,"Township" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by Township

UNION ALL 

SELECT TownshipDirection as ColumnValue,"TownshipDirection" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by TownshipDirection

UNION ALL

SELECT A.RangeName as ColumnValue,"RangeName" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader` A  Where Country = 'US' 
group by A.RangeName

UNION ALL


SELECT RangeDirection as ColumnValue ,"RangeDirection" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by RangeDirection

UNION ALL

SELECT District as ColumnValue,"District" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by District

UNION ALL 

SELECT Area as ColumnValue,"Area" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by Area

UNION ALL

SELECT Offshore as ColumnValue,"Offshore" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by Offshore

UNION ALL

SELECT OffshoreBlock as ColumnValue ,"OffshoreBlock" as ColumnName,count(UWI) as WellCount 
FROM `Saga.WellPerformanceHeader`  Where Country = 'US' 
group by OffshoreBlock;





CALL Saga.ProcSagaWellPath();
CALL Saga.ProcSagaWellStick();


END;



CREATE OR REPLACE PROCEDURE `Saga.ProcSagaBaseTablesRefresh`()
BEGIN

 ---------------------------------------------------------------------
 --Production Loading
 ---------------------------------------------------------------------
--drop table Saga.Production;
CREATE TABLE IF NOT EXISTS Saga.Production
(
  WellID INT64,
  UWI STRING,
  ProductionMonth DATE,
  Oil NUMERIC(20, 3),
  Gas NUMERIC(20, 3),
  Water NUMERIC(20, 3),
  DaysOnProduction INT64,
  SequenceMonth INT64,
  APIState STRING,
  BasinName STRING,
  CalculatedMajorPhase STRING,
  County STRING,
  DisplayFormation STRING,
  DispositionType STRING,
  FieldName STRING,
  InterpretedProducingFormation STRING,
  LastInjectionFormation STRING,
  LastInjectionType STRING,
  LeaseID STRING,
  LeaseName STRING,
  OperatorName STRING,
  OriginalOperator STRING,
  Slant STRING,
  StateName STRING,
  UltimateOwner STRING,
  UWI10 STRING,
  WellStatus STRING,
  WellType STRING
)
CLUSTER BY WellID, ProductionMonth, SequenceMonth;

Truncate table Saga.Production;

insert into Saga.Production
SELECT 
b.WellID
,b.UWI
,b.ProductionMonth
,b.Oil
,b.Gas
,b.Water
,cast(b.DaysOnProduction as int64) as DaysOnProduction
,b.SequenceMonth
,b.APIState
,BasinName
,CalculatedMajorPhase
,County
,DisplayFormation
,DispositionType
,FieldName
,InterpretedProducingFormation
,LastInjectionFormation
,LastInjectionType
,LeaseID
,LeaseName
,OperatorName
,OriginalOperator
,Slant
,StateName
,UltimateOwner
,UWI10
,WellStatus
,WellType
 FROM Saga.WellPerformanceHeader a

 JOIN Saga.viewSagaProduction b ON

 a.wellid=b.wellid;
 
 ---------------------------------------------------------------------
 --Injection Loading
 ---------------------------------------------------------------------
 
 
 --drop table Saga.Injection;
CREATE TABLE IF NOT EXISTS Saga.Injection
(
  WellID INT64,
  UWI STRING,
  InjectionMonth DATE,
  InjectionType STRING,
  InjectionFormation STRING,
  TotalLiquid NUMERIC(20, 3),
  TotalGas NUMERIC(20, 3),
  MaxInjPressure NUMERIC(20, 3),
  AvInjPressure NUMERIC(20, 3),
  DaysOnInjection INT64,
  SequenceMonth INT64,
  APIState STRING,
  BasinName STRING,
  CalculatedMajorPhase STRING,
  County STRING,
  DisplayFormation STRING,
  DispositionType STRING,
  FieldName STRING,
  InterpretedProducingFormation STRING,
  LastInjectionFormation STRING,
  LastInjectionType STRING,
  LeaseID STRING,
  LeaseName STRING,
  OperatorName STRING,
  OriginalOperator STRING,
  Slant STRING,
  StateName STRING,
  UltimateOwner STRING,
  UWI10 STRING,
  WellStatus STRING,
  WellType STRING
)
CLUSTER BY WellID, InjectionMonth, SequenceMonth, OperatorName;

TRUNCATE TABLE Saga.Injection;
insert into  Saga.Injection
SELECT 
b.WellID
,b.UWI
,b.InjectionMonth
,b.InjectionType
,b.InjectionFormation
,b.TotalLiquid
,b.TotalGas
,b.MaxInjPressure
,b.AvInjPressure
,cast(b.DaysOnInjection as int64) as DaysOnInjection
,b.SequenceMonth
,b.APIState
,BasinName
,CalculatedMajorPhase
,County
,DisplayFormation
,DispositionType
,FieldName
,InterpretedProducingFormation
,LastInjectionFormation
,LastInjectionType
,LeaseID
,LeaseName
,OperatorName
,OriginalOperator
,Slant
,StateName
,UltimateOwner
,UWI10
,WellStatus
,WellType
 FROM Saga.WellPerformanceHeader a

 JOIN Saga.viewSagaInjection b ON

 a.wellid=b.wellid;

 ---------------------------------------------------------------------
 --VentFlare Loading
 ---------------------------------------------------------------------
  --drop  table Saga.VentFlare;
CREATE TABLE IF NOT EXISTS Saga.VentFlare
(
  WellID INT64,
  UWI STRING,
  APIState STRING,
  ProductionMonth DATE,
  Vent NUMERIC(20, 3),
  Flare NUMERIC(20, 3),
  VentFlare NUMERIC(20, 3),
  TotalDisposed NUMERIC(20, 3),
  Used NUMERIC(20, 3),
  Inferred NUMERIC(20, 3),
  Other NUMERIC(20, 3),
  TotalUsed NUMERIC(20, 3),
  SequenceMonth INT64,
  BasinName STRING,
  CalculatedMajorPhase STRING,
  County STRING,
  DisplayFormation STRING,
  DispositionType STRING,
  FieldName STRING,
  InterpretedProducingFormation STRING,
  LastInjectionFormation STRING,
  LastInjectionType STRING,
  LeaseID STRING,
  LeaseName STRING,
  OperatorName STRING,
  OriginalOperator STRING,
  Slant STRING,
  StateName STRING,
  UltimateOwner STRING,
  UWI10 STRING,
  WellStatus STRING,
  WellType STRING
)
CLUSTER BY WellID, ProductionMonth, SequenceMonth, OperatorName;

TRUNCATE TABLE Saga.VentFlare;
insert into Saga.VentFlare
SELECT 
b.WellID
,b.UWI
,b.APIState
,b.ProductionMonth
,b.Vent
,b.Flare
,b.VentFlare
,b.TotalDisposed
,b.Used
,b.Inferred
,b.Other
,b.TotalUsed
,b.SequenceMonth
,BasinName
,CalculatedMajorPhase
,County
,DisplayFormation
,DispositionType
,FieldName
,InterpretedProducingFormation
,LastInjectionFormation
,LastInjectionType
,LeaseID
,LeaseName
,OperatorName
,OriginalOperator
,Slant
,StateName
,UltimateOwner
,UWI10
,WellStatus
,WellType
 FROM Saga.WellPerformanceHeader a

 JOIN Saga.viewSagaVentFlare b ON

 a.wellid=b.wellid;
 ---------------------------------------------------------------------
 --ProductionForecast Loading
 ---------------------------------------------------------------------
 --drop table Saga.ProductionForecast;
CREATE TABLE IF NOT EXISTS Saga.ProductionForecast
(
  WellID INT64,
  UWI STRING,
  ProductionMonth DATE,
  Oil Float64,
  Gas Float64,
  Water Float64,
  SequenceMonth INT64,
  BasinName STRING,
  CalculatedMajorPhase STRING,
  County STRING,
  DisplayFormation STRING,
  DispositionType STRING,
  FieldName STRING,
  InterpretedProducingFormation STRING,
  LastInjectionFormation STRING,
  LastInjectionType STRING,
  LeaseID STRING,
  LeaseName STRING,
  OperatorName STRING,
  OriginalOperator STRING,
  Slant STRING,
  StateName STRING,
  UltimateOwner STRING,
  UWI10 STRING,
  WellStatus STRING,
  WellType STRING
)
CLUSTER BY WellID, ProductionMonth, SequenceMonth, OperatorName;

TRUNCATE TABLE Saga.ProductionForecast;
insert into Saga.ProductionForecast
SELECT 
b.WellID
,b.UWI
,b.ProductionMonth
,b.Oil 
,b.Gas 
,b.Water
,b.SequenceMonth
,BasinName
,CalculatedMajorPhase
,County
,DisplayFormation
,DispositionType
,FieldName
,InterpretedProducingFormation
,LastInjectionFormation
,LastInjectionType
,LeaseID
,LeaseName
,OperatorName
,OriginalOperator
,Slant
,StateName
,UltimateOwner
,UWI10
,WellStatus
,WellType
 FROM Saga.WellPerformanceHeader a

 JOIN Saga.viewSagaProductionForecast b ON

 a.wellid=b.wellid;
  ---------------------------------------------------------------------
 --Perforations Loading
 -----------------------------------------------------------------------
 drop table Saga.Perforations;
create table Saga.Perforations 
cluster by WellID
as 

SELECT * FROM Saga.viewSagaWellPerforations;

  ---------------------------------------------------------------------
 --Tests Loading
 -----------------------------------------------------------------------
 drop table Saga.Tests;
 create table Saga.Tests 
cluster by WellID
as 

SELECT * FROM Saga.viewSagaWellTests;
------------------------------------------------------------------------
 --Casing Loading
 -----------------------------------------------------------------------
Create table if not EXISTS  Saga.Casing
(
WellID	INT64
,DrillStringType	STRING
,Purpose	STRING
,HoleSize	NUMERIC(20,3)
,Size	NUMERIC(20,3)
,Weight	STRING
,TopDepth	INT64
,BottomDepth	INT64
,MultiStageToolDepth	STRING
,Grade	STRING
,SlurryVolume	NUMERIC(20,3)
,Cement	NUMERIC(20,3)
,CementClass	STRING
,ScreenType	STRING
,CementPSI	NUMERIC(20,3)
,AmountPulled	INT64
,CementTop	INT64
,CementBottom	INT64
,PackerSet	STRING
,MultiplePackers	BOOLEAN
)
cluster by WellID;

truncate table Saga.Casing;
Insert into Saga.Casing
SELECT 
 WellID,DrillStringType,Purpose,HoleSize,Size,Weight,TopDepth,BottomDepth,
   MultiStageToolDepth,Grade,SlurryVolume,Cement,CementClass,ScreenType,CementPSI,AmountPulled,
   Cementtop,CementBottom,PackerSet,IFNULL(MultiplePackers,false) as MultiplePackers
 FROM tgs_gold.Completiondata_DrillString
  UNION DISTINCT
   SELECT WellID,DrillStringType,Purpose,HoleSize,Size,Weight,TopDepth,BottomDepth,
   MultiStageToolDepth,Grade,SlurryVolume,Cement,CementClass,ScreenType,CementPSI,AmountPulled,
   Cementtop,CementBottom,PackerSet,IFNULL(MultiplePackers,false) as MultiplePackers
   FROM  tgs_gold.dbo_DrillString ;
------------------------------------------------------------------------
 --WellFormationTop Loading
 ----------------------------------------------------------------------
--Create table if not EXISTS Saga_dev.WellFormationTop
--   (
--   WellID	INT64
--   ,FormationName	STRING
--   ,FormationAge	STRING
--   ,FormationFranklinCode	STRING
--   ,FormationTopDepth	INT64
--   ,FormationStateID	STRING
--)
--cluster by WellID;
--
--truncate table Saga.WellFormationTop;
--insert into Saga.WellFormationTop(wellid,
--FormationName,
--FormationTopDepth)
--
--SELECT 
--wellid,
--FormationName,
--FormationTopDepth
-- FROM Saga.formationtop ;

Create or replace table  Saga.WellFormationTop
cluster by WellID
as 

SELECT * FROM `tgs_gold.dbo_WellFormationTop`;

END;

CREATE OR REPLACE PROCEDURE `Saga.ProcSagaStatsInjection_usingTempTable`()
begin

Create temp table StatsInjection (Wellid int64,UWI String,APIState string,CumLiquid DECIMAL(20,3), CumGas DECIMAL(20,3),
                           FirstMonth DATE, LastMonth DATE,HasInjection BOOLEAN, ActiveInjection BOOLEAN, LastInjectionType String, LastInjectionFormation String);
                           

                      INSERT INTO StatsInjection          
(wellid,UWI,APIState,Cumliquid, CumGas, FirstMonth, LastMonth,HasInjection)            
SELECT wi.WellID,wi.UWI,wi.APIState,         
       COALESCE(SUM(TotalLiquid), 0) AS CumLiquid,            
       COALESCE(SUM(TotalGas), 0) AS CumGas,           
       MIN(DATE(InjectionMonth)) AS FirstMonth,              
       MAX(DATE(InjectionMonth)) AS LastMonth,
          true as HasInjection
FROM  Saga.Injection wi 
WHERE (TotalLiquid>0 or TotalGas>0)   
GROUP BY wi.WellID,wi.UWI,wi.APIState;

create temp table LastInjectionMonth (APIState string, APILastMonth date);
insert into LastInjectionMonth (apistate, APILastMonth)
select apistate, max(date(LastMonth)) APILastMonth
from StatsInjection 
group by apistate  ;

UPDATE StatsInjection si
SET ActiveInjection = CASE WHEN DATE_DIFF(date(si.LastMonth), date(lm.APILastMonth),MONTH) <=6 THEN true ELSE false END 
--select si.*, lm.lastmonth
FROM LastInjectionMonth lm
where  si.apistate = lm.apistate;

UPDATE StatsInjection  S
SET LastInjectionType= I.InjectionType,
    LastInjectionFormation= I.InjectionFormation

--SELECT S.*
FROM  Saga.Injection I 
where I.WellID = S.WellID AND I.InjectionMonth = S.LastMonth;

Update Saga.WellPerformanceHeader W
set 
CumInjectionLiquid=S.CumLiquid
,CumInjectionGas=S.CumGas
,InjectionFirstMonth=S.FirstMonth
,InjectionLastMonth=S.LastMonth
,HasInjection=S.HasInjection
,Activeinjection=S.Activeinjection
,LastInjectionType=S.LastInjectionType
,LastInjectionFormation=S.LastInjectionFormation
from 
StatsInjection S 
where  S.WellID = w.WellID;
end;

CREATE OR REPLACE PROCEDURE `Saga.ProcSagaStatsProduction_usingTempTable`()
begin
--Create the temp table for holding and Processing different attributes
 
   CREATE TEMP TABLE SagaStatsProduction (
	WellID int64 not null,
	UWI String not null,
	APIState string not null,
	FirstMonth date ,
	LastMonth date ,
	CumOil decimal(20, 3) ,
	CumGas decimal(20, 3) ,
	CumGOR decimal(20, 3) ,
	CumWater decimal(20, 3) ,
	CumYield decimal(20, 3) ,
	CumBOE decimal(20, 3) ,
	FirstMoOil decimal(20, 3) ,
	FirstMoGas decimal(20, 3) ,
	FirstMoWater decimal(20, 3) ,
	SecondMoOil decimal(20, 3) ,
	SecondMoGas decimal(20, 3) ,
	SecondMoWater decimal(20, 3) ,
	Cum3MoOil decimal(20, 3) ,
	Cum3MoGas decimal(20, 3) ,
	Cum3MoWater decimal(20, 3) ,
	Cum3MoGOR decimal(20, 3) ,
	Cum3MoYield decimal(20, 3) ,
	Cum6MoOil decimal(20, 3) ,
	Cum6MoGas decimal(20, 3) ,
	Cum6MoWater decimal(20, 3) ,
	Cum6MoGOR decimal(20, 3) ,
	Cum6MoYield decimal(20, 3) ,
	Cum6MoBOE decimal(20, 3) ,
	Cum9MoOil decimal(20, 3) ,
	Cum9MoGas decimal(20, 3) ,
	Cum9MoWater decimal(20, 3) ,
	Cum9MoGOR decimal(20, 3) ,
	Cum9MoYield decimal(20, 3) ,
	Cum1YrOil decimal(20, 3) ,
	Cum1YrGas decimal(20, 3) ,
	Cum1YrWater decimal(20, 3) ,
	Cum1YrGOR decimal(20, 3) ,
	Cum1YrYield decimal(20, 3) ,
	Cum1YrBOE decimal(20, 3) ,
	Cum2YrOil decimal(20, 3) ,
	Cum2YrGas decimal(20, 3) ,
	Cum2YrWater decimal(20, 3) ,
	Cum3YrOil decimal(20, 3) ,
	Cum3YrGas decimal(20, 3) ,
	Cum3YrWater decimal(20, 3) ,
	Cum5YrOil decimal(20, 3) ,
	Cum5YrGas decimal(20, 3) ,
	Cum5YrWater decimal(20, 3) ,
	Cum10YrOil decimal(20, 3) ,
	Cum10YrGas decimal(20, 3) ,
	Cum10YrWater decimal(20, 3) ,
	Cum15YrOil decimal(20, 3) ,
	Cum15YrGas decimal(20, 3) ,
	Cum15YrWater decimal(20, 3) ,
	Cum20YrOil decimal(20, 3) ,
	Cum20YrGas decimal(20, 3) ,
	Cum20YrWater decimal(20, 3) ,
	Recent1MoOil decimal(20, 3) ,
	Recent1MoGas decimal(20, 3) ,
	Recent1MoWater decimal(20, 3) ,
	Recent3MoOil decimal(20, 3) ,
	Recent3MoGas decimal(20, 3) ,
	Recent3MoWater decimal(20, 3) ,
	Recent6MoOil decimal(20, 3) ,
	Recent6MoGas decimal(20, 3) ,
	Recent6MoWater decimal(20, 3) ,
	Recent1YrOil decimal(20, 3) ,
	Recent1YrGas decimal(20, 3) ,
	Recent1YrWater decimal(20, 3) ,
	MaxOil decimal(20, 3) ,
	MaxOilMonth date ,
	MaxOilPlus2 decimal(20, 3) ,
	MaxGas decimal(20, 3) ,
	MaxGasMonth date ,
	MaxGasPlus2 decimal(20, 3) ,
	MaxWater decimal(20, 3) ,
	MaxWaterMonth date ,
	MaxWaterPlus2 decimal(20, 3) ,
	MaxBOE decimal(20, 3) ,
	MaxBOEMonth date ,
	Latest1YrBOE6 decimal(20, 3) ,
	Latest1YrBOE20 decimal(20, 3) 
   );


   INSERT INTO SagaStatsProduction              
(WellID,UWI,APIState,CumOil, CumGas, CumWater,MaxOil, MaxGas, MaxWater)              
SELECT Distinct p.WellID,p.UWI,p.APIState
  , COALESCE(SUM(p.Oil), 0) AS CumOil              
  , COALESCE(SUM(p.Gas), 0) AS CumGas              
  , COALESCE(SUM(p.Water), 0) AS CumWater              
  , COALESCE(MAX(p.Oil), 0) AS MaxOil              
  , COALESCE(MAX(p.Gas), 0) AS MaxGas              
  , COALESCE(MAX(p.Water), 0) AS MaxWater              
FROM  Saga.Production p
GROUP BY p.WellID,p.UWI,p.APIState         
HAVING SUM(p.Oil) >0 OR SUM(p.Gas) >0 OR SUM(p.Water)>0  ;

Update SagaStatsProduction sp      
 set Lastmonth = LastProductionMonth   ,
	 FirstMonth = FirstProductionMonth
       
--select *
 from(       
			 select wpr.WellID,
			 MAX( date(wpr.ProductionMonth))as LastProductionMonth  ,
			 MIN(date(wpr.ProductionMonth)) as FirstProductionMonth   
			  
			 From Saga.Production wpr 
			 Where  ( wpr.Oil >0 OR  wpr.Gas >0 ) 
			 Group by wpr.WellID 
			 
   )a  
 where  sp.WellID = a.WellID ;


 -- ------------------------------------------------------------------------------------              
-- Get the first months production              
-- ------------------------------------------------------------------------------------              
         
UPDATE SagaStatsProduction  wps              
SET FirstMoOil = COALESCE(wpr.Oil, 0),              
    FirstMoGas = COALESCE(wpr.Gas, 0),              
    FirstMoWater = COALESCE(wpr.Water, 0)      
	--Select    wps.WellID, COALESCE(wpr.Oil, 0)Oil,COALESCE(wpr.Gas, 0)Gas,COALESCE(wpr.Water, 0) Water   
FROM Saga.Production wpr where wps.WellID = wpr.WellID    
AND date(wps.FirstMonth) = date(wpr.ProductionMonth);

-- ------------------------------------------------------------------------------------              
-- Get the second month's production              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction s              
SET SecondMoOil = COALESCE(T.Oil, 0),              
    SecondMoGas = COALESCE(T.Gas, 0),              
    SecondMoWater = COALESCE(T.Water, 0)              
from                  
  (               
  select p.WellID,              
    p.Oil as oil,              
    p.Gas as Gas,              
    p.Water as Water              
  FROM  Saga.Production p inner join                
  ( 
    select S.WellID,MIN(date(p.ProductionMonth)) as productionMonth              
    From  SagaStatsProduction s
    JOIN  Saga.Production p  ON s.WellID = p.WellID 
          AND  date(s.FirstMonth) < date(p.ProductionMonth)          
   Group by S.WellID           
                 
  ) l on p.WellID=l.WellID  and date(p.ProductionMonth)= date(l.productionMonth)
                
 ) T where s.WellID=T.WellID ;


-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 3 Months              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET Cum3MoOil = COALESCE(T.Oil, 0),              
    Cum3MoGas = COALESCE(T.Gas, 0),              
    Cum3MoWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 3 MONTH) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 2 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID  ;

-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 6 Months              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET Cum6MoOil = COALESCE(T.Oil, 0),              
    Cum6MoGas = COALESCE(T.Gas, 0),              
    Cum6MoWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 6 MONTH) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 5 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID; 
-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 9 Months              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET Cum9MoOil = COALESCE(T.Oil, 0),              
    Cum9MoGas = COALESCE(T.Gas, 0),              
    Cum9MoWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 9 MONTH) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 8 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID; 
-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 12 Months              
-- ------------------------------------------------------------------------------------   
UPDATE SagaStatsProduction wps              
SET Cum1yrOil = COALESCE(T.Oil, 0),              
    Cum1yrGas = COALESCE(T.Gas, 0),              
    Cum1yrWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 1 YEAR) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 11 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID; 

-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 2 years              
-- ------------------------------------------------------------------------------------   
UPDATE SagaStatsProduction wps              
SET Cum2yrOil = COALESCE(T.Oil, 0),              
    Cum2yrGas = COALESCE(T.Gas, 0),              
    Cum2yrWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 2 YEAR) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 23 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID; 

-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 3 years              
-- ------------------------------------------------------------------------------------   
UPDATE SagaStatsProduction wps              
SET Cum3yrOil = COALESCE(T.Oil, 0),              
    Cum3yrGas = COALESCE(T.Gas, 0),              
    Cum3yrWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 3 YEAR) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 35 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID; 
-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 5 years              
-- ------------------------------------------------------------------------------------   
UPDATE SagaStatsProduction wps              
SET Cum5yrOil = COALESCE(T.Oil, 0),              
    Cum5yrGas = COALESCE(T.Gas, 0),              
    Cum5yrWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 5 YEAR) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 59 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID; 
-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 10 years              
-- ------------------------------------------------------------------------------------   
UPDATE SagaStatsProduction wps              
SET Cum10yrOil = COALESCE(T.Oil, 0),              
    Cum10yrGas = COALESCE(T.Gas, 0),              
    Cum10yrWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 10 YEAR) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 119 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID; 
-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 15 years              
-- ------------------------------------------------------------------------------------   
UPDATE SagaStatsProduction wps              
SET Cum15yrOil = COALESCE(T.Oil, 0),              
    Cum15yrGas = COALESCE(T.Gas, 0),              
    Cum15yrWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 15 YEAR) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 179 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID;
-- ------------------------------------------------------------------------------------              
-- Calculate Cum of first 20 years              
-- ------------------------------------------------------------------------------------   
UPDATE SagaStatsProduction wps              
SET Cum20yrOil = COALESCE(T.Oil, 0),              
    Cum20yrGas = COALESCE(T.Gas, 0),              
    Cum20yrWater = COALESCE(T.Water, 0)              
FROM                
              
(              
 SELECT   wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
  
 FROM  SagaStatsProduction wps 
 JOIN  Saga.Production wpr  ON wps.WellID = wpr.WellID      
                              AND DATE_ADD( wps.FirstMonth,INTERVAL 20 YEAR) > date(wpr.ProductionMonth)              
                              AND DATE_ADD(wps.FirstMonth,INTERVAL 239 MONTH ) <= wps.LastMonth                         
              
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID; 
-- ------------------------------------------------------------------------------------              
-- Calculate Cum of most recent 1 Month              
-- ------------------------------------------------------------------------------------              
              
            
UPDATE SagaStatsProduction wps              
SET Recent1MoOil = COALESCE(wpr.Oil, 0),              
    Recent1MoGas = COALESCE(wpr.Gas, 0),              
    Recent1MoWater = COALESCE(wpr.Water, 0)               
  FROM  Saga.Production wpr where wps.WellID = wpr.WellID 
                                       AND wps.LastMonth = date(wpr.ProductionMonth);

-- ------------------------------------------------------------------------------------              
-- Calculate Cum of most recent 3 Month              
-- ------------------------------------------------------------------------------------              
              
UPDATE SagaStatsProduction wps              
SET Recent3MoOil = COALESCE(T.Oil, 0),              
    Recent3MoGas = COALESCE(T.Gas, 0),              
    Recent3MoWater = COALESCE(T.Water, 0)              
FROM            
(              
   SELECT wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
 FROM SagaStatsProduction wps
 JOIN Saga.Production wpr  ON wps.WellID = wpr.WellID 
                              AND DATE_ADD( wps.FirstMonth,INTERVAL -3 Month) < Date(wpr.ProductionMonth)                                 
							  AND DATE_ADD( wps.FirstMonth,INTERVAL -2 Month) >= wps.FirstMonth                     
 
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID;

-- ------------------------------------------------------------------------------------              
-- Calculate Cum of most recent 6 Month              
-- ------------------------------------------------------------------------------------              
              
UPDATE SagaStatsProduction wps              
SET Recent6MoOil = COALESCE(T.Oil, 0),              
    Recent6MoGas = COALESCE(T.Gas, 0),              
    Recent6MoWater = COALESCE(T.Water, 0)              
FROM            
(              
   SELECT wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
 FROM SagaStatsProduction wps
 JOIN Saga.Production wpr  ON wps.WellID = wpr.WellID 
                              AND DATE_ADD( wps.FirstMonth,INTERVAL -6 Month) < Date(wpr.ProductionMonth)                                 
							  AND DATE_ADD( wps.FirstMonth,INTERVAL -5 Month) >= wps.FirstMonth                     
 
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID;
-- ------------------------------------------------------------------------------------              
-- Calculate Cum of most recent 1 Year              
-- ------------------------------------------------------------------------------------              
              
UPDATE SagaStatsProduction wps              
SET Recent1yrOil = COALESCE(T.Oil, 0),              
    Recent1yrGas = COALESCE(T.Gas, 0),              
    Recent1yrWater = COALESCE(T.Water, 0)              
FROM            
(              
   SELECT wps.WellID,              
          SUM(wpr.Oil) AS Oil,              
          SUM(wpr.Gas) AS Gas,              
          Sum(wpr.Water) AS Water              
 FROM SagaStatsProduction wps
 JOIN Saga.Production wpr  ON wps.WellID = wpr.WellID 
                              AND DATE_ADD( wps.FirstMonth,INTERVAL -1 Year) < Date(wpr.ProductionMonth)                                 
							  AND DATE_ADD( wps.FirstMonth,INTERVAL -11 Month) >= wps.FirstMonth                     
 
   GROUP BY wps.WellID              
)  T where wps.WellID = T.WellID;
              
-------------------------------------------------------------------
--Update Latest1YrBOE6,Latest1YrBOE20
-------------------------------------------------------------------
UPDATE SagaStatsProduction S
SET 
Latest1YrBOE6=T.Latest1YrBOE6,
Latest1YrBOE20=T.Latest1YrBOE20
FROM 
(
Select wp.WellID,
sum(coalesce(wp.OIl,0))+(SUM(coalesce(wp.gas,0))/6) as Latest1YrBOE6,
sum(coalesce(wp.OIl,0))+(SUM(coalesce(wp.gas,0))/20) as Latest1YrBOE20
FROM 
(	    
	   Select S.Wellid,A.APIState,A.APILastMonth
       FROM SagaStatsProduction S
	   JOIN (
			   Select W.APIState,max(W.Lastmonth)APILastMonth
			   FROM SagaStatsProduction W 
			   GROUP BY W.APIState
	       ) A on S.APIState=A.APIState  	   
     
) A 
JOIN Saga.Production wp ON wp.Wellid=A.Wellid 
                                        and wp.ProductionMonth <=A.APILastMonth and wp.ProductionMonth >= DATE_ADD( A.APILastMonth,INTERVAL -11 Month)
GROUP BY wp.WellID
)T where T.WellID=S.WellID ;
-- ------------------------------------------------------------------------------------              
-- Get the max oil production date (first occurence)              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET MaxOilMonth = T.ProductionMonth              
FROM              
 (              
    SELECT wps.WellID, MIN(date(wpr.ProductionMonth)) AS ProductionMonth              
    FROM SagaStatsProduction wps 
   JOIN  Saga.Production wpr ON wps.WellID = wpr.WellID  
                                     AND wps.MaxOil = COALESCE(wpr.Oil,0)                                      
    GROUP BY wps.WellID              
 ) AS T where wps.WellID = T.WellID;
 -- ------------------------------------------------------------------------------------         
-- Get the max oil plus the next two              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET MaxOilPlus2 = T.Oil              
FROM            
(              
   SELECT wps.WellID, SUM(wpr.Oil) AS Oil              
   FROM SagaStatsProduction wps
  JOIN  Saga.Production wpr ON wps.WellID = wpr.WellID  
                              AND date(wpr.ProductionMonth) >= wps.MaxOilMonth              
                              AND date(wpr.ProductionMonth) < DATE_ADD( wps.MaxOilMonth,INTERVAL 3 MONTH)                  
   GROUP BY wps.WellID
   HAVING COUNT(*) > 2              
) AS T where wps.WellID = T.WellID ;                 
              
-- ------------------------------------------------------------------------------------              
-- Get the max gas production date (first occurence)              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET MaxGasMonth = T.ProductionMonth              
FROM              
 (              
    SELECT wps.WellID, MIN(date(wpr.ProductionMonth)) AS ProductionMonth              
    FROM SagaStatsProduction wps 
	JOIN Saga.Production wpr ON wps.WellID = wpr.WellID  
                                       AND wps.MaxGas = COALESCE(wpr.Gas,0)                                     
    GROUP BY wps.WellID              
 ) AS T where wps.WellID = T.WellID;

  -- ------------------------------------------------------------------------------------         
-- Get the max Gas plus the next two              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET MaxGasPlus2 = T.Gas              
FROM            
(              
   SELECT wps.WellID, SUM(wpr.Gas) AS Gas              
   FROM SagaStatsProduction wps
  JOIN  Saga.Production wpr ON wps.WellID = wpr.WellID  
                              AND date(wpr.ProductionMonth) >= wps.MaxGasMonth              
                              AND date(wpr.ProductionMonth) < DATE_ADD( wps.MaxGasMonth,INTERVAL 3 MONTH)                  
   GROUP BY wps.WellID
   HAVING COUNT(*) > 2              
) AS T where wps.WellID = T.WellID ;   
-- ------------------------------------------------------------------------------------              
-- Get the max water production date (first occurence)              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET MaxWaterMonth = T.ProductionMonth              
FROM              
 (              
    SELECT wps.WellID, MIN(date(wpr.ProductionMonth)) AS ProductionMonth              
    FROM SagaStatsProduction wps 
   JOIN  Saga.Production wpr ON wps.WellID = wpr.WellID  
                                        AND wps.MaxWater = COALESCE(wpr.Water,0)                                                  
    GROUP BY wps.WellID
 ) AS T where wps.WellID = T.WellID ;
   -- ------------------------------------------------------------------------------------         
-- Get the max water plus the next two              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET MaxWaterPlus2 = T.Water              
FROM            
(              
   SELECT wps.WellID, SUM(wpr.Water) AS Water              
   FROM SagaStatsProduction wps
  JOIN  Saga.Production wpr ON wps.WellID = wpr.WellID  
                              AND date(wpr.ProductionMonth) >= wps.MaxWaterMonth              
                              AND date(wpr.ProductionMonth) < DATE_ADD( wps.MaxWaterMonth,INTERVAL 3 MONTH)                  
   GROUP BY wps.WellID
   HAVING COUNT(*) > 2              
) AS T where wps.WellID = T.WellID ;

-- ------------------------------------------------------------------------------------              
-- Get the max BOE and BOE Date (first occurence)              
-- ------------------------------------------------------------------------------------              
UPDATE SagaStatsProduction wps              
SET wps.MaxBOE = T.MaxBOE,              
    wps.MaxBOEMonth = T.ProductionMonth              
FROM               
(              
   SELECT  WellID,           
          (COALESCE(wpr.Oil,0) + COALESCE(wpr.Gas,0)/6) AS MaxBOE,              
          date(ProductionMonth) as ProductionMonth,              
          ROW_NUMBER() OVER (PARTITION BY WellID ORDER BY (COALESCE(wpr.Oil,0) + COALESCE(wpr.Gas,0)/6) DESC, date(ProductionMonth) ASC) AS RowNumber              
   FROM   Saga.Production wpr           
                
) AS T               
where 
wps.WellID = T.WellID  AND T.RowNumber = 1  ;            
-------------------------------------              
-- calculate GORs              
-------------------------------------              
UPDATE SagaStatsProduction 
SET               
 CumGOR = case when CumOil=0 then null else cast( 1000*CumGas/CumOil as decimal) end,              
 Cum3MoGOR = case when Cum3MoOil=0 then null else cast( 1000*Cum3MoGas/Cum3MoOil as decimal) end,              
 Cum6MoGOR = case when Cum6MoOil=0 then null else cast( 1000*Cum6MoGas/Cum6MoOil as decimal) end,              
 Cum9MoGOR = case when Cum9MoOil=0 then null else cast( 1000*Cum9MoGas/Cum9MoOil as decimal) end,              
 Cum1YrGOR = case when Cum1YrOil=0 then null else cast( 1000*Cum1YrGas/Cum1YrOil as decimal) end ,
 -------------------------------------------------
 -- Following fields are calculated during updateuswells for Regular wells
 --------------------------------------------------
 CumYield=CASE WHEN coalesce(CumGas,0) <> 0 THEN (CumOil*1000)/CumGas END,        
 CumBOE=CumOil+(CumGas/6.00),        
 Cum3MoYield=case when coalesce(Cum3MoGas,0)=0 then null else cast( (Cum3MoOil*1000)/coalesce(Cum3MoGas,0) as decimal) end ,        
 Cum6MoYield=case when coalesce(Cum6MoGas,0)=0 then null else cast(((Cum6MoOil*1000)/coalesce(Cum6MoGas,0)) as decimal) end ,        
 Cum6MoBOE=Cum6MoOil+(Cum6MoGas/6.00),        
 Cum9MoYield=case when coalesce(Cum9MoGas,0)=0 then null else cast((( Cum9MoOil*1000)/coalesce(Cum9MoGas,0)) as decimal) end ,        
 Cum1YrYield=case when coalesce(Cum1YrGas,0)=0 then null else cast((( Cum1YrOil*1000)/coalesce(Cum1YrGas,0)) as decimal) end ,        
 Cum1YrBOE=Cum1YrOil+(Cum1YrGas/6.00) where 1=1;

     
    UPDATE Saga.WellPerformanceHeader W 
	SET   FirstMonth=S.FirstMonth
		,LastMonth=S.LastMonth
		,CumOil=S.CumOil
		,CumGas=S.CumGas
		,CumGOR=S.CumGOR
		,CumWater=S.CumWater
		,CumYield=S.CumYield
		,CumBOE=S.CumBOE
		,FirstMoOil=S.FirstMoOil
		,FirstMoGas=S.FirstMoGas
		,FirstMoWater=S.FirstMoWater
		,SecondMoOil=S.SecondMoOil
		,SecondMoGas=S.SecondMoGas
		,SecondMoWater=S.SecondMoWater
		,Cum3MoOil=S.Cum3MoOil
		,Cum3MoGas=S.Cum3MoGas
		,Cum3MoWater=S.Cum3MoWater
		,Cum3MoGOR=S.Cum3MoGOR
		,Cum3MoYield=S.Cum3MoYield
		,Cum6MoOil=S.Cum6MoOil
		,Cum6MoGas=S.Cum6MoGas
		,Cum6MoWater=S.Cum6MoWater
		,Cum6MoGOR=S.Cum6MoGOR
		,Cum6MoYield=S.Cum6MoYield
		,Cum6MoBOE=S.Cum6MoBOE
		,Cum9MoOil=S.Cum9MoOil
		,Cum9MoGas=S.Cum9MoGas
		,Cum9MoWater=S.Cum9MoWater
		,Cum9MoGOR=S.Cum9MoGOR
		,Cum9MoYield=S.Cum9MoYield
		,Cum1YrOil=S.Cum1YrOil
		,Cum1YrGas=S.Cum1YrGas
		,Cum1YrWater=S.Cum1YrWater
		,Cum1YrGOR=S.Cum1YrGOR
		,Cum1YrYield=S.Cum1YrYield
		,Cum1YrBOE=S.Cum1YrBOE
		,Cum2YrOil=S.Cum2YrOil
		,Cum2YrGas=S.Cum2YrGas
		,Cum2YrWater=S.Cum2YrWater
		,Cum3YrOil=S.Cum3YrOil
		,Cum3YrGas=S.Cum3YrGas
		,Cum3YrWater=S.Cum3YrWater
		,Cum5YrOil=S.Cum5YrOil
		,Cum5YrGas=S.Cum5YrGas
		,Cum5YrWater=S.Cum5YrWater
		,Cum10YrOil=S.Cum10YrOil
		,Cum10YrGas=S.Cum10YrGas
		,Cum10YrWater=S.Cum10YrWater
		,Cum15YrOil=S.Cum15YrOil
		,Cum15YrGas=S.Cum15YrGas
		,Cum15YrWater=S.Cum15YrWater
		,Cum20YrOil=S.Cum20YrOil
		,Cum20YrGas=S.Cum20YrGas
		,Cum20YrWater=S.Cum20YrWater
		,Recent1MoOil=S.Recent1MoOil
		,Recent1MoGas=S.Recent1MoGas
		,Recent1MoWater=S.Recent1MoWater
		,Recent3MoOil=S.Recent3MoOil
		,Recent3MoGas=S.Recent3MoGas
		,Recent3MoWater=S.Recent3MoWater
		,Recent6MoOil=S.Recent6MoOil
		,Recent6MoGas=S.Recent6MoGas
		,Recent6MoWater=S.Recent6MoWater
		,Recent1YrOil=S.Recent1YrOil
		,Recent1YrGas=S.Recent1YrGas
		,Recent1YrWater=S.Recent1YrWater
		,MaxOil=S.MaxOil
		,MaxOilMonth=S.MaxOilMonth
		,MaxOilPlus2=S.MaxGasPlus2
		,MaxGas=S.MaxGas
		,MaxGasMonth=S.MaxGasMonth
		,MaxGasPlus2=S.MaxGasPlus2
		,MaxWater=S.MaxWater
		,MaxWaterMonth=S.MaxWaterMonth
		,MaxWaterPlus2=S.MaxWaterPlus2
		,MaxBOE=S.MaxBOE
		,MaxBOEMonth=S.MaxBOEMonth
		,Latest1YrBOE6=S.Latest1YrBOE6
		,Latest1YrBOE20=S.Latest1YrBOE20
		,BOEMaxPer1000Ft=CASE WHEN coalesce(W.LateralLength,0)=0 THEN NULL 
                    ELSE CAST(((S.MaxBOE*1000)/LateralLength) as DECIMAL) END  

      FROM 
	  SagaStatsProduction S where S.wellid=W.wellid;
            end;
			
			
			CREATE OR REPLACE PROCEDURE `Saga.ProcSagaStatsVentFlare_usingTempTable`()
Begin
 Create temp table VentFlareStats
		(		
		WellID int64,
		UWI string,
		APIState string,
		FirstMonth	date,
		LastMonth	date,
		CumVFVol	Decimal(20,3),
		HasVF	boolean,
		ActiveVF boolean,
		HasDisposition	boolean,
		DispositionType	string
		)	;

    Insert into VentFlareStats(WellID,UWI,APIState,HasDisposition,HasVF,DispositionType,ActiveVF)
	   SELECT DISTINCT wv.WellID,wv.UWI,wv.APIState,true,false,'USED',false 
	   FROM Saga.VentFlare wv
	   WHERE  (wv.TotalDisposed > 0 or wv.TotalUsed > 0 );

      UPDATE VentFlareStats S 
	   SET CumVFVol=X.TotalDisposed,
	       FirstMonth=X.Firstmonth,
		   LastMonth=X.Lastmonth,
		   HasVF=true
	   
	   FROM  (
	           SELECT   w.WellID,
			   SUM(w.TotalDisposed )AS TotalDisposed,
			   MIN(date(w.ProductionMonth)) AS Firstmonth, MAX(date(ProductionMonth)) AS Lastmonth				
	           FROM Saga.VentFlare w
			   WHERE w.TotalDisposed > 0 
			   GROUP by w.WellID
	       ) X where X.WellID=S.WellID ;

         CREATE temp TABLE lm(APIState String,APILastmonth date);

	   INSERT INTO lm(APIState,APILastmonth)
	   SELECT w.APIState,MAX(date(LastMonth)) as LastMonth
	   FROM VentFlareStats w
	   WHERE HasVF=true
	   GROUP by w.APIState;

	    UPDATE VentFlareStats S
		SET DispositionType=CASE WHEN coalesce(S.CumVFVol,0) > 0 THEN 'Disposed' ELSE 'Used' END ,
		ActiveVF=CASE WHEN DATE_DIFF( date(S.LastMonth), date(lm.APILastmonth),MONTH) <=6 THEN true ELSE false END 
		FROM lm LM 
    where LM.APIState=S.APIState;

    UPDATE Saga.WellPerformanceHeader W
        SET VentFlareFirstMonth=S.FirstMonth
		,VentFlareLastMonth=S.LastMonth
		,CumVentFlareVol=S.CumVFVol
		,HasVentFlare=S.HasVF
		,HasDisposition=S.HasDisposition
		,DispositionType=S.DispositionType
		,ActiveVentFlare=S.ActiveVF
        FROM VentFlareStats s 
        where 
        
        s.wellid=w.wellid;

end;


CREATE OR REPLACE PROCEDURE `Saga.ProcSagaStatsWellForecast_usingTempTable`()
begin
CREATE temp TABLE SagaStatsWellForecast(
	WellID int64 NOT null,
	UWI String NOT null ,	
	ForecastOilRemaining decimal(20,3) ,
	ForecastOilUltimate decimal(20,3) ,
	ForecastGasRemaining decimal(20,3) ,
	ForecastGasUltimate decimal(20,3) ,
	ForecastWaterRemaining decimal(20,3) ,
	ForecastWaterUltimate decimal(20,3) ,
	ForecastGORUltimate decimal(20,3) ,
	ForecastYieldUltimate decimal(20,3) ,
	ForecastWaterCutUltimate decimal(20,3) ,
	ForecastBOERemaining decimal(20,3) ,
	ForecastBOEUltimate decimal(20,3) ,
	ForecastOil1MonthRate decimal(20,3) ,
	ForecastGas1MonthRate decimal(20,3) ,
	ForecastWater1MonthRate decimal(20,3) ,
	ForecastOil1YearCum decimal(20,3) ,
	ForecastGas1YearCum decimal(20,3) ,
	ForecastWater1YearCum decimal(20,3) ,
	ForecastBOE1MonthRate decimal(20,3) ,
	ForecastBOE1YearCum decimal(20,3) 	
	);
  INSERT INTO SagaStatsWellForecast(WellID ,UWI)	  	
	 SELECT distinct WellID,UWI
	 FROM Saga.ProductionForecast;

   /* ForecastOil1MonthRate, ForecastGas1MonthRate, ForecastWater1MonthRate - 1st month oil/gas/water */
	
	UPDATE SagaStatsWellForecast Head
	SET ForecastOil1MonthRate = Prod.Oil,
		ForecastGas1MonthRate= Prod.Gas,
		ForecastWater1MonthRate = Prod.Water
	FROM 
	(SELECT distinct rs.WellID,cast(coalesce(SUM(round(rs.Oil,3)), 0) as decimal) AS Oil, cast(coalesce(SUM(round(rs.Gas,3)), 0) as decimal) AS Gas, coalesce(SUM(round(cast(substring(cast(rs.Water as string),1,10) as decimal),3)), 0)  AS Water
		FROM (
			SELECT WellID, Oil, Gas, Water, ROW_NUMBER()
			  over (Partition BY WellID
					ORDER BY date(ProductionMonth) ASC ) AS RowNumber
			FROM Saga.ProductionForecast
			WHERE ((Oil > 0) OR (Gas > 0) OR (Water > 0))
			) rs 
			WHERE RowNumber = 1
	GROUP BY rs.WellID) as Prod
	WHERE Head.WellID = Prod.WellID;

  /* ForecastOil1YearCum, ForecastGas1YearCum, ForecastWater1YearCum - 1st Year Oil/Gas/Water total */
	UPDATE SagaStatsWellForecast Head
	SET ForecastOil1YearCum = Prod.Oil,
		ForecastGas1YearCum = Prod.Gas,
		ForecastWater1YearCum = Prod.Water
	FROM 
	(SELECT distinct rs.WellID,cast(coalesce(SUM(round(rs.Oil,3)), 0) as decimal) AS Oil, cast(coalesce(SUM(round(rs.Gas,3)), 0) as decimal) AS Gas, coalesce(SUM(round(cast(substring(cast(rs.Water as string),1,10) as decimal),3)), 0)  AS Water
		FROM (
			SELECT WellID, Oil, Gas, Water, ROW_NUMBER()
			  over (Partition BY WellID
					ORDER BY date(ProductionMonth) ASC ) AS RowNumber
			FROM Saga.ProductionForecast
		
			) rs 
			WHERE RowNumber <= 12 
	GROUP BY rs.WellID) as Prod
	WHERE Head.WellID = Prod.WellID;

  /* ForecastOilRemaining, ForecastGasRemaining, ForecastWaterRemaining - Oil/Gas/Water Forecast total */
	UPDATE SagaStatsWellForecast Head
	SET ForecastOilRemaining = Prod.Oil,
		ForecastGasRemaining = Prod.Gas,
		ForecastWaterRemaining = Prod.Water
	FROM 
	(select distinct WellID,cast(coalesce(SUM(round(Oil,3)), 0) as decimal) AS Oil, cast(coalesce(SUM(round(Gas,3)), 0) as decimal) AS Gas, coalesce(SUM(round(cast(substring(cast(Water as string),1,10) as decimal),3)), 0)  AS Water
			FROM Saga.ProductionForecast		
			GROUP BY WellID) as Prod
	WHERE Head.WellID = Prod.WellID;

  /* ForecastOilUltimate, ForecastGasUltimate, ForecastWaterUltimate - Sum of total Cumulative Oil + total Forecast Oil */
	UPDATE SagaStatsWellForecast C
	SET ForecastOilUltimate = coalesce(CumOil,0) + ForecastOilRemaining,
		ForecastGasUltimate = coalesce(CumGas,0) + ForecastGasRemaining,
		ForecastWaterUltimate = coalesce(CumWater,0) + ForecastGasRemaining
	 FROM 
	  Saga.WellPerformanceHeader V where C.wellid=V.wellid;


/* ForecastGORUltimate, ForecastYieldUltimate, ForecastWaterCutUltimate  */
	UPDATE SagaStatsWellForecast
	SET ForecastGORUltimate = 
			CASE
				WHEN ForecastOilUltimate = 0 THEN 0
				ELSE (ForecastGasUltimate * 1000) / ForecastOilUltimate END,

		ForecastYieldUltimate = 
		   CASE 
				WHEN ForecastGasUltimate = 0 THEN 0
				ELSE (ForecastOilUltimate / ForecastGasUltimate) * 1000 END,
	
		ForecastWaterCutUltimate = 
		   CASE 
				WHEN ForecastOilUltimate + ForecastGasUltimate = 0 THEN 0
				ELSE ForecastWaterUltimate / (ForecastOilUltimate + ForecastGasUltimate) END
		,ForecastBOE1MonthRate = (ForecastGas1MonthRate / 6.0) + ForecastOil1MonthRate
		,ForecastBOE1YearCum = (ForecastGas1YearCum / 6.0) + ForecastOil1YearCum
		,ForecastBOERemaining = (ForecastGasRemaining / 6.0) + ForecastOilRemaining
		,ForecastBOEUltimate = (ForecastGasUltimate / 6.0) + ForecastOilUltimate
    where 1=1;

    UPDATE Saga.WellPerformanceHeader W
    SET WellForecastOilRemaining=S.ForecastOilRemaining
	,WellForecastOilUltimate=S.ForecastOilUltimate
	,WellForecastGasRemaining=S.ForecastGasRemaining
	,WellForecastGasUltimate=S.ForecastGasUltimate
	,WellForecastWaterRemaining=S.ForecastWaterRemaining
	,WellForecastWaterUltimate=S.ForecastWaterUltimate
	,WellForecastGORUltimate=S.ForecastGORUltimate
	,WellForecastYieldUltimate=S.ForecastYieldUltimate
	,WellForecastWaterCutUltimate=S.ForecastWaterCutUltimate
	,WellForecastBOERemaining=S.ForecastBOERemaining
	,WellForecastBOEUltimate=S.ForecastBOEUltimate
	,WellForecastOil1MonthRate=S.ForecastOil1MonthRate
	,WellForecastGas1MonthRate=S.ForecastGas1MonthRate
	,WellForecastWater1MonthRate=S.ForecastWater1MonthRate
	,WellForecastOil1YearCum=S.ForecastOil1YearCum
	,WellForecastGas1YearCum=S.ForecastGas1YearCum
	,WellForecastWater1YearCum=S.ForecastWater1YearCum
	,WellForecastBOE1MonthRate=S.ForecastBOE1MonthRate
	,WellForecastBOE1YearCum=S.ForecastBOE1YearCum
	,EURPer1000Ft=CASE WHEN coalesce(W.LateralLength,0)=0 THEN  null
                 ELSE cast(((S.ForecastBOEUltimate*1000)/W.LateralLength) as DECIMAL) END
    FROM  SagaStatsWellForecast S where S.wellid = W.wellid;

end;

CREATE OR REPLACE PROCEDURE `Saga.ProcSagaWellPath`()
BEGIN

declare lowerlimit INT64 DEFAULT 0;
declare upperlimit INT64 DEFAULT 0;
declare maxvalue INT64 DEFAULT 0;
declare increment  INT64 DEFAULT 1250000;

DROP TABLE IF EXISTS Saga.ds_temp;
CREATE TABLE IF NOT EXISTS Saga.ds_temp (
  UWI STRING,
  DepthObservationNumber INT64,
  surfacelat STRING,
  SurfaceLong STRING,
  ROWNUM   INT64)
  CLUSTER BY UWI, ROWNUM ;

INSERT INTO Saga.ds_temp (UWI,DepthObservationNumber,SurfaceLat,SurfaceLong,ROWNUM)   
SELECT
  d.UWI,
  d.DepthObservationNumber,
  CAST(s.SurfaceLatitude_WGS84 + d.DeltaLatitude AS string) AS surfacelat,
  CAST(s.SurfaceLongitude_WGS84 + d.DeltaLongitude AS string) AS surfacelong,
  RANK() OVER (ORDER BY UWI) ROWNUM
  
FROM (SELECT *
       FROM (SELECT UWI,DepthObservationNumber,DeltaLongitude,DeltaLatitude, 
              ROW_NUMBER() OVER (Partition by UWI, CAST(DeltaLatitude AS string),CAST(DeltaLongitude AS string) 
                                    ORDER BY  DepthObservationNumber) rn
              FROM `tgs_gold.dbo_tblDirectionalSurveyDetail`  
              WHERE NorthReference='T')
       WHERE rn = 1) d
JOIN `Saga.view_surface_locations` s ON s.SurfaceUWI = LEFT(d.UWI,10)  
WHERE   EXISTS (SELECT 1 FROM Saga.WellPerformanceHeader w WHERE w.UWI = d.UWI)
AND NOT EXISTS (SELECT UWI10
                  FROM (SELECT UWI10, UWI
                          FROM `tgs-production-data-dev-7e18.Saga.WellPerformanceHeader`  
                        GROUP BY UWI10,UWI HAVING COUNT(*) > 1) x  WHERE x.UWI10 = s.SurfaceUWI) ;

SET maxvalue = (SELECT MAX(ROWNUM) FROM Saga.ds_temp);
SET upperlimit = lowerlimit + increment;

#Truncate table
TRUNCATE TABLE `Saga.TBL_WELLSPATH`;

LOOP 

  IF lowerlimit > maxvalue THEN LEAVE;
  ELSE
    INSERT INTO `Saga.TBL_WELLSPATH`(                     

    SELECT UWI, ST_GEOGFROMTEXT(CONCAT('LINESTRING','(',coord,')'))   
    FROM (
            SELECT UWI, STRING_AGG(CONCAT(surfacelong,' ',surfacelat)) AS coord 
            FROM  (SELECT UWI,surfacelong,surfacelat  FROM Saga.ds_temp 
                   WHERE  ROWNUM > lowerlimit and ROWNUM <= upperlimit
                   ORDER BY DepthObservationNumber)         
            GROUP BY UWI
            
          )  
          WHERE (CHAR_LENGTH(coord) -CHAR_LENGTH(REPLACE(coord, ',', '')) + 1) > 1     
    
    );
  
    SET lowerlimit=upperlimit;
    SET upperlimit=upperlimit + increment;  
  END IF;
END LOOP;
DROP TABLE Saga.ds_temp;
END;


CREATE OR REPLACE PROCEDURE `tgs-production-data-2211.Saga.ProcSagaWellStick`()
BEGIN

TRUNCATE TABLE `Saga.tbl_wellstick`;

INSERT INTO `Saga.tbl_wellstick`
SELECT
  SurfaceUWI,
  BottomUWI,
  geom
FROM (
   
    SELECT
      s.SurfaceUWI,
      b.BottomUWI,
      --s.SurfaceGeoPoint,
      --b.BottomGeoPoint,
      ST_MAKELINE(s.SurfaceGeoPoint,b.BottomGeoPoint) AS geom
      --,ROW_NUMBER() OVER(PARTITION BY s.SurfaceUWI, b.BottomUWI ORDER BY s.SurfaceUWI) AS rnk
    FROM
      `Saga.view_surface_locations` s
    INNER JOIN
      `Saga.view_bottom_locations` b
    ON
      s.SurfaceUWI=LEFT(b.BottomUWI,10) AND s.ParentWellID=b.ParentWellID
    WHERE
          s.SurfaceLongitude_WGS84 <> b.BottomLongitude_WGS84
      AND s.SurfaceLatitude_WGS84 <> b.BottomLatitude_WGS84
      AND b.BottomUWI  NOT IN (SELECT LEFT(UWI,12) FROM `tgs_gold.dbo_tblDirectionalSurveyDetail` )
      AND s.SurfaceUWI NOT IN (SELECT UWI10
                                 FROM (SELECT UWI10, UWI FROM `tgs-production-data-dev-7e18.Saga.WellPerformanceHeader`  
                                        GROUP BY UWI10,UWI HAVING COUNT(*) > 1) x )
      AND  3.2808399 * ST_DISTANCE(s.SurfaceGeoPoint,b.BottomGeoPoint) > 25    
      AND 3.2808399 * ST_DISTANCE(s.SurfaceGeoPoint,b.BottomGeoPoint) < 15000
      )
  
WHERE
  ST_ASTEXT(geom) NOT LIKE '%POINT%';

END;

CREATE OR REPLACE PROCEDURE `Saga.ProcWellPerformanceHeaderRefresh`()
BEGIN


CREATE TABLE IF NOT EXISTS Saga.WellPerformanceHeader
(
  Country STRING,
  WellID INT64,
  ParentWellID INT64,
  UWI10 STRING,
  UWI STRING,
  County STRING,
  StateName STRING,
  WellAPIState STRING,
  WellAPICounty STRING,
  APIWell STRING,
  APISideTrack STRING,
  APICompletion STRING,
  WellName STRING,
  WellNumber STRING,
  StateWellID STRING,
  CalculatedMajorPhase STRING,
  SurfaceLatitude BIGNUMERIC(18, 15),
  SurfaceLongitude BIGNUMERIC(18, 15),
  BottomLatitude BIGNUMERIC(18, 15),
  BottomLongitude BIGNUMERIC(18, 15),
  SurfaceLatitude_NAD83 BIGNUMERIC(18, 15),
  SurfaceLongitude_NAD83 BIGNUMERIC(18, 15),
  BottomLatitude_NAD83 BIGNUMERIC(18, 15),
  BottomLongitude_NAD83 BIGNUMERIC(18, 15),
  SurfaceLatitude_NAD27 BIGNUMERIC(18, 15),
  SurfaceLongitude_NAD27 BIGNUMERIC(18, 15),
  BottomLatitude_NAD27 BIGNUMERIC(18, 15),
  BottomLongitude_NAD27 BIGNUMERIC(18, 15),
  SurfaceLatitude_EPSG3857 FLOAT64,
  SurfaceLongitude_EPSG3857 FLOAT64,
  BottomLatitude_EPSG3857 FLOAT64,
  BottomLongitude_EPSG3857 FLOAT64,
  SurfaceLatitude_WGS84 BIGNUMERIC(18, 15),
  SurfaceLongitude_WGS84 BIGNUMERIC(18, 15),
  SurfaceGeoPoint GEOGRAPHY,
  BottomLatitude_WGS84 BIGNUMERIC(18, 15),
  BottomLongitude_WGS84 BIGNUMERIC(18, 15),
  BottomGeoPoint GEOGRAPHY,
  SurfaceLatLongQualifier STRING,
  BottomLatLongQualifier STRING,
  TotalVerticalDepth INT64,
  MeasuredDepth INT64,
  PerfIntervalTop INT64,
  PerfIntervalBottom INT64,
  ElevationGround INT64,
  ElevationDrillFloor INT64,
  ElevationKellyBushing INT64,
  ElevationWaterDepth INT64,
  DisplayElevation STRING,
  Slant STRING,
  SpudDate DATE,
  TDDATE DATE,
  CompletionDate DATE,
  FirstProductionDate DATE,
  PlugDate DATE,
  DisplayFormation STRING,
  InterpretedProducingFormation STRING,
  LeaseID STRING,
  StateLeaseID STRING,
  LeaseName STRING,
  FieldName STRING,
  OperatorName STRING,
  UltimateOwner STRING,
  OriginalOperator STRING,
  WellStatus STRING,
  WellType STRING,
  plotinfo STRING,
  BasinName STRING,
  DisplayLocation STRING,
  Section STRING,
  Township STRING,
  TownshipDirection STRING,
  RangeName STRING,
  RangeDirection STRING,
  QuarterQuarter STRING,
  District STRING,
  Abstract STRING,
  Survey STRING,
  Block STRING,
  Offshore STRING,
  Area STRING,
  OffshoreBlock STRING,
  DistanceNS STRING,
  DistanceNSD STRING,
  DistanceEW STRING,
  DistanceEWD STRING,
  Perf BOOL,
  Test BOOL,
  HasProduction BOOL,
  HasForecast BOOL,
  OffShoreFlg BOOL,
  HasDrillString BOOL,
  HasCompletion BOOL,
  HasReportedTops BOOL,
  HasInjection BOOL,
  ActiveInjection BOOL,
  HasDisposition BOOL,
  HasVentFlare BOOL,
  ActiveVentFlare BOOL,
  LateralLength INT64,
  DrillingDays INT64,
  NumberOfStages INT64,
  MaxTreatmentRate NUMERIC(20, 3),
  MaxTreatmentPressure NUMERIC(20, 3),
  ProppantType STRING,
  ProppantAmount NUMERIC(20, 3),
  ProppantAmountPerFt NUMERIC(20, 3),
  FractureFluidType STRING,
  FractureFluidAmount NUMERIC(20, 3),
  FractureFluidAmountPerFt NUMERIC(20, 3),
  TreatmentRemarks STRING,
  AcidAmount NUMERIC(20, 3),
  BOEMaxPer1000Ft NUMERIC(20, 3),
  EURPer1000Ft NUMERIC(20, 3),
  FirstMonth DATE,
  LastMonth DATE,
  CumOil NUMERIC(20, 3),
  CumGas NUMERIC(20, 3),
  CumGOR NUMERIC(20, 3),
  CumWater NUMERIC(20, 3),
  CumYield NUMERIC(20, 3),
  CumBOE NUMERIC(20, 3),
  FirstMoOil NUMERIC(20, 3),
  FirstMoGas NUMERIC(20, 3),
  FirstMoWater NUMERIC(20, 3),
  SecondMoOil NUMERIC(20, 3),
  SecondMoGas NUMERIC(20, 3),
  SecondMoWater NUMERIC(20, 3),
  Cum3MoOil NUMERIC(20, 3),
  Cum3MoGas NUMERIC(20, 3),
  Cum3MoWater NUMERIC(20, 3),
  Cum3MoGOR NUMERIC(20, 3),
  Cum3MoYield NUMERIC(20, 3),
  Cum6MoOil NUMERIC(20, 3),
  Cum6MoGas NUMERIC(20, 3),
  Cum6MoWater NUMERIC(20, 3),
  Cum6MoGOR NUMERIC(20, 3),
  Cum6MoYield NUMERIC(20, 3),
  Cum6MoBOE NUMERIC(20, 3),
  Cum9MoOil NUMERIC(20, 3),
  Cum9MoGas NUMERIC(20, 3),
  Cum9MoWater NUMERIC(20, 3),
  Cum9MoGOR NUMERIC(20, 3),
  Cum9MoYield NUMERIC(20, 3),
  Cum1YrOil NUMERIC(20, 3),
  Cum1YrGas NUMERIC(20, 3),
  Cum1YrWater NUMERIC(20, 3),
  Cum1YrGOR NUMERIC(20, 3),
  Cum1YrYield NUMERIC(20, 3),
  Cum1YrBOE NUMERIC(20, 3),
  Cum2YrOil NUMERIC(20, 3),
  Cum2YrGas NUMERIC(20, 3),
  Cum2YrWater NUMERIC(20, 3),
  Cum3YrOil NUMERIC(20, 3),
  Cum3YrGas NUMERIC(20, 3),
  Cum3YrWater NUMERIC(20, 3),
  Cum5YrOil NUMERIC(20, 3),
  Cum5YrGas NUMERIC(20, 3),
  Cum5YrWater NUMERIC(20, 3),
  Cum10YrOil NUMERIC(20, 3),
  Cum10YrGas NUMERIC(20, 3),
  Cum10YrWater NUMERIC(20, 3),
  Cum15YrOil NUMERIC(20, 3),
  Cum15YrGas NUMERIC(20, 3),
  Cum15YrWater NUMERIC(20, 3),
  Cum20YrOil NUMERIC(20, 3),
  Cum20YrGas NUMERIC(20, 3),
  Cum20YrWater NUMERIC(20, 3),
  Recent1MoOil NUMERIC(20, 3),
  Recent1MoGas NUMERIC(20, 3),
  Recent1MoWater NUMERIC(20, 3),
  Recent3MoOil NUMERIC(20, 3),
  Recent3MoGas NUMERIC(20, 3),
  Recent3MoWater NUMERIC(20, 3),
  Recent6MoOil NUMERIC(20, 3),
  Recent6MoGas NUMERIC(20, 3),
  Recent6MoWater NUMERIC(20, 3),
  Recent1YrOil NUMERIC(20, 3),
  Recent1YrGas NUMERIC(20, 3),
  Recent1YrWater NUMERIC(20, 3),
  MaxOil NUMERIC(20, 3),
  MaxOilMonth DATE,
  MaxOilPlus2 NUMERIC(20, 3),
  MaxGas NUMERIC(20, 3),
  MaxGasMonth DATE,
  MaxGasPlus2 NUMERIC(20, 3),
  MaxWater NUMERIC(20, 3),
  MaxWaterMonth DATE,
  MaxWaterPlus2 NUMERIC(20, 3),
  MaxBOE NUMERIC(20, 3),
  MaxBOEMonth DATE,
  Latest1YrBOE6 NUMERIC(20, 3),
  Latest1YrBOE20 NUMERIC(20, 3),
  WellForecastOilRemaining NUMERIC(20, 3),
  WellForecastOilUltimate NUMERIC(20, 3),
  WellForecastGasRemaining NUMERIC(20, 3),
  WellForecastGasUltimate NUMERIC(20, 3),
  WellForecastWaterRemaining NUMERIC(20, 3),
  WellForecastWaterUltimate NUMERIC(20, 3),
  WellForecastGORUltimate NUMERIC(20, 3),
  WellForecastYieldUltimate NUMERIC(20, 3),
  WellForecastWaterCutUltimate NUMERIC(20, 3),
  WellForecastBOERemaining NUMERIC(20, 3),
  WellForecastBOEUltimate NUMERIC(20, 3),
  WellForecastOil1MonthRate NUMERIC(20, 3),
  WellForecastGas1MonthRate NUMERIC(20, 3),
  WellForecastWater1MonthRate NUMERIC(20, 3),
  WellForecastOil1YearCum NUMERIC(20, 3),
  WellForecastGas1YearCum NUMERIC(20, 3),
  WellForecastWater1YearCum NUMERIC(20, 3),
  WellForecastBOE1MonthRate NUMERIC(20, 3),
  WellForecastBOE1YearCum NUMERIC(20, 3),
  InjectionFirstMonth DATE,
  InjectionLastMonth DATE,
  CumInjectionLiquid NUMERIC(20, 3),
  CumInjectionGas NUMERIC(20, 3),
  LastInjectionType STRING,
  LastInjectionFormation STRING,
  VentFlareFirstMonth DATE,
  VentFlareLastMonth DATE,
  CumVentFlareVol NUMERIC(20, 3),
  DispositionType STRING
)
CLUSTER BY WellID, UWI, ParentWellID, County;




-- --Clearing the full data is just for now. As per requirements we can decide to whether to allow below statement or not.
 Truncate table Saga.WellPerformanceHeader;

--Inserting the headers data into WellPerformanceHeader table.

INSERT INTO Saga.WellPerformanceHeader
(Country
,WellID
,ParentWellID
,UWI10
,UWI
,County
,StateName
,WellAPIState
,WellAPICounty
,APIWell
,APISideTrack
,APICompletion
,WellName
,WellNumber
,StateWellID
,CalculatedMajorPhase
,SurfaceLatitude
,SurfaceLongitude
,BottomLatitude
,BottomLongitude
,SurfaceLatitude_NAD83
,SurfaceLongitude_NAD83
,BottomLatitude_NAD83
,BottomLongitude_NAD83
,SurfaceLatitude_NAD27
,SurfaceLongitude_NAD27
,BottomLatitude_NAD27
,BottomLongitude_NAD27
,SurfaceLatitude_EPSG3857
,SurfaceLongitude_EPSG3857
,BottomLatitude_EPSG3857
,BottomLongitude_EPSG3857
,SurfaceLatitude_WGS84
,SurfaceLongitude_WGS84
,SurfaceGeoPoint
,BottomLatitude_WGS84
,BottomLongitude_WGS84
,BottomGeoPoint
,SurfaceLatLongQualifier
,BottomLatLongQualifier
,TotalVerticalDepth
,MeasuredDepth
,PerfIntervalTop
,PerfIntervalBottom
,ElevationGround
,ElevationDrillFloor
,ElevationKellyBushing
,ElevationWaterDepth
,DisplayElevation
,Slant
,SpudDate
,TDDATE
,CompletionDate
,FirstProductionDate
,PlugDate
,DisplayFormation
,InterpretedProducingFormation
,LeaseID
,StateLeaseID
,LeaseName
,FieldName
,OperatorName
,UltimateOwner
,OriginalOperator
,WellStatus
,WellType
,plotinfo
,BasinName
,DisplayLocation
,Section
,Township
,TownshipDirection
,RangeName
,RangeDirection
,QuarterQuarter
,District
,Abstract
,Survey
,Block
,Offshore
,Area
,OffshoreBlock
,DistanceNS
,DistanceNSD
,DistanceEW
,DistanceEWD
,Perf
,Test
,HasProduction
,HasForecast
,OffShoreFlg
,HasDrillString
,HasCompletion
,HasReportedTops
,HasInjection
,ActiveInjection
,HasDisposition
,HasVentFlare
,ActiveVentFlare
,LateralLength
,DrillingDays
,NumberOfStages
,MaxTreatmentRate
,MaxTreatmentPressure
,ProppantType
,ProppantAmount
,ProppantAmountPerFt
,FractureFluidType
,FractureFluidAmount
,FractureFluidAmountPerFt
,TreatmentRemarks
,AcidAmount
,BOEMaxPer1000Ft
,EURPer1000Ft)



WITH
  Lease AS (
  SELECT
    Wellid,
    STRING_AGG(CAST(Leaseid AS string), '~')  AS Leaseid,
    STRING_AGG(LeaseName, '~') AS LeaseName,
    STRING_AGG(StateLeaseID, '~') AS StateLeaseID,
    STRING_AGG(CAST(GroupID AS string), '~') AS GroupID
  FROM
    tgs_gold.dbo_viewUSWellsForecast WHERE Country = 'US'
  GROUP BY
    Wellid ),
  Flags AS (
  SELECT
    WellID,
    DisplayAPI,
    MAX(Perf)Perf,
    MAX(Test)Test,
    MAX(hasProduction)HasProduction,
    MAX(HasForecast)HasForecast,
    MAX(OffShoreFlg)OffShoreFlg,
    MAX(HasDrillString)HasDrillString,
    MAX(HasCompletion)HasCompletion,
    MAX(HasReportedTops)HasReportedTops,
    MAX(hasinjection)HasInjection,
    MAX(ActiveInjection)ActiveInjection,
    MAX(hasDisposition)HasDisposition,
    MAX(hasventflare)HasVentFlare,
    MAX(activeventflare)ActiveVentFlare,
    --MAX(HardCopy)HardCopy,
    --MAX(RasterImages)RasterImages,
    -- MAX(SmartRasters)SmartRasters,
    --MAX(DigitalCurves)DigitalCurves,
    --MAX(EditedSonic)EditedSonic,
    --MAX(WorkStationReady)WorkStationReady,
    --MAX(DirectionalSurvey)DirectionalSurvey,
    --MAX(Velocity)Velocity,
    --MAX(ValidatedWellHeader)ValidatedWellHeader,
    --MAX(ARLAS)ARLAS
  FROM
    tgs_gold.dbo_viewUSWellsForecast WHERE Country = 'US'
  GROUP BY
    WellID,
    DisplayAPI ),
  DF AS (
  SELECT
    Y.Wellid,
    STRING_AGG(Y.DisplayFormation, '~')DisplayFormation
  FROM (
    SELECT
      DISTINCT Wellid,
      DisplayFormation
    FROM (
      SELECT
        *,
        RANK() OVER(PARTITION BY wellid ORDER BY Lastmonth DESC, Firstmonth DESC) rnk
      FROM
        tgs_gold.dbo_viewUSWellsForecast
      WHERE
        DisplayFormation IS NOT NULL AND Country = 'US')X
    WHERE
      rnk=1 )Y
  GROUP BY
    Y.WellID ),
-- ParentWell AS (
-- SELECT WellID,UWI10
-- FROM (
-- SELECT WellID,UWI10,ROW_NUMBER() OVER(PARTITION BY UWI10 ORDER BY DISPLAYAPI)rno
-- FROM tgs_gold.dbo_viewUSWellsForecast
-- )X
-- WHERE rno=1
-- )
ParentWell

AS
(
SELECT DISTINCT WellID,UWI10,ParentWellID
  FROM (    
     SELECT WellID,UWI10,FIRST_VALUE(WellID) OVER(PARTITION BY UWI10,CASE WHEN APIStateSource in ('15','17') THEN StateWellID ELSE '1' END ORDER BY DISPLAYAPI,HasProduction DESC, MeasuredDepth) ParentWellID

     FROM ( Select DISTINCT WELLID,UWI10,DisplayAPI,StateWellID,APIStateSource,HasProduction,MeasuredDepth FROM  tgs_gold.dbo_viewUSWellsForecast WHERE ApiWell IS NOT NULL AND COUNTRY='US' )A
 UNION ALL

SELECT  DISTINCT WellID,UWI10,WellID AS ParentWellID FROM tgs_gold.dbo_viewUSWellsForecast WHERE ApiWell IS NULL AND COUNTRY='US'

)X 
)




SELECT
trim(country),
  WellID,
  ParentWellID,
  UWI10,
  UWI,
  TRIM(County),
  TRIM(StateName),
  WellAPIState,
  WellAPICounty,
  APIWell,
  APISideTrack,
  APICompletion,
  TRIM(WellName),
  TRIM(WellNumber),
  StateWellID,
  CalculatedMajorPhase,
  SurfaceLatitude,
  SurfaceLongitude,
  BottomLatitude,
  BottomLongitude,
  SurfaceLatitude_NAD83,
  SurfaceLongitude_NAD83,
  BottomLatitude_NAD83,
  BottomLongitude_NAD83,
  SurfaceLatitude_NAD27,
  SurfaceLongitude_NAD27,
  BottomLatitude_NAD27,
  BottomLongitude_NAD27,
  SurfaceLatitude_EPSG3857,
  SurfaceLongitude_EPSG3857,
  BottomLatitude_EPSG3857,
  BottomLongitude_EPSG3857,
  SurfaceLatitude_WGS84,
  SurfaceLongitude_WGS84,
  ST_GEOGPOINT(case when SurfaceLongitude_WGS84 between -180 and 180 then SurfaceLongitude_WGS84 else null end, 
			   case when SurfaceLatitude_WGS84 between -90 and 90 then SurfaceLatitude_WGS84 else null end) as SurfaceGeoPoint,
  BottomLatitude_WGS84,
  BottomLongitude_WGS84,
  ST_GEOGPOINT(case when BottomLongitude_WGS84 between -180 and 180 then BottomLongitude_WGS84 else null end, 
			   case when BottomLatitude_WGS84 between -90 and 90 then BottomLatitude_WGS84 else null end) as BottomGeoPoint,
  TRIM(SurfaceLatLongQualifier),
  TRIM(BottomLatLongQualifier),
  TotalVerticalDepth,
  MeasuredDepth,
  PerfIntervalTop,
  PerfIntervalBottom,
  ElevationGround,
  ElevationDrillFloor,
  ElevationKellyBushing,
  ElevationWaterDepth,
  TRIM(DisplayElevation),
  TRIM(Slant),
  cast(SpudDate as date) as SpudDate,
  cast(TDDate as date) as TDDate,
  CompletionDate,
  FirstProductionDate,
  PlugDate,
  TRIM(DisplayFormation),
  InterpretedProducingFormation,
  Leaseid,
  StateLeaseID,
  TRIM(LeaseName),
  TRIM(FieldName),
  TRIM(CurrentOperator),
  UltimateOwner,
  OriginalOperator,
  TRIM(WellStatus),
  WellType,
  Plotinfo,
  TRIM(BasinName),
  DisplayLocation,
  Section,
  Township,
  TownshipDirection,
  `Range`,
  RangeDirection,
  QuarterQuarter,
  District,
  Abstract,
  Survey,
  Block,
  Offshore,
  Area,
  OffshoreBlock,
  DistanceNS,
  DistanceNSD,
  DistanceEW,
  DistanceEWD,
  cast(Perf as boolean) as Perf,
  cast(Test as boolean) as Test,
  cast(hasProduction as boolean) as hasProduction,
  cast(HasForecast as boolean) as HasForecast ,
  OffShoreFlg,
  HasDrillString,
  HasCompletion,
  HasReportedTops,
  hasinjection,
  ActiveInjection,
  hasDisposition,
  hasventflare,
  activeventflare,
  --HardCopy,
  --RasterImages,
  --SmartRasters,
  --DigitalCurves,
  --EditedSonic,
  --WorkStationReady,
  --DirectionalSurvey,
  --Velocity,
  --ValidatedWellHeader,
  --ARLAS,
  LateralLength,
  DrillingDays,
  NumberOfStages,
  MaxTreatmentRate,
  MaxTreatmentPressure,
  ProppantType,
  ProppantAmount,
  cast(ProppantAmountPerFt as Numeric) as ProppantAmountPerFt,
  FractureFluidType,
  FractureFluidAmount,
  cast(FractureFluidAmountPerFt as Numeric) as FractureFluidAmountPerFt,
  TreatmentRemarks,
  AcidAmount,
  BOEMaxPer1000Ft,
  EURPer1000Ft
    
FROM (
  SELECT
  V.Country,
    V.WellID,
    V.UWI10,
    V.DisplayAPI AS UWI,
    V.County,
    V.StateName,
    V.WellAPIState,
    V.WellAPICounty,
    V.APIWell,
    V.APISideTrack,
    V.APICompletion,
    replace(V.WellName,'""',"'") as WellName,
    V.WellNumber,
    V.StateWellID,
    V.CalculatedMajorPhase,
    --Latlongs 
    V.SurfaceLatitude,
    V.SurfaceLongitude,
    V.BottomLatitude,
    V.BottomLongitude,
    V.SurfaceLatitude_NAD83,
    V.SurfaceLongitude_NAD83,
    V.BottomLatitude_NAD83,
    V.BottomLongitude_NAD83,
    V.SurfaceLatitude_NAD27,
    V.SurfaceLongitude_NAD27,
    V.BottomLatitude_NAD27,
    V.BottomLongitude_NAD27,
    V.SurfaceLatitude_EPSG3857,
    V.SurfaceLongitude_EPSG3857,
    V.BottomLatitude_EPSG3857,
    V.BottomLongitude_EPSG3857,
    V.SurfaceLatitude_WGS84,
    V.SurfaceLongitude_WGS84,
    V.BottomLatitude_WGS84,
    V.BottomLongitude_WGS84,
    V.SurfaceLatLongQualifier,
    V.BottomLatLongQualifier,
    --depths AND dates 
    V.TotalVerticalDepth,
    V.MeasuredDepth,
    PerfIntervalTop,
    PerfIntervalBottom,
    V.ElevationGround,
    V.ElevationDrillFloor,
    V.ElevationKellyBushing,
    V.ElevationWaterDepth,
    V.DisplayElevation,
    V.Slant,
    V.SpudDate,
    V.TDDate,
    V.CompletionDate,
    V.FirstProductionDate,
    V.PlugDate,
    --IDs 
    P1.DisplayFormation,
    InterpretedProducingFormation,
    C1.Leaseid,
    C1.StateLeaseID,
    C1.LeaseName,
    /*C1.GroupID,*/ 
    V.FieldName,
    V.OperatorName AS CurrentOperator,
    V.UltimateOwner,
    V.OriginalOperator,
    V.WellStatus,
    V.WellType,
    V.Plotinfo,
    V.BasinName,
    --Location 
    V.DisplayLocation,
    V.Section,
    V.Township,
    V.TownshipDirection,
    V.Range,
    V.RangeDirection,
    V.QuarterQuarter,
    V.District,
    V.Abstract,
    V.Survey,
    V.Block,
    V.Offshore,
    V.Area,
    V.OffshoreBlock,
    V.DistanceNS,
    V.DistanceNSD,
    V.DistanceEW,
    V.DistanceEWD,
    --Flags 
   case when C2.Perf='1' then 'true' when C2.Perf='0' or C2.Perf is null then 'false'
	else C2.Perf end Perf,
	case when C2.Test='1' then 'true' when C2.Test='0' or C2.Test is null then 'false'
	else C2.Test end Test,
	case when C2.hasProduction='1' then 'true' when C2.hasProduction='0' or C2.hasProduction is null then 'false'
	else C2.hasProduction end hasProduction,
	case when C2.HasForecast='1' then 'true' when C2.HasForecast='0' or C2.HasForecast is null then 'false'
	else C2.HasForecast end HasForecast,
	case when C2.OffShoreFlg is null then false else C2.OffShoreFlg end OffShoreFlg,
	case when C2.HasDrillString is null then false else C2.HasDrillString end HasDrillString,
	case when C2.HasCompletion is null then false else C2.HasCompletion end HasCompletion,
	case when C2.HasReportedTops is null then false else C2.HasReportedTops end HasReportedTops,
	case when C2.hasinjection is null then false else C2.hasinjection end hasinjection,
    case when C2.ActiveInjection is null then false else C2.ActiveInjection end ActiveInjection,
	case when C2.hasDisposition is null then false else C2.hasDisposition end hasDisposition,
	case when C2.hasventflare is null then false else C2.hasventflare end hasventflare,
	case when C2.activeventflare is null then false else C2.activeventflare end activeventflare,
    --C2.HardCopy,
    --C2.RasterImages,
    --C2.SmartRasters,
    --C2.DigitalCurves,
    --C2.EditedSonic,
    --C2.WorkStationReady,
    --C2.DirectionalSurvey,
    --C2.Velocity,
    --C2.ValidatedWellHeader,
    --C2.ARLAS,
    --Completiondata 
    V.LateralLength,
    V.DrillingDays,
    V.NumberOfStages,
    V.MaxTreatmentRate,
    V.MaxTreatmentPressure,
    V.ProppantType,
    V.ProppantAmount,
CASE WHEN coalesce(LateralLength,0)=0 THEN NULL

                       WHEN  cast((ProppantAmount/LateralLength) as bignumeric) > 20000 THEN NULL

                                        ELSE cast( (ProppantAmount/LateralLength) as bignumeric) END  ProppantAmountPerFt,
    V.FractureFluidType,
    V.FractureFluidAmount,
    CASE WHEN coalesce(LateralLength,0)=0 THEN NULL

                       WHEN  cast((FractureFluidAmount/LateralLength) as bignumeric) > 20000 THEN NULL

                                        ELSE cast( (FractureFluidAmount/LateralLength) as bignumeric) END  FractureFluidAmountPerFt,
    V.TreatmentRemarks,
    V.AcidAmount,
   --CASE WHEN coalesce(LateralLength,0)=0 THEN NULL

                       --ELSE  cast(((MaxBOE*1000)/LateralLength) as bignumeric)  END 
					   null as BOEMaxPer1000Ft,
  --CASE WHEN coalesce(LateralLength,0)=0 THEN NULL

                 --ELSE cast(((coalesce(WellForecastBOEUltimate,WellFormationForecastBOEUltimate)*1000)/LateralLength) as bignumeric) END 
				 null as EURPer1000Ft,
				 case when V.wellnumber='Lease' then V.WellID else P.ParentWellID  end AS ParentWellID,
    ROW_NUMBER() OVER(PARTITION BY V.WellID ORDER BY v.LastMonth DESC)rno
  FROM
    tgs_gold.dbo_viewUSWellsForecast V
  JOIN
    Lease C1
  ON
    C1.WellID=V.WellID
  JOIN
    Flags C2
  ON
    C2.WellID=V.WellID
  JOIN
	ParentWell P 
  ON 
	P.UWI10=V.UWI10 AND P.WellID=V.WellID
  LEFT JOIN
    DF P1
  ON
    P1.WellID=V.WellID 
	WHERE COUNTRY='US')X
WHERE
  rno=1;

    
 CALL Saga.ProcSagaBaseTablesRefresh();


END;

CREATE OR REPLACE PROCEDURE `Saga.ProcWellPerformanceStatsUpdate`()
BEGIN
CALL Saga.ProcSagaStatsInjection_usingTempTable();
CALL Saga.ProcSagaStatsProduction_usingTempTable();
CALL Saga.ProcSagaStatsVentFlare_usingTempTable();
CALL Saga.ProcSagaStatsWellForecast_usingTempTable();
END;