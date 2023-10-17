CREATE OR REPLACE TABLE FUNCTION `project.dataset.tf_hexBinMaker`(p_gridStartPoint GEOGRAPHY, p_gridWidth INT64, p_bufferMeters INT64) AS (
with arrayTable as (

/*
Created by: Bryan Jacques

Description: Creates a table function that returns tessellated hexagon geographies and their centroids to aid in geospatial analysis

Inputs:
  p_gridStartPoint    // geopoint geography of bottom left start point of hexagon grid
  ,p_gridWidth        // width of grid. Is not 1:1 with number of hexagons across x axis, but higher the number the wider and taller grid will be
  ,p_bufferMeters     // distance between hexagon centroid in meters 

Example run:
-- select * from `project.dataset.tf_hexBinMaker`(st_geogpoint(-85,45),100,100)
*/

select arrayIx from unnest(generate_array(1,p_gridWidth)) as arrayIx
)

,firstRow as (
select
  arrayIx as xIndex
  ,st_geogpoint(
    st_boundingbox(st_buffer(p_gridStartPoint,arrayIx * p_bufferMeters)).xmax
    ,st_y(p_gridStartPoint)
    ) as xPoint
from arrayTable
)

,grid as (
select
  xIndex
  ,arrayIx as yIndex
  ,st_geogpoint(
    st_x(xPoint)
    ,st_boundingbox(st_buffer(xPoint,arrayIx * p_bufferMeters)).ymax
    ) as geoPoint
from firstRow
cross join arrayTable
)


-- helper sub table for hexStartPoints
,gridWithOddY as (
select
  xIndex
  ,yIndex
  ,geoPoint
from grid
where mod(yIndex,2) = 1
)

,hexStartPoints as (
select
  xIndex
  ,yIndex
  ,geoPoint
  ,dense_rank() over (order by yIndex) as denseYRank
from gridWithOddY
where true
qualify mod(denseYRank,2) = mod(xIndex,2)
)

,hexGrid as (
select
  hsp.xIndex
  ,hsp.yIndex
  ,st_makeline([
    p1.geoPoint
    ,p2.geoPoint
    ,p3.geoPoint
    ,p4.geoPoint
    ,p5.geoPoint
    ,p6.geoPoint
    ,p1.geoPoint
    ]) as hexLine
  ,hsp.geoPoint
  ,p1.geoPoint as p1
  ,p2.geoPoint as p2
  ,p3.geoPoint as p3
  ,p4.geoPoint as p4
  ,p5.geoPoint as p5
  ,p6.geoPoint as p6  
from hexStartPoints as hsp
left join grid as p1
  on hsp.xIndex = p1.xIndex
  and hsp.yIndex = p1.yIndex - 1
left join grid as p2
  on hsp.xIndex = p2.xIndex
  and hsp.yIndex = p2.yIndex - 2
left join grid as p3
  on hsp.xIndex = p3.xIndex - 1
  and hsp.yIndex = p3.yIndex - 3
left join grid as p4
  on hsp.xIndex = p4.xIndex - 2
  and hsp.yIndex = p4.yIndex - 2
left join grid as p5
  on hsp.xIndex = p5.xIndex - 2
  and hsp.yIndex = p5.yIndex - 1
left join grid as p6
  on hsp.xIndex = p6.xIndex - 1
  and hsp.yIndex = p6.yIndex
)

-- restate indexes
select
  dense_rank() over (order by xIndex) as xIndex
  ,dense_rank() over (order by yIndex) as yIndex
  ,dense_rank() over (order by xIndex)
    || ','
    || dense_rank() over (order by yIndex)
    as binId
  ,st_makepolygon(hexLine) as hexBin
  ,st_centroid(hexLine) as centroid
from hexGrid
order by xIndex, yIndex
);
