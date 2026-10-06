---
name: spatial
description: >
  Answer questions about spatial data using DuckDB. Use when the user mentions locations,
  coordinates, lat/lng, distances, maps, addresses, "near", "within", "closest", geographic
  names, or spatial file formats (GeoJSON, Shapefile, GeoPackage, GPX, GeoParquet). Also
  triggers when the user wants to find places, buildings, or roads — Overture Maps provides
  free global data on S3 with zero API keys. Handles spatial joins, distance calculations,
  containment checks, density analysis, and format conversions for geographic data.
argument-hint: <question or file> [additional context]
allowed-tools: Bash
---

You are answering spatial questions using DuckDB's spatial extension and, when needed, Overture Maps as a free global data source.

Question or file: `$0`
Additional context: `${1:-}`

## Step 1 — Understand what the user needs

Classify the question:

| Pattern | Data source | Key functions |
|---------|-------------|---------------|
| "Find X near Y" (no user file) | Overture Maps on S3 | `ST_Distance_Spheroid`, bbox filtering |
| "How far between A and B" | Geocode or user data | `ST_Distance_Spheroid` |
| "Which points fall inside polygons" | User files | `ST_Contains` |
| "Analyze this GeoJSON/Shapefile/GPX" | User file | `ST_Read`, measurement functions |
| "Show density/hotspots" | User or Overture data | H3 hex binning |
| "Convert to GeoJSON/GeoPackage" | User file | `COPY TO (FORMAT GDAL)` |
| "Count buildings/roads in area" | Overture Maps | bbox filtering + aggregation |

If the question involves real-world places, POIs, buildings, roads, or boundaries and the user hasn't provided a file, use **Overture Maps** — read `references/overture.md` for S3 paths and schema.

For spatial function syntax, read `references/functions.md`.

## Step 2 — Write and run the query

Always start with:
```sql
LOAD spatial;
SET geometry_always_xy = true;
```

Add extensions as needed:
- Overture/remote data: `LOAD httpfs; CREATE SECRET (TYPE S3, PROVIDER config, REGION 'us-west-2');`
- H3 hex binning: `INSTALL h3 FROM community; LOAD h3;`

### Key principles

**bbox filtering first** — When querying Overture, always filter on `bbox.xmin/xmax/ymin/ymax` before any spatial function. This uses Parquet predicate pushdown and avoids downloading the full dataset.

**Always set `geometry_always_xy = true`** — This ensures all spatial functions interpret coordinates as longitude, latitude (the standard for Overture, GeoJSON, and most data sources). Without it, spheroid functions assume latitude first and return wrong results.

**Use spheroid functions for real-world distances** — `ST_Distance_Spheroid` returns meters on the WGS84 ellipsoid. Plain `ST_Distance` uses planar coordinates and gives meaningless results for lat/lng. **Important:** spheroid functions (`ST_Distance_Spheroid`, `ST_Area_Spheroid`, etc.) require `POINT_2D` inputs, not generic `GEOMETRY`. Overture geometry columns are typed `GEOMETRY('OGC:CRS84')` and cannot be cast directly. Extract coordinates first:
```sql
ST_Point(ST_X(geometry), ST_Y(geometry))::POINT_2D
```

**CSV with lat/lng needs conversion** — `ST_Point(longitude, latitude)` (longitude first). This is the most common gotcha.

Run the query in a single bash call:

```bash
duckdb -c "
LOAD spatial;
<ADDITIONAL_SETUP>
<YOUR_QUERY>
"
```

## Step 3 — Present results

- For tabular results: show the data directly
- For spatial results: consider exporting to GeoJSON for visualization (`COPY TO 'result.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON')`)
- For distance/area results: use human-readable units (km for large distances, m for small)
- For density/hotspot results: describe the pattern and offer to export for visualization

If the query fails:
- **`duckdb: command not found`** → delegate to `/duckdb-skills:install-duckdb`
- **Missing extension** → `INSTALL spatial; LOAD spatial;` or `INSTALL h3 FROM community; LOAD h3;`
- **S3 access denied** → suggest checking AWS credentials
- **No results with Overture** → widen the bbox, check the category spelling, or try a broader search


# DuckDB Spatial Functions Reference

## Setup

```sql
INSTALL spatial; LOAD spatial;
SET geometry_always_xy = true;  -- ensures lng/lat order for all spatial functions
-- For H3 hex binning:
INSTALL h3 FROM community; LOAD h3;
```

## Reading spatial files

| Format | Read method |
|--------|-------------|
| GeoJSON | `ST_Read('file.geojson')` |
| Shapefile | `ST_Read('file.shp')` |
| GeoPackage | `ST_Read('file.gpkg')` |
| FlatGeobuf | `ST_Read('file.fgb')` |
| KML | `ST_Read('file.kml')` |
| GPX | `ST_Read('file.gpx')` |
| GeoParquet | `FROM 'file.geoparquet'` (native, no ST_Read needed) |
| OSM PBF | `ST_ReadOSM('region.osm.pbf')` (multithreaded, tags as MAP column) |
| CSV with lat/lng | `SELECT *, ST_Point(lng, lat) AS geom FROM 'file.csv'` |

`ST_Read` uses GDAL internally and supports 50+ formats.

## Writing spatial files

```sql
COPY (SELECT * FROM ...) TO 'out.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
COPY (SELECT * FROM ...) TO 'out.gpkg' WITH (FORMAT GDAL, DRIVER 'GPKG');
COPY (SELECT * FROM ...) TO 'out.shp' WITH (FORMAT GDAL, DRIVER 'ESRI Shapefile');
COPY (SELECT * FROM ...) TO 'out.fgb' WITH (FORMAT GDAL, DRIVER 'FlatGeobuf');
```

Set `geometry_always_xy = true` before writing to avoid axis order issues with KML or WGS84.

## Key functions

### Construction
| Function | Description |
|----------|-------------|
| `ST_Point(x, y)` | Create point from lon, lat |
| `ST_MakeEnvelope(xmin, ymin, xmax, ymax)` | Create bounding box rectangle |
| `ST_GeomFromText('POLYGON(...)')` | Parse WKT |
| `ST_GeomFromGeoJSON('{"type":...}')` | Parse GeoJSON |
| `ST_MakeLine(geom_array)` | Create line from points |

### Distance & proximity
| Function | Description |
|----------|-------------|
| `ST_Distance(a, b)` | Planar distance (units depend on CRS). Accepts any `GEOMETRY`. |
| `ST_Distance_Spheroid(a, b)` | Geodesic distance in **meters** (WGS84). **Requires `POINT_2D` inputs** — see note below. |
| `ST_DWithin(a, b, dist)` | Is planar distance ≤ dist? |
| `ST_DWithin_Spheroid(a, b, dist)` | Is geodesic distance ≤ dist meters? **Requires `POINT_2D` inputs.** |

> **`POINT_2D` requirement:** Spheroid functions (`ST_Distance_Spheroid`, `ST_Area_Spheroid`, `ST_Length_Spheroid`, `ST_DWithin_Spheroid`) only accept `POINT_2D`, not generic `GEOMETRY`. Overture Maps and `ST_Read()` return `GEOMETRY` types that cannot be cast directly. Extract and rebuild:
> ```sql
> ST_Point(ST_X(geometry), ST_Y(geometry))::POINT_2D
> ```

### Spatial relationships
| Function | Returns true when |
|----------|-------------------|
| `ST_Contains(a, b)` | a fully contains b |
| `ST_Within(a, b)` | a is fully within b |
| `ST_Intersects(a, b)` | a and b share any space |
| `ST_Covers(a, b)` | a covers b (no boundary distinction) |
| `ST_Disjoint(a, b)` | a and b share no space |
| `ST_Touches(a, b)` | a and b touch at boundary only |

### Measurement
| Function | Description |
|----------|-------------|
| `ST_Area(geom)` | Planar area |
| `ST_Area_Spheroid(geom)` | Geodesic area in **square meters** |
| `ST_Length(geom)` | Planar length of linestring |
| `ST_Length_Spheroid(geom)` | Geodesic length in **meters** |
| `ST_Perimeter(geom)` | Perimeter of polygon |
| `ST_NPoints(geom)` | Number of vertices |

### Transformation
| Function | Description |
|----------|-------------|
| `ST_Transform(geom, 'EPSG:from', 'EPSG:to')` | Reproject |
| `ST_Centroid(geom)` | Center point |
| `ST_Buffer(geom, dist)` | Buffer/expand geometry |
| `ST_Simplify(geom, tolerance)` | Simplify (Douglas-Peucker) |
| `ST_ConvexHull(geom)` | Convex hull |
| `ST_Union(a, b)` | Merge two geometries |
| `ST_Intersection(a, b)` | Intersection of two geometries |
| `ST_Difference(a, b)` | Subtract b from a |
| `ST_FlipCoordinates(geom)` | Swap x/y (for axis order issues) |

### Aggregation
| Function | Description |
|----------|-------------|
| `ST_Extent_Agg(geom)` | Bounding box of all geometries |
| `ST_Union_Agg(geom)` | Union of all geometries |
| `ST_Collect(array)` | Create GeometryCollection |

### Accessors
| Function | Description |
|----------|-------------|
| `ST_X(point)` | Get longitude |
| `ST_Y(point)` | Get latitude |
| `ST_GeometryType(geom)` | Type name (POINT, POLYGON, etc.) |
| `ST_AsText(geom)` | WKT representation |
| `ST_AsGeoJSON(geom)` | GeoJSON representation |
| `ST_XMin/XMax/YMin/YMax(geom)` | Bounding box coordinates |

## H3 hexagonal binning

H3 converts lat/lng to hexagonal cells at different resolutions. Great for density maps and hotspot analysis.

| Resolution | Avg edge | Use case |
|------------|----------|----------|
| 0 | ~1,107 km | Continental |
| 3 | ~59 km | Metropolitan regions |
| 5 | ~8 km | Cities |
| 7 | ~1.2 km | Neighborhoods |
| 9 | ~174 m | City blocks |
| 11 | ~25 m | Individual buildings |
| 13 | ~3.3 m | Parking spots |

Key functions:
```sql
-- Point to hex cell
h3_latlng_to_cell(lat, lng, resolution) → UBIGINT

-- Hex cell to center point
h3_cell_to_latlng(cell) → STRUCT(lat, lng)

-- Hex cell to polygon boundary (for visualization)
h3_cell_to_boundary_wkt(cell) → VARCHAR (WKT)

-- Ring of cells around a center
h3_grid_disk(cell, k) → UBIGINT[] (all cells within k rings)

-- Density example: count points per hex
SELECT h3_latlng_to_cell(lat, lng, 7) AS hex,
       count(*) AS cnt,
       h3_cell_to_boundary_wkt(hex) AS boundary
FROM my_points
GROUP BY hex
ORDER BY cnt DESC;
```

## Common patterns

### CSV with lat/lng → spatial queries
```sql
SELECT *, ST_Point(longitude, latitude) AS geom FROM 'data.csv';
```

### Distance matrix between all pairs
```sql
SELECT a.name, b.name,
       ST_Distance_Spheroid(a.geom, b.geom) AS dist_m
FROM locations a, locations b
WHERE a.name < b.name;
```

### Points inside polygons
```sql
SELECT p.name, z.zone_name
FROM points p, zones z
WHERE ST_Contains(z.geom, p.geom);
```

### Nearest neighbor (top-K per row)
```sql
SELECT a.name, b.name AS nearest,
       ST_Distance_Spheroid(a.geom, b.geom) AS dist_m
FROM locations a
CROSS JOIN LATERAL (
    SELECT name, geom FROM targets b
    ORDER BY ST_Distance_Spheroid(a.geom, b.geom)
    LIMIT 3
) b;
```