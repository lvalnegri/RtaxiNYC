Rfuns::load_pkgs('arrow', 'data.table', 'fasttime', 'sf')

yb <- qs::qread('./data-raw/locations.sf')
ybx <- st_bbox(yb)

# read file
yh <- read_parquet(file.path(out_path, 'ytt0901.parquet')) |> as.data.table()
for(x in names(yh)) if(grepl('vctrs', class(yh[[x]])[1])) yh[[x]] <- NA_real_

# remove records with zero coordinates or outside NYC bounding box
yh <- yh[!(Start_Lon == 0 | End_Lon == 0 | End_Lat == 0 | Start_Lat == 0)]
yh <- yh[Start_Lon %between% c(ybx['xmin'], ybx['xmax'])]
yh <- yh[End_Lon %between% c(ybx['xmin'], ybx['xmax'])]
yh <- yh[Start_Lat %between% c(ybx['ymin'], ybx['ymax'])]
yh <- yh[End_Lat %between% c(ybx['ymin'], ybx['ymax'])]

# lookup for pickup and dropoff zone (delete records outside any of both)
yhpu <- yh[, .(x = Start_Lon, y = Start_Lat)] |> 
          st_as_sf(coords = c('x', 'y'), crs = 4326) |> 
          st_join(yb |> subset(select = 'id'), join = st_within) |> 
          st_drop_geometry() |> 
          setnames('pu_id')
yhdo <- yh[, .(x = End_Lon, y = End_Lat)] |> 
          st_as_sf(coords = c('x', 'y'), crs = 4326) |> 
          st_join(yb |> subset(select = 'id'), join = st_within) |> 
          st_drop_geometry() |> 
          setnames('do_id')
yh <- cbind(yh, yhpu, yhdo)
yh[, c('Start_Lon', 'Start_Lat', 'End_Lon', 'End_Lat') := NULL]
yh <- yh[!(is.na(pu_id) |is.na(do_id))]

setnames(yh, 
         c('vendor_id', 'pu_dt', 'do_dt', 'p_count', 'distance', 'ratecode', 
              'store_forward', 'payment_type', 'fare_amount', 'con_surcharge', 'mta_tax', 
              'tip_amount', 'tolls_amount', 'total_amount', 'pu_id', 'do_id'
        )
)


# clean remaining columns
yh[, `:=`( pu_dt = fastPOSIXct(pu_dt), do_dt = fastPOSIXct(do_dt) )]

