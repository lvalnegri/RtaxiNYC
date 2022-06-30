# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### INSTALLATION ###############################################
# https://arrow.apache.org/docs/r/articles/install.html
# Sys.setenv(LIBARROW_MINIMAL = "false")
# install.packages('arrow')
# arrow::read_parquet(file.path(out_path, 'ytt2203.parquet'))
################################################################

Rfuns::load_pkgs('data.table', 'sf')

out_path <- file.path(ext_path, 'us', 'nyc_taxi')

# ancillary stuff
yzk <- fread(
          'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv', 
          col.names = c('id', 'borough', 'name', 'zone')
) |> setcolorder(c('id', 'name'))
yzk[borough == 'Unknown', c('name', 'zone') := NA]
fwrite(yzk, './data-raw/locations.csv')

tmpd <- tempdir()
tmpf <- tempfile()
download.file('https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip', tmpf)
unzip(tmpf, exdir = tmpd)
yb <- st_read(file.path(tmpd, grep('.*shp$', unzip(tmpf, list = TRUE)$Name, value = TRUE))) |> 
          subset(select = 'LocationID') |> 
          merge(yzk, by.x = 'LocationID', by.y = 'id') |> 
          st_transform(4326) |> 
unlink(tmpd)
# mapview::mapview(yb, zcol = 'borough')
qs::qsave(yb, './data-raw/locations.sf')

# Yellow Taxi Trip (ytt)
for(x in 10:22){
    for(y in 1:12){
        yp <- paste0(ifelse(y < 10, '0', ''), y)
        download.file(
            paste0('https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_20', x, '-', yp, '.parquet'), 
            file.path(out_path, paste0('ytt', x, yp, '.parquet'))
        )
    }
}


# Green Taxi Trip  (gtt)



# For-Hire Vehicle Trip (hvt)



# High Volume For-Hire Vehicle Trip (vvt)
