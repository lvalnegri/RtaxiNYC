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
          setnames('LocationID', 'id') |> 
          merge(yzk) |> 
          st_transform(4326)
unlink(tmpd)
unlink(tmpf)
# mapview::mapview(yb, zcol = 'borough')
qs::qsave(yb, './data-raw/locations.sf')

fwrite(
  data.table(
    var   = c( rep('vendor_id', 2), rep('ratecode', 7), rep('payment_type', 6) ),
    code  = c(1, 2, 1:6, 99, 1:6),
    value = c(
      'Creative Mobile Technologies, LLC', 'VeriFone Inc.',
      'Standard', 'JFK', 'Newark', 'Nassau or Westchester', 'Negotiated', 'Group', 'Unknown',
      'Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown', 'Voided Trip'
    )
  ), './data-raw/attributes.csv')


# Yellow Taxi Trip (ytt)
for(x in 10:21){
    for(y in 1:12){
        yp <- paste0(ifelse(y < 10, '0', ''), y)
        message('Downloading ', x, yp)
        tryCatch(
            download.file(
                paste0('https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_20', x, '-', yp, '.parquet'), 
                file.path(out_path, paste0('ytt', x, yp, '.parquet')),
                quiet = TRUE
            ),
            error = \(e) message(paste0('\n=> => ERROR: ytt', x, yp, ' not succeeded!\n'))
        )
    }
}

# Green Taxi Trip  (gtt)
for(x in 10:21){
    for(y in 1:12){
        yp <- paste0(ifelse(y < 10, '0', ''), y)
        message('Downloading ', x, yp)
        tryCatch(
            download.file(
                paste0('', x, '-', yp, '.parquet'), 
                file.path(out_path, paste0('gtt', x, yp, '.parquet')),
                quiet = TRUE
            ),
            error = \(e) message(paste0('\n=> => ERROR: gtt', x, yp, ' not succeeded!\n'))
        )
    }
}

# For-Hire Vehicle Trip (hvt)
for(x in 10:21){
    for(y in 1:12){
        yp <- paste0(ifelse(y < 10, '0', ''), y)
        message('Downloading ', x, yp)
        tryCatch(
            download.file(
                paste0('', x, '-', yp, '.parquet'), 
                file.path(out_path, paste0('hvt', x, yp, '.parquet')),
                quiet = TRUE
            ),
            error = \(e) message(paste0('\n=> => ERROR: hvt', x, yp, ' not succeeded!\n'))
        )
    }
}

# High Volume For-Hire Vehicle Trip (vvt)
for(x in 10:21){
    for(y in 1:12){
        yp <- paste0(ifelse(y < 10, '0', ''), y)
        message('Downloading ', x, yp)
        tryCatch(
            download.file(
                paste0('', x, '-', yp, '.parquet'), 
                file.path(out_path, paste0('vvt', x, yp, '.parquet')),
                quiet = TRUE
            ),
            error = \(e) message(paste0('\n=> => ERROR: vvt', x, yp, ' not succeeded!\n'))
        )
    }
}
