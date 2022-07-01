Rfuns::load_pkgs('arrow', 'data.table', 'fasttime')

fpath <- file.path(ext_path, 'us', 'nyc_taxi')

# setnames(yt, 
#          c('vendor_id', 'pu_dt', 'do_dt', 'p_count', 'distance', 'ratecode', 
#            'store_forward', 'pu_id', 'do_id', 'payment_type', 'fare_amount', 
#            'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
#            'imp_surcharge', 'total_amount', 'con_surcharge', 'airport_fee'
#          )
# )

process_year <- function(x, m = 1:12, fpath = file.path(ext_path, 'us', 'nyc_taxi')){
    message('\n===============')
    message('Processing ', x + 2000)
    dts <- rbindlist(lapply(
              m,
              \(y){
                  yp <- paste0(ifelse(y < 10, '0', ''), y)
                  message('  >>> ', x, yp)
                  read_parquet(
                        file.path(fpath, paste0('ytt', x, yp, '.parquet')), 
                        col_select =  c(2:5, 8:11, 17) # NULL  # 1:4
                  ) |> as.data.table()
              }
    ))
    setnames(dts, c('pu_dt', 'do_dt', 'p_count', 'distance', 'pu_id', 'do_id', 'pay_type', 'fare', 'total'))
    message('\n  Ordering dataset...')
    setorderv(dts, c('pu_dt', 'do_dt'))
    message('  Adding date-time measures...')
    dts[, `:=`( 
              pu_month = month(pu_dt),
              pu_week = isoweek(pu_dt),
              pu_day = mday(pu_dt),
              pu_hour = hour(pu_dt),
              pu_min = minute(pu_dt),
              pu_wday = as.integer(strftime(pu_dt, '%u')),
              duration = as.integer(do_dt - pu_dt, units = 'mins')
        )][, c('pu_dt', 'do_dt') := NULL]
    setcolorder(dts, c('pu_month', 'pu_week', 'pu_day', 'pu_hour', 'pu_min', 'pu_wday',
                       'duration', 'distance', 'pu_id', 'do_id', 'p_count', 'fare', 'total'))
    message('  Saving...')
    write_fst_idx(x + 2000, c('pu_month', 'pu_day'), dts, fpath)
    rm(dts)
    gc()
}

# for(x in 11:21) process_year(x)
process_year(22, 1:3)
