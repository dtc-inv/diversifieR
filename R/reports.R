
port_to_perf_summary <- function(p) {
  if (is.null(p$benchmark)) {
    bench <- p$port_ret
  } else {
    bench <- p$benchmark$port_ret
  }
  if (is.null(p$rf)) {
    rf <- rf_from_const(
      zoo::index(p$asset_ret[1]),
      zoo::index(p$asset_ret[nrow(p$asset_ret)]),
      0.025
    )
  } else {
    rf <- p$rf
  }
  perf_summary(p$asset_ret, bench, rf)
}


perf_summary <- function(asset, bench, rf, freq = "days") {
  if (ncol(rf) > 1) {
    warning("rf has more than one column, only taking first column")
    rf <- rf[, 1]
  }
  combo <- xts_cbind(asset, bench)
  combo <- xts_cbind(combo, rf)
  sd <- first_comm_start(combo)
  ed <- last_comm_end(combo)
  combo <- cut_time(combo, sd, ed)
  res <- rm_na_col(combo)
  if (ncol(res$miss_ret) > 1) {
    if (colnames(bench) %in% colnames(res$miss_ret)) {
      warning("benchmark missing")
      return(NULL)
    }
    if (colnames(rf) %in% colnames(res$miss_ret)) {
      warning("rf missing")
      return(NULL)
    }
    if (all(colnames(asset)) %in% colnames(res$miss_ret)) {
      warning("all assets missing")
      return(NULL)
    }
  }
  ix <- ncol(asset) - sum(colnames(asset) %in% colnames(res$miss_ret))
  combo <- res$ret
  asset <- combo[, 1:ix]
  bench <- combo[, (ix+1):(ncol(combo)-1)]
  rf <- combo[, ncol(combo)]
  geo_ret <- calc_geo_ret(combo, freq)
}
