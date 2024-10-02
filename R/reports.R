
port_to_perf_summary <- function(p) {
  if (is.null(p$benchmark)) {
    bench <- NULL
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
  combo <- combo[, -ncol(combo)]
  hist_cov <- cov(combo) * freq_to_scaler(freq)
  if (nrow(combo) >= 252) {
    geo_ret <- calc_geo_ret(combo, freq)
  } else {
    geo_ret <- apply(combo + 1, 2, prod)
  }
  vol <- calc_vol(combo, freq)
  down_vol <- calc_down_vol(combo, "days")
  max_dd <- calc_max_drawdown(combo)
  sharpe <- calc_sharpe_ratio(combo, rf, "days")
  sortino <- calc_sortino_ratio(combo, rf, "days")
  recov <- geo_ret / -max_dd
  if (!is.null(bench)) {
    x <- rbind(geo_ret, vol, down_vol, max_dd, sharpe, sortino, recov)
    xdf <- data.frame(
      Metric = c("Geometric Return", "Volatility", "Downside Vol",
      "Worst Drawdown", "Sharpe Ratio", "Sortino Ratio", "Recovery"),
      x,
      row.names = NULL
    )
  } else {
    a_cov <- list()
    for (i in 1:ncol(bench)) {
      a_ret <- asset - bench[, rep(i, ncol(asset))]
      a_cov[[i]] <- cov(a_ret) * freq_to_scaler(freq)
    }
    bench_pad_na <- rep(NA, length(a_cov))
    te <- c(sqrt(diag(a_cov[[1]])), bench_pad_na)
    up_capt <- c(calc_up_capture(asset, bench[, 1]), bench_pad_na)
    down_capt <- c(calc_down_capture(asset, bench[, 1]), bench_pad_na)
    xbeta <- hist_cov[, (ncol(asset) + 1)] / hist_cov[(ncol(asset) + 1),
                                                      (ncol(asset) + 1)]
    act_ret <- c(geo_ret[1:ncol(asset)] - geo_ret[(ncol(asset)+1)],
      bench_pad_na)
  }


}
