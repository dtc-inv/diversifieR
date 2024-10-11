
port_to_perf_summary <- function(p, bench = NULL, rf = NULL) {
  if (is.null(bench) && !is.null(p$benchmark)) {
    bench <- p$benchmark$port_ret
  }
  if (is_null_dim(p$rf)) {
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
  if (is.null(bench)) {
    x <- rbind(geo_ret, vol, down_vol, max_dd, sharpe, sortino, recov)
    xdf <- data.frame(
      Metric = c("Geometric Return", "Volatility", "Downside Vol",
      "Worst Drawdown", "Sharpe Ratio", "Sortino Ratio", "Recovery"),
      x,
      row.names = NULL
    )
    colnames(xdf) <- c("Metric", colnames(asset))
    return(xdf)
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
    n_bench <- ncol(bench)
    if (n_bench > 1) {
      for (i in 2:n_bench) {
        te <- rbind(te, c(sqrt(diag(a_cov[[i]])), bench_pad_na))
        up_capt <- rbind(
          up_capt,
          c(calc_up_capture(asset, bench[, i]),bench_pad_na)
        )
        down_capt <- rbind(
          down_capt,
          c(calc_down_capture(asset, bench[, i]), bench_pad_na)
        )
        xbeta <- rbind(
          xbeta,
          hist_cov[, (ncol(asset) + i)] /
            hist_cov[(ncol(asset) + i), (ncol(asset) + i)]
        )
        act_ret <- rbind(
          act_ret,
          c(geo_ret[1:ncol(asset)] - geo_ret[(ncol(asset)+i)], bench_pad_na)
        )
      }
    }
    x <- rbind(geo_ret, act_ret, vol, down_vol, max_dd, sharpe, sortino, recov,
               te, xbeta, act_ret / te, up_capt, down_capt)
    metric <- c(
      "Geometric Return",
      paste0("Active Return: Bench ", 1:n_bench),
      "Volatility",
      "Downside Vol",
      "Worst Drawdown",
      "Sharpe Ratio",
      "Sortino Ratio",
      "Recovery",
      paste0("Tracking Error: Bench ", 1:n_bench),
      paste0("Beta: Bench ", 1:n_bench),
      paste0("Info Ratio", 1:n_bench),
      paste0("Up Capture", 1:n_bench),
      paste0("Down Capture", 1:n_bench)
    )
    xdf <- data.frame(
      Metric = metric,
      x,
      row.names = NULL
    )
    colnames(xdf) <- c("Metric", colnames(asset),
                       paste0("Bench ", 1:n_bench, ": ", colnames(bench)))
    return(xdf)
  }
}

#' @title Helper function to set color palette
#' @return list of colors
#' @export
dtc_col <- function() {
  c(rgb(0, 48, 87, maxColorValue = 255),     # navy blue
    rgb(204, 159, 38, maxColorValue = 255),  # gold
    rgb(197, 82, 101, maxColorValue = 255),  # berry
    rgb(113, 158, 139, maxColorValue = 255), # pine
    rgb(124, 126, 127, maxColorValue = 255), # deep charcoal
    rgb(124, 94, 119, maxColorValue = 255),  # amethyst
    rgb(222, 137, 88, maxColorValue = 255),  # orange
    rgb(141, 169, 180, maxColorValue = 255), # ocean
    rgb(192, 109, 89, maxColorValue = 255))  # terra cotta
}

#' @title Set n number of assets to plot to DTC color palette
#' @param n number of assets / items to plot
#' @return list of colors
#' @details if the number of assets exceed the number of colors in the DTC
#'   palette, 1:n colors will be generated based on numbers
#' @export
set_plot_col <- function(n) {
  col <- dtc_col()
  if (n > length(col)) {
    col <- c(col, 1:(n - length(col)))
  }
  return(col)
}

#' @title Plot Drawdowns
#' @param x xts object with time-series of returns
#' @export
viz_drawdowns <- function(x) {
  dd <- calc_drawdown(x)
  dat <- xts_to_tidy(dd)
  col <- set_plot_col(ncol(x))
  ggplot(dat, aes(x = Date, y = value, color = name)) +
    geom_line() + 
    scale_color_manual(values = col) +
    scale_y_continuous(labels = scales::percent) +
    ylab("") +
    labs(color = "", title = "Drawdowns") +
    theme_light()
}

#' @title Plot Cumulative Returns
#' @param x xts object with time-series of returns
#' @param init_val initial starting value of wealth
#' @export
viz_wealth_index <- function(x, init_val = 100) {
  wi <- ret_to_price(x) * init_val
  dat <- xts_to_tidy(wi)
  col <- set_plot_col(ncol(x))
  ggplot(dat, aes(x = Date, y = value, color = name)) +
    geom_line() + 
    scale_color_manual(values = col) +
    scale_y_continuous(labels = scales::number) +
    ylab("") +
    labs(color = "", title = "Cumulative Wealth") +
    theme_light()
}

#' @title Plot Rolling Volatility
#' @param x xts object with time-series of returns
#' @param n number of periods for rolling window
#' @param freq string to represent frequency for scaling
#' @export
viz_roll_vol <- function(x, n, freq) {
  col <- set_plot_col(ncol(x))
  x <- xts_to_dataframe(x)
  rv <- slider::slide(x[, -1], ~apply(.x, 2, sd), .complete = TRUE, 
                      .before = (n-1))
  rv <- do.call('rbind', rv) * sqrt(freq_to_scaler(freq))
  rv <- data.frame(Date = x$Date[(n):nrow(x)], rv)
  colnames(rv) <- colnames(x)
  dat <- pivot_longer(rv, cols = -Date)
  ggplot(dat, aes(x = Date, y = value, color = name)) +
    geom_line() + 
    scale_color_manual(values = col) +
    scale_y_continuous(labels = scales::percent) +
    ylab("") +
    labs(color = "", 
         title = paste0("Rolling ", n, " ", freq, " Volatility")) +
    theme_light()
}

#' @title Plot Rolling Beta
#' @param x xts object of assets to calculate beta for
#' @param b xts object of one benchmark to calculate beta to
#' @param rf xts object of risk-free rate
#' @param n number of rolling periods to use in the calculation
#' @param freq string to represent frequency for chart title
#' @export
viz_roll_beta <- function(x, b, rf, n, freq) {
  if (ncol(b) > 1) {
    b <- b[, 1]
    warning('more than one benchmark column provided, taking first column')
  }
  if (ncol(rf) > 1) {
    rf <- rf[, 1]
    warning('more than one rf column provided, taking first column')
  }
  xbeta <- roll_beta(x, b, rf, n)
  dat <- xts_to_tidy(xbeta)
  col <- set_plot_col(ncol(xbeta))
  ggplot(dat, aes(x = Date, y = value, color = name)) +
    geom_line() + 
    scale_color_manual(values = col) +
    scale_y_continuous(labels = scales::number) +
    ylab("") +
    labs(color = "", 
         title = paste0("Rolling ", n, " ", freq, " Beta to ", colnames(b))) +
    theme_light()
}

#' @title Handle combing and cleaning asset, rf, and benchmarks
#' @param x xts object of asset(s)
#' @param b xts object of benchmark(s)
#' @param rf optional xts object of risk-free
#' @details
#'   Utility function to handle common task of needing to line up asset, bench,
#'   and rf returns. Function will truncate all time-series based on common
#'   start and end dates, and will clean returns and remove missing columns if
#'   NAs exceed 5% threshold. 
#' @export
clean_asset_bench_rf <- function(x, b, rf = NULL) {
  if (!is.null(rf)) {
    combo <- xts_cbind_inter(x, rf)
    if (!is.null(colnames(combo$miss_ret))) {
      if (colnames(combo$miss_ret) == colnames(rf)) {
        stop('rf is missing')
      }
    }
  }
  combo <- xts_cbind_inter(combo$ret, b)
  if (!is.null(colnames(combo$miss_ret))) {
    if (colnames(combo$miss_ret) == colnames(b)) {
      stop('benchmark is missing')
    }
  }
  res <- list()
  if (!is.null(rf)) {
    res$rf <- combo$ret[, colnames(combo$ret) %in% colnames(rf)]
  } else {
    res$rf <- NULL
  }
  res$b <- combo$ret[, colnames(combo$ret) %in% colnames(b)]
  res$x <- combo$ret[, colnames(combo$ret) %in% colnames(x)]
  return(res)
}

#' @export
clean_ret_const <- function(x, a = 0) {
  x[is.na(x)] <- a
  return(x)
}
