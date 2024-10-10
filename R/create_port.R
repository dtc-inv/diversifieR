#' @title Match DTCNames to daily and monthly returns
#' @param dtc_name DTCNames to match
#' @param ret list of daily and monthly returns from Database
#' @details
#' Daily returns are matched first. If daily returns are missing then the
#' monthly return files are checked. If there is a combo of daily and monthly
#' the daily returns are converted to monthly returns and combined.
#' @export
dtc_name_match_ret <- function(dtc_name, ret) {
  month_bool <- FALSE
  ixd <- match(dtc_name, colnames(ret$d))
  # no daily returns found,exit function by returning result list of monthly
  # data
  if (all(is.na(ixd))) {
    ixm <- match(dtc_name[is.na(ixd)], colnames(ret$m))
    if (all(is.na(ixm))) {
      warning("no returns found")
      return(NULL)
    }
    combo <- ret$m[, na.omit(ixm)]
    if (any(is.na(ixm))) {
      miss <- dtc_name[!dtc_name %in% colnames(combo)]
    } else {
      miss <- NULL
    }
    res <- list()
    res$r <- combo
    res$miss <- miss
    res$month_bool <- TRUE
    return(res)
  }
  # if daily returns are missing begin month check procedure
  if (any(is.na(ixd))) {
    ixm <- match(dtc_name[is.na(ixd)], colnames(ret$m))
    # there are monthly returns to fill
    if (any(ixm)) {
      month_bool <- TRUE
      d <- ret$d[, na.omit(ixd)]
      d[is.na(d)] <- 0
      dm <- change_freq(d, 'months')
      dm[dm == 0] <- NA
      m <- ret$m[, na.omit(ixm)]
      combo <- xts_cbind(dm, m)
      miss <- dtc_name[!dtc_name %in% colnames(combo)]
      # there are no monthly returns to fill
    } else {
      combo <- ret$d[, na.omit(ixd)]
      miss <- dtc_name[is.na(ixd)]
    }
    # if daily is not missing
  } else {
    combo <- ret$d[, ixd]
    miss <- NULL
  }
  res <- list()
  res$r <- combo
  res$miss <- miss
  res$month_bool <- month_bool
  return(res)
}


#' @title Create a Portfolio from ids
#' @param msl master security list from Database
#' @param ret return list from Database
#' @param wgt optional weight vector for portfolio
#' @param nm optional string for portfolio name
#' @param reb_freq optional character to specify rebalance frequency
#' @export
port_from_ids <- function(msl, ret, ids, wgt = NULL, nm = "Port",
                          reb_freq = "M") {
  if (is.null(wgt)) {
    wgt <- rep(1/length(ids), length(ids))
  }
  ix <- id_match_msl(ids, msl)
  if (any(is.na(ix))) {
    warning(paste0(ids[is.na(ix)], " not found"))
    ids <- ids[!is.na(ix)]
  }
  wgt <- wgt[!is.na(ix)]
  wgt <- wgt / sum(wgt)
  dtc_name <- msl$DTCName[na.omit(ix)]
  res <- dtc_name_match_ret(dtc_name, ret)
  asset_ret <- res$r
  asset_ret <- cut_time(asset_ret, first_comm_start(asset_ret),
                        last_comm_end(asset_ret))
  clean <- rm_na_col(asset_ret)
  clean_ret <- clean$ret
  ix <- colnames(asset_ret) %in% colnames(clean_ret)
  if (any(ix == FALSE)) {
    wgt <- wgt[ix]
    warning(paste0(ids[!ix], " returns exceeded NA threshold"))
  }
  names(wgt) <- colnames(clean_ret)
  if (res$month_bool) {
    ret_freq = "M"
  } else {
    ret_freq = "D"
  }
  p <- Portfolio$new(
    name = nm,
    asset_ret = clean_ret,
    reb_wgt = wgt,
    ret_freq = ret_freq,
    reb_freq = reb_freq
  )
  p$lazy_reb_wgt()
  p$rebal()
  return(p)
}


#' @export
port_from_holdings <- function(
    mdf,
    ret,
    nm = 'Port',
    reb_freq = c('BH', 'D', 'M', 'Q', 'A'),
    re_wgt_na = TRUE,
    clean_ret = TRUE,
    fill_na_wgt = TRUE)
{
  wgt <- tidyr::pivot_wider(
    mdf$match,
    id_cols = returnInfo,
    names_from = DTCName,
    values_from = pctVal
  )
  wgt <- df_to_xts(wgt)
  if (fill_na_wgt) {
    wgt[is.na(wgt)] <- 0
  }
  ix <- match(colnames(wgt), colnames(ret$d))
  if (any(is.na(ix))) {
    ix_m <- match(colnames(wgt), colnames(ret$m))
    m_repl <- ix_m[is.na(ix)]
    if (any(m_repl, na.rm = TRUE)) {
      ret_d <- ret$d[, na.omit(ix)]
    }
  }
  if (all(is.na(ix))) {
    stop('no asset returns found')
  }
  if (any(is.na(ix))) {
    miss_df <- data.frame(
      DTCName = colnames(wgt)[is.na(ix)],
      LastWgt = as.numeric(wgt[nrow(wgt), is.na(ix)])
    )
    if (re_wgt_na) {
      wgt <- wgt[, !is.na(ix)]
      wgt <- wgt / rowSums(wgt)
    }
  } else {
    miss_df <- data.frame()
  }
  asset_ret <- ret$d[, na.omit(ix)]
  first_days <- rep(NA, ncol(asset_ret))
  last_days <- rep(NA, ncol(asset_ret))
  for (i in 1:ncol(asset_ret)) {
    first_days[i] <- zoo::index(na.omit(asset_ret[, i]))[1]
    last_days[i] <- zoo::index(na.omit(asset_ret[, i]))[nrow(na.omit(asset_ret[, i]))]
  }
  first_days <- as.Date(first_days)
  last_days <- as.Date(last_days)
  asset_ret <- cut_time(asset_ret, max(first_days), min(last_days))
  if (clean_ret) {
    eps <- floor(0.05 * nrow(asset_ret))
    bad_col <- rep(FALSE, ncol(asset_ret))
    for (i in 1:ncol(asset_ret)) {
      miss <- sum(is.na(asset_ret)[, 1])
      if (miss > eps) {
        bad_col[i] <- TRUE
      } else {
        asset_ret[miss, i] <- 0
      }
    }
    if (any(bad_col)) {
      x <- data.frame(
        DTCName = colnames(asset_ret)[bad_col],
        LastWgt = as.numeric(wgt[nrow(wgt), bad_col])
      )
      miss_df <- rbind(miss_df, x)
    }
  } else {
    if (any(is.na(asset_ret))) {
      warning(paste0(sum(is.na(asset_ret))), ' returns missing, replacing
              with 0s')
      asset_ret[is.na(asset_ret)] <- 0
    }
  }
  p <- Portfolio$new(
    name = nm,
    asset_ret = asset_ret,
    reb_wgt = wgt
  )
  p$lazy_reb_wgt(reb_freq = reb_freq)
  p$rebal()
  res <- list()
  res$port <- p
  res$miss_df <- miss_df
  return(res)
}

#' @export
rf_from_const <- function(date_start, date_end, a) {
  dt <- us_trading_days(as.Date(date_start), as.Date(date_end))
  rf <- xts(rep((1+a)^(1/252)-1, length(dt)), dt)
  colnames(rf) <- "rf"
  return(rf)
}
